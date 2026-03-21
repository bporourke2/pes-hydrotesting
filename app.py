from flask import Flask, render_template, request, session, redirect, url_for, jsonify, abort
from flask_socketio import SocketIO, join_room, leave_room
from flask_compress import Compress
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.utils import secure_filename
from authlib.integrations.flask_client import OAuth
from dotenv import load_dotenv
from functools import wraps
from collections import OrderedDict
import logging
import math
import json
import uuid
import shutil
import os
import re
import tempfile
import threading
from datetime import datetime
import pandas as pd
from logic import PipelineApp, Section, parse_station, station_format, rupture_analysis, multi_rupture_analysis, temp_correction_factor, nps_to_od, od_to_nps, NPS_OD
from db import (
    load_save as db_load_save, write_save, delete_save as db_delete_save,
    load_all_saves, clear_save_portfolio,
    load_portfolios, save_portfolios, upsert_portfolio, delete_portfolio,
    load_companies, save_companies, add_company, remove_company,
)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger('hydrotest')

def log_action(action, detail=''):
    user = session.get('user', {})
    name = user.get('name') or user.get('email') or 'anonymous'
    ip = request.remote_addr or '?'
    msg = f'[{name}] [{ip}] {action}'
    if detail:
        msg += f' — {detail}'
    logger.info(msg)

def get_od(params):
    """Resolve OD from params: prefer NPS lookup, fall back to stored od."""
    nps = params.get('nps')
    if nps:
        od = nps_to_od(str(nps))
        if od:
            return od
    return float(params.get('od', 42.0))

load_dotenv()

_APP_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
Compress(app)
socketio = SocketIO(app, async_mode='eventlet', cors_allowed_origins='*')

# Survey DataFrame cache: LRU with max 20 entries
_df_cache = OrderedDict()
_df_cache_lock = threading.Lock()

def get_cached_df(file_path, sheet=0):
    try:
        mtime = os.path.getmtime(file_path)
    except OSError:
        return None
    key = (file_path, mtime, sheet)
    with _df_cache_lock:
        if key in _df_cache:
            _df_cache.move_to_end(key)
            return _df_cache[key]
        import pandas as pd
        df = pd.read_excel(file_path, sheet_name=sheet)
        _df_cache[key] = df
        while len(_df_cache) > 20:
            _df_cache.popitem(last=False)
        return df

def get_sheet_names(file_path):
    """Return list of sheet names from an Excel file."""
    import openpyxl
    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    names = wb.sheetnames
    wb.close()
    return names

# --- Security helpers ---
_SAVE_ID_RE = re.compile(r'^[a-f0-9]{8}$')

def validate_save_id(save_id):
    """Reject path traversal: only allow 8-char hex IDs."""
    if not _SAVE_ID_RE.match(save_id):
        abort(400, 'Invalid save ID')

def check_save_owner(data):
    """Verify current user owns this save. Returns True if OK, aborts 403 if not."""
    owner_sub = data.get('owner_sub')
    if owner_sub and owner_sub != session.get('user', {}).get('sub'):
        abort(403, 'You do not have permission to access this analysis.')
    return True

def check_portfolio_access(data):
    """Verify current user has access to the portfolio this save belongs to.
    Admins always pass. Standard users need the portfolio in their list."""
    user = session.get('user', {})
    if user.get('role') == 'hydro-admin':
        return True
    allowed = get_user_portfolio_ids(user)
    if allowed is None:
        return True  # admin
    save_pf = data.get('portfolio_id')
    if not save_pf or save_pf not in allowed:
        abort(403, 'You do not have access to this portfolio.')
    return True

def safe_write_json(filepath, data):
    """Atomic JSON write via temp file + rename."""
    dirname = os.path.dirname(filepath)
    fd, tmp = tempfile.mkstemp(suffix='.json', dir=dirname)
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(data, f)
        os.replace(tmp, filepath)
    except Exception:
        os.unlink(tmp)
        raise

def validate_file_path(file_path):
    """Ensure file_path is inside the project directory."""
    resolved = os.path.realpath(file_path)
    base = os.path.realpath(_APP_DIR)
    if not resolved.startswith(base + os.sep) and resolved != base:
        return False
    return True

# C2 — No fallback secret key
secret = os.environ.get('SECRET_KEY')
if not secret:
    if os.environ.get('FLASK_ENV') == 'production' or not app.debug:
        raise RuntimeError("SECRET_KEY must be set in production. Sessions will not survive restarts without it.")
    import secrets as _secrets
    secret = _secrets.token_hex(32)
    logging.warning("SECRET_KEY not set — generated ephemeral key. Sessions will not survive restarts.")
app.secret_key = secret

# Upload size limit (50 MB)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

# Session cookie hardening
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = os.environ.get('SESSION_COOKIE_SECURE', 'true').lower() == 'true'

# CSRF defense-in-depth: verify Origin header on state-changing requests
@app.before_request
def csrf_origin_check():
    if request.method not in ('POST', 'PUT', 'DELETE', 'PATCH'):
        return
    origin = request.headers.get('Origin')
    if origin:
        expected = request.host_url.rstrip('/')
        if origin != expected:
            abort(403)

@app.context_processor
def inject_user_role():
    """Make user_is_admin available in all templates."""
    return {'user_is_admin': session.get('user', {}).get('role') == 'hydro-admin'}

# --- OAuth / OIDC (multi-provider) ---
from oauth_config import (load_providers, get_provider, get_default_provider,
                          get_enabled_providers, migrate_from_env)
from user_store import (upsert_user, get_user, is_admin, has_access,
                        is_first_user, load_users, set_user_role, delete_user,
                        get_user_portfolio_ids, set_user_portfolios,
                        add_portfolio_to_user, remove_portfolio_from_all_users)

# Migrate legacy .env config on first run
migrate_from_env()

oauth = OAuth(app)
_registered_providers = set()

def _ensure_registered(provider):
    """Lazily register an OAuth client for a provider if not already done."""
    name = f"oauth_{provider['id']}"
    if name not in _registered_providers:
        oauth.register(
            name=name,
            client_id=provider['client_id'],
            client_secret=provider['client_secret'],
            server_metadata_url=provider.get('discovery_url'),
            client_kwargs={'scope': provider.get('scopes', 'openid email profile')},
        )
        _registered_providers.add(name)
    return getattr(oauth, name)

def _reload_provider(provider):
    """Force re-register a provider (after config edit)."""
    name = f"oauth_{provider['id']}"
    _registered_providers.discard(name)
    # Remove from authlib's internal registry so it re-reads config
    oauth._clients.pop(name, None)
    oauth._registry.pop(name, None)
    return _ensure_registered(provider)

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user'):
            # Allow unauthenticated access if no providers configured
            if not get_enabled_providers():
                return redirect(url_for('oauth_setup'))
            session['next'] = request.url
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user'):
            session['next'] = request.url
            return redirect(url_for('login'))
        if session['user'].get('role') != 'hydro-admin':
            abort(403, 'Admin access required.')
        return f(*args, **kwargs)
    return decorated

@app.route('/login')
def login():
    provider_id = request.args.get('provider')
    if provider_id:
        provider = get_provider(provider_id)
    else:
        provider = get_default_provider()
    if not provider:
        return redirect(url_for('oauth_setup'))
    enabled = get_enabled_providers()
    if len(enabled) > 1 and not provider_id:
        return render_template('login_select.html', providers=enabled)
    client = _ensure_registered(provider)
    session['_oauth_provider_id'] = provider['id']
    callback_url = url_for('auth_callback', _external=True)
    return client.authorize_redirect(callback_url)

@app.route('/auth/callback')
def auth_callback():
    provider_id = session.pop('_oauth_provider_id', None)
    provider = get_provider(provider_id) if provider_id else get_default_provider()
    if not provider:
        return "OAuth provider not found.", 400
    client = _ensure_registered(provider)
    token = client.authorize_access_token()
    userinfo = token.get('userinfo') or client.userinfo()

    # Extract groups from the configured claim (default: "groups")
    groups_claim = provider.get('groups_claim', 'groups')
    groups = userinfo.get(groups_claim, [])
    if isinstance(groups, str):
        groups = [g.strip() for g in groups.split(',')]

    sub = userinfo.get('sub')
    email = userinfo.get('email')
    name = userinfo.get('name') or userinfo.get('preferred_username')

    # First user ever gets auto-promoted to admin
    first = is_first_user()

    user_record = upsert_user(sub, email, name, groups, provider['id'])
    if first and user_record.get('role') != 'hydro-admin':
        set_user_role(sub, 'hydro-admin', lock=True)
        user_record['role'] = 'hydro-admin'

    # Block login if user has no valid role (not in hydro or hydro-admin group)
    if not user_record.get('role'):
        log_action('LOGIN_DENIED', f"{email} — no matching group ({groups})")
        session.clear()
        return render_template('login_denied.html', email=email, groups=groups), 403

    session['user'] = {
        'sub': sub,
        'email': email,
        'name': name,
        'role': user_record['role'],
    }
    session['_oauth_provider_id'] = provider['id']
    next_url = session.pop('next', None)
    log_action('LOGIN', f"{email} role={user_record['role']}")
    return redirect(next_url or url_for('welcome'))

@app.route('/logout')
def logout():
    log_action('LOGOUT')
    provider_id = session.get('_oauth_provider_id')
    provider = get_provider(provider_id) if provider_id else None
    session.clear()
    if provider and provider.get('logout_url'):
        return redirect(provider['logout_url'])
    return redirect(url_for('login'))

app.jinja_env.filters['station_format'] = station_format

def build_wt_column(df, col_map):
    """Apply wall thickness to df['_wt'], supporting a constant value or a named column."""
    wt_col = col_map.get('wt')
    if wt_col == '__constant__':
        df['_wt'] = float(col_map.get('wt_constant', 0.5))
    else:
        df['_wt'] = pd.to_numeric(df[wt_col], errors='coerce')
    return df

SAVES_DIR = os.path.join(_APP_DIR, 'saves')
UPLOADS_DIR = os.path.join(_APP_DIR, 'uploads')
DEMO_FILE = os.path.join(_APP_DIR, 'data', 'Testdata.xlsx')
os.makedirs(SAVES_DIR, exist_ok=True)
os.makedirs(UPLOADS_DIR, exist_ok=True)

DEV_MODE = False
DOCS_DIR = None
_ALLOWED_DOC_EXTS = {'.pdf', '.txt', '.md', '.docx', '.xlsx', '.csv'}

def load_docs():
    if not DOCS_DIR:
        return []
    idx = os.path.join(DOCS_DIR, '_index.json')
    if not os.path.exists(idx):
        return []
    with open(idx) as f:
        return json.load(f)

def save_docs(docs):
    safe_write_json(os.path.join(DOCS_DIR, '_index.json'), docs)

grade_smys = {
    'B': 35000,
    'X42': 42000,
    'X52': 52000,
    'X60': 60000,
    'X65': 65000,
    'X70': 70000,
    'X80': 80000
}

@app.route('/')
@login_required
def welcome():
    saves = load_all_saves()
    # Filter saves by portfolio access (admins see all, standard users see assigned portfolios)
    allowed_pf_ids = get_user_portfolio_ids(session.get('user', {}))
    if allowed_pf_ids is not None:
        saves = [s for s in saves if s.get('portfolio_id') and s['portfolio_id'] in allowed_pf_ids]
    portfolios = load_portfolios()
    pf_map = {pf['id']: pf['name'] for pf in portfolios}

    # Build company_tree: Company > Portfolio > Spread > Test Segment (save)
    from collections import OrderedDict
    pf_company_map = {pf['id']: pf.get('company', '') for pf in portfolios}
    # tree_raw[company][pf_id][spread] = [saves]
    tree_raw = OrderedDict()
    for s in saves:
        pi = s.get('project_info') or {}
        pf_id   = s.get('portfolio_id') or '__none__'
        company = (pi.get('owner_company', '').strip()
                   or pf_company_map.get(pf_id, '').strip()
                   or 'No Company')
        spread  = pi.get('spread', '').strip() or 'No Spread'
        if company not in tree_raw:
            tree_raw[company] = OrderedDict()
        if pf_id not in tree_raw[company]:
            tree_raw[company][pf_id] = OrderedDict()
        if spread not in tree_raw[company][pf_id]:
            tree_raw[company][pf_id][spread] = []
        tree_raw[company][pf_id][spread].append(s)

    company_names = sorted(tree_raw.keys(), key=lambda c: (c == 'No Company', c.lower()))
    company_tree = []
    for company in company_names:
        pf_groups = []
        for pf_id, spread_map in tree_raw[company].items():
            pf_name = pf_map.get(pf_id, 'Uncategorized') if pf_id != '__none__' else 'Uncategorized'
            spreads = []
            for spread_name, group_saves in spread_map.items():
                spreads.append({'name': spread_name, 'saves': group_saves})
            pf_groups.append({'id': pf_id, 'name': pf_name, 'spreads': spreads})
        company_tree.append({'company': company, 'portfolios': pf_groups})

    return render_template('welcome.html', saves=saves, portfolios=portfolios, company_tree=company_tree)

@app.route('/portfolio/create', methods=['POST'])
@login_required
def portfolio_create():
    name = request.form.get('name', '').strip()
    if name:
        portfolios = load_portfolios()
        if not any(pf['name'].lower() == name.lower() for pf in portfolios):
            new_id = str(uuid.uuid4())[:8]
            upsert_portfolio(new_id, name, request.form.get('company', '').strip() or None)
            # Auto-assign the new portfolio to the creating user
            user_sub = session.get('user', {}).get('sub')
            if user_sub:
                add_portfolio_to_user(user_sub, new_id)
    next_page = request.form.get('next', 'welcome')
    if next_page not in ('welcome', 'settings'):
        next_page = 'welcome'
    return redirect(url_for(next_page))

@app.route('/portfolio/edit/<portfolio_id>', methods=['POST'])
@admin_required
def portfolio_edit(portfolio_id):
    validate_save_id(portfolio_id)
    portfolios = load_portfolios()
    existing = next((pf for pf in portfolios if pf['id'] == portfolio_id), None)
    if existing:
        name = request.form.get('name', existing['name']).strip() or existing['name']
        company = request.form.get('company', '').strip() or None
        upsert_portfolio(portfolio_id, name, company)
    return redirect(url_for('settings'))

@app.route('/portfolio/delete/<portfolio_id>', methods=['POST'])
@admin_required
def portfolio_delete(portfolio_id):
    validate_save_id(portfolio_id)
    delete_portfolio(portfolio_id)
    clear_save_portfolio(portfolio_id)
    remove_portfolio_from_all_users(portfolio_id)
    next_page = request.args.get('next', 'welcome')
    if next_page not in ('welcome', 'settings'):
        next_page = 'welcome'
    return redirect(url_for(next_page))

@app.route('/settings', methods=['GET'])
@admin_required
def settings():
    return render_template('settings.html', portfolios=load_portfolios(),
                           companies=load_companies(), oauth_providers=load_providers(),
                           users=load_users(), docs=load_docs(), dev_mode=DEV_MODE)

@app.route('/settings/company/add', methods=['POST'])
@login_required
def company_add():
    name = request.form.get('name', '').strip()
    if name:
        add_company(name)
        log_action('COMPANY_ADD', name)
    return redirect(url_for('settings'))

@app.route('/settings/company/delete', methods=['POST'])
@admin_required
def company_delete():
    name = request.form.get('name', '').strip()
    if name:
        remove_company(name)
        log_action('COMPANY_DELETE', name)
    return redirect(url_for('settings'))

# --- OAuth Provider Management ---
from oauth_config import add_provider, update_provider, delete_provider, set_default_provider

@app.route('/settings/oauth/add', methods=['POST'])
@admin_required
def oauth_add():
    data = {
        'name': request.form.get('name', '').strip(),
        'provider_type': request.form.get('provider_type', 'oidc'),
        'issuer_url': request.form.get('issuer_url', '').strip().rstrip('/'),
        'client_id': request.form.get('client_id', '').strip(),
        'client_secret': request.form.get('client_secret', '').strip(),
        'discovery_url': request.form.get('discovery_url', '').strip(),
        'scopes': request.form.get('scopes', 'openid email profile').strip(),
        'logout_url': request.form.get('logout_url', '').strip(),
        'app_slug': request.form.get('app_slug', '').strip(),
        'groups_claim': request.form.get('groups_claim', 'groups').strip(),
    }
    if data['name'] and data['client_id'] and data['client_secret']:
        p = add_provider(data)
        log_action('OAUTH_ADD', f"{p['name']} ({p['id']})")
    return redirect(url_for('settings') + '#oauth')

@app.route('/settings/oauth/edit/<provider_id>', methods=['POST'])
@admin_required
def oauth_edit(provider_id):
    data = {}
    for key in ('name', 'provider_type', 'issuer_url', 'client_id', 'client_secret',
                'discovery_url', 'scopes', 'logout_url', 'app_slug', 'groups_claim'):
        val = request.form.get(key)
        if val is not None:
            data[key] = val.strip()
    # Don't overwrite secret with empty string if user left it blank
    if not data.get('client_secret'):
        data.pop('client_secret', None)
    data['enabled'] = request.form.get('enabled') == 'on'
    p = update_provider(provider_id, data)
    if p:
        _reload_provider(p)
        log_action('OAUTH_EDIT', f"{p['name']} ({p['id']})")
    return redirect(url_for('settings') + '#oauth')

@app.route('/settings/oauth/delete/<provider_id>', methods=['POST'])
@admin_required
def oauth_delete(provider_id):
    provider = get_provider(provider_id)
    if provider:
        delete_provider(provider_id)
        _registered_providers.discard(f"oauth_{provider_id}")
        log_action('OAUTH_DELETE', f"{provider['name']} ({provider_id})")
    return redirect(url_for('settings') + '#oauth')

@app.route('/settings/oauth/default/<provider_id>', methods=['POST'])
@admin_required
def oauth_set_default(provider_id):
    set_default_provider(provider_id)
    log_action('OAUTH_DEFAULT', provider_id)
    return redirect(url_for('settings') + '#oauth')

@app.route('/settings/user/role', methods=['POST'])
@admin_required
def user_set_role():
    sub = request.form.get('sub')
    role = request.form.get('role')
    if sub and role:
        u = set_user_role(sub, role, lock=True)
        if u:
            log_action('USER_ROLE', f"{u.get('email')} -> {role}")
    return redirect(url_for('settings') + '#users')

@app.route('/settings/user/delete', methods=['POST'])
@admin_required
def user_delete():
    sub = request.form.get('sub')
    if sub and sub != session.get('user', {}).get('sub'):
        user = get_user(sub)
        if user:
            delete_user(sub)
            log_action('USER_DELETE', user.get('email', sub))
    return redirect(url_for('settings') + '#users')

@app.route('/settings/docs/upload', methods=['POST'])
@admin_required
def docs_upload():
    if not DEV_MODE:
        abort(403)
    f = request.files.get('doc')
    if not f or not f.filename:
        return redirect(url_for('settings') + '#docs')
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in _ALLOWED_DOC_EXTS:
        return redirect(url_for('settings') + '#docs')
    safe_name = secure_filename(f.filename)
    doc_id = uuid.uuid4().hex[:8]
    stored_name = f'{doc_id}_{safe_name}'
    os.makedirs(DOCS_DIR, exist_ok=True)
    dest = os.path.join(DOCS_DIR, stored_name)
    f.save(dest)
    docs = load_docs()
    docs.append({
        'id': doc_id,
        'original_name': f.filename,
        'stored_name': stored_name,
        'uploaded_by': session.get('user', {}).get('email', 'unknown'),
        'upload_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'size': os.path.getsize(dest),
    })
    save_docs(docs)
    log_action('DOCS_UPLOAD', f.filename)
    return redirect(url_for('settings') + '#docs')

@app.route('/settings/docs/delete', methods=['POST'])
@admin_required
def docs_delete():
    if not DEV_MODE:
        abort(403)
    doc_id = request.form.get('id', '')
    if not re.match(r'^[a-f0-9]{8}$', doc_id):
        return redirect(url_for('settings') + '#docs')
    docs = load_docs()
    doc = next((d for d in docs if d['id'] == doc_id), None)
    if doc:
        dest = os.path.join(DOCS_DIR, doc['stored_name'])
        if os.path.exists(dest) and validate_file_path(dest):
            os.remove(dest)
        docs = [d for d in docs if d['id'] != doc_id]
        save_docs(docs)
        log_action('DOCS_DELETE', doc['original_name'])
    return redirect(url_for('settings') + '#docs')

@app.route('/rupture/<save_id>', methods=['GET', 'POST'])
@login_required
def rupture(save_id):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        abort(404)
    check_portfolio_access(data)

    col_map = data.get('col_map', {})
    file_path = data.get('file_path', '')
    if not file_path or not os.path.exists(file_path) or not validate_file_path(file_path):
        abort(400, 'Survey file not available for this save.')

    sheet = col_map.get('sheet', 0)
    df = get_cached_df(file_path, sheet=sheet)
    params = data.get('params', {})

    # Parse station/elevation columns matching how Section does it
    station_col = col_map.get('station')
    elev_col = col_map.get('elev')
    wt_col = col_map.get('wt')
    if not station_col or not elev_col or not wt_col:
        abort(400, 'Column mapping incomplete.')

    df = df.copy()
    df['_stn'] = df[station_col].apply(lambda v: parse_station(v))
    df['_elev'] = pd.to_numeric(df[elev_col], errors='coerce')
    build_wt_column(df, col_map)
    df = df.dropna(subset=['_stn', '_elev', '_wt'])

    start = parse_station(params['start'])
    end = parse_station(params['end'])
    df = df[(df['_stn'] >= min(start, end)) & (df['_stn'] <= max(start, end))].copy()
    df = df.sort_values('_stn').reset_index(drop=True)

    od = get_od(params)
    avg_wt = float(df['_wt'].mean())

    result = None
    multi_result = None
    error = None
    rup_station_inputs = []
    specific_weight = 62.4
    loaded_ra_id = ''
    loaded_ra = None

    stns_list = df['_stn'].tolist()
    elevs_list = df['_elev'].tolist()
    stn_min = df['_stn'].min()
    stn_max = df['_stn'].max()

    def _run(raw_stns, sw):
        parsed = []
        for s in raw_stns:
            stn = parse_station(s.strip())
            if not (stn_min <= stn <= stn_max):
                raise ValueError(f'Rupture station {s.strip()} is outside the analysis section range.')
            parsed.append(stn)
        if len(parsed) == 1:
            return rupture_analysis(stns_list, elevs_list, parsed[0], od, avg_wt, sw), None
        return None, multi_rupture_analysis(stns_list, elevs_list, parsed, od, avg_wt, sw)

    if request.method == 'POST':
        loaded_ra_id = request.form.get('ra_id', '').strip()
        if loaded_ra_id:
            for ra in data.get('rupture_analyses', []):
                if ra.get('id') == loaded_ra_id:
                    loaded_ra = ra
                    break
        try:
            raw = [s for s in request.form.getlist('rup_station') if s.strip()]
            specific_weight = float(request.form.get('specific_weight', 62.4))
            if raw:
                rup_station_inputs = raw
                result, multi_result = _run(raw, specific_weight)
                log_action('RUPTURE_ANALYSIS', f'save={save_id} stations={raw}')
        except Exception as e:
            error = str(e)
    elif request.method == 'GET':
        raw = [s for s in request.args.getlist('rup_station') if s.strip()]
        loaded_ra_id = request.args.get('ra_id', '').strip()
        if loaded_ra_id:
            for ra in data.get('rupture_analyses', []):
                if ra.get('id') == loaded_ra_id:
                    loaded_ra = ra
                    break
        if raw:
            try:
                specific_weight = float(request.args.get('specific_weight', 62.4))
                rup_station_inputs = raw
                result, multi_result = _run(raw, specific_weight)
            except Exception as e:
                error = str(e)

    rupture_analyses = data.get('rupture_analyses', [])
    return render_template('rupture.html',
        save_id=save_id,
        save_name=data.get('name', 'Untitled'),
        params=params,
        od=od,
        nps=params.get('nps', od_to_nps(od) or ''),
        avg_wt=round(avg_wt, 4),
        section_start=stn_min,
        section_end=stn_max,
        result=result,
        multi_result=multi_result,
        error=error,
        rup_station_inputs=rup_station_inputs,
        rup_station_input=rup_station_inputs[0] if len(rup_station_inputs) == 1 else (rup_station_inputs[0] if rup_station_inputs else ''),
        specific_weight=specific_weight,
        rupture_analyses=rupture_analyses,
        loaded_ra_id=loaded_ra_id,
        loaded_ra=loaded_ra,
    )


@app.route('/rupture/<save_id>/save', methods=['POST'])
@login_required
def rupture_save(save_id):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        abort(404)
    check_portfolio_access(data)

    col_map = data.get('col_map', {})
    file_path = data.get('file_path', '')
    if not file_path or not os.path.exists(file_path) or not validate_file_path(file_path):
        abort(400)

    sheet = col_map.get('sheet', 0)
    df = get_cached_df(file_path, sheet=sheet)
    params = data.get('params', {})
    station_col = col_map.get('station')
    elev_col = col_map.get('elev')
    wt_col = col_map.get('wt')

    df = df.copy()
    df['_stn'] = df[station_col].apply(lambda v: parse_station(v))
    df['_elev'] = pd.to_numeric(df[elev_col], errors='coerce')
    build_wt_column(df, col_map)
    df = df.dropna(subset=['_stn', '_elev', '_wt'])
    start = parse_station(params['start'])
    end = parse_station(params['end'])
    df = df[(df['_stn'] >= min(start, end)) & (df['_stn'] <= max(start, end))].copy()
    df = df.sort_values('_stn').reset_index(drop=True)
    od = get_od(params)
    avg_wt = float(df['_wt'].mean())

    stns_list = df['_stn'].tolist()
    elevs_list = df['_elev'].tolist()
    stn_min = df['_stn'].min()
    stn_max = df['_stn'].max()

    raw_stns = [s for s in request.form.getlist('rup_station') if s.strip()]
    specific_weight_str = request.form.get('specific_weight', '62.4')
    label = request.form.get('label', '').strip() or 'Unnamed'
    ra_id = request.form.get('ra_id', '').strip()
    specific_weight = 62.4
    redirect_params = {'save_id': save_id, 'specific_weight': specific_weight_str}

    try:
        specific_weight = float(specific_weight_str)
        if not raw_stns:
            raise ValueError('No rupture station provided.')
        parsed_stns = []
        for s in raw_stns:
            stn = parse_station(s.strip())
            if not (stn_min <= stn <= stn_max):
                raise ValueError(f'Rupture station {s.strip()} outside range.')
            parsed_stns.append(stn)

        if len(parsed_stns) == 1:
            result = rupture_analysis(stns_list, elevs_list, parsed_stns[0], od, avg_wt, specific_weight)
            entry_results = {
                'rup_elev': result['rup_elev'],
                'threshold_elev': result['threshold_elev'],
                'upstream_drained_ft': result['upstream_drained_ft'],
                'downstream_drained_ft': result['downstream_drained_ft'],
                'total_released_gal': result['total_released_gal'],
                'total_released_bbl': result['total_released_bbl'],
                'pct_released': result['pct_released'],
            }
            mode = 'single'
        else:
            mr = multi_rupture_analysis(stns_list, elevs_list, parsed_stns, od, avg_wt, specific_weight)
            entry_results = {
                'total_released_gal': mr['total_released_gal'],
                'total_released_bbl': mr['total_released_bbl'],
                'pct_released': mr['pct_released'],
                'per_rupture': [{
                    'rup_station': r['rup_station'],
                    'rup_elev': r['rup_elev'],
                    'total_released_gal': r['total_released_gal'],
                    'pct_released': r['pct_released'],
                } for r in mr['per_rupture']],
            }
            mode = 'multi'
    except Exception:
        for s in raw_stns:
            redirect_params['rup_station'] = s
        return redirect(url_for('rupture', **redirect_params))

    entry_id = ra_id if ra_id else uuid.uuid4().hex[:8]
    actor = session.get('user', {}).get('name') or session.get('user', {}).get('email') or 'Unknown'
    now_iso = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    entry = {
        'id': entry_id,
        'label': label,
        'mode': mode,
        'rup_station': raw_stns[0] if len(raw_stns) == 1 else raw_stns,
        'specific_weight': specific_weight,
        'saved_at': now_iso,
        'saved_by': actor,
        'results': entry_results,
    }

    if 'rupture_analyses' not in data:
        data['rupture_analyses'] = []

    if ra_id:
        replaced = False
        for i, ra in enumerate(data['rupture_analyses']):
            if ra.get('id') == ra_id:
                # Preserve original save metadata; record the update
                entry['saved_at'] = ra.get('saved_at', now_iso)
                entry['saved_by'] = ra.get('saved_by', actor)
                entry['modified_at'] = now_iso
                entry['modified_by'] = actor
                data['rupture_analyses'][i] = entry
                replaced = True
                break
        if not replaced:
            data['rupture_analyses'].append(entry)
    else:
        data['rupture_analyses'].append(entry)

    write_save(data)
    log_action('RUPTURE_SAVE', f'save={save_id} label={label} mode={mode} stns={raw_stns}')

    from urllib.parse import quote
    qs_parts = [f'specific_weight={specific_weight}', f'ra_id={entry_id}']
    for s in raw_stns:
        qs_parts.append(f'rup_station={quote(s, safe="")}')
    return redirect(f'/rupture/{save_id}?' + '&'.join(qs_parts))


@app.route('/rupture/<save_id>/rename_ra', methods=['POST'])
@login_required
def rupture_rename_ra(save_id):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        abort(404)
    check_portfolio_access(data)

    ra_id = request.form.get('ra_id', '').strip()
    new_label = request.form.get('label', '').strip()
    if ra_id and new_label:
        for ra in data.get('rupture_analyses', []):
            if ra.get('id') == ra_id:
                ra['label'] = new_label
                break
        write_save(data)
        log_action('RUPTURE_RENAME_RA', f'save={save_id} ra_id={ra_id} label={new_label}')

    return redirect(url_for('rupture', save_id=save_id))


@app.route('/rupture/<save_id>/delete_ra', methods=['POST'])
@login_required
def rupture_delete_ra(save_id):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        abort(404)
    check_portfolio_access(data)

    ra_id = request.form.get('ra_id', '').strip()
    if ra_id:
        before = len(data.get('rupture_analyses', []))
        data['rupture_analyses'] = [ra for ra in data.get('rupture_analyses', []) if ra.get('id') != ra_id]
        if len(data['rupture_analyses']) < before:
            write_save(data)
            log_action('RUPTURE_DELETE_RA', f'save={save_id} ra_id={ra_id}')

    return redirect(url_for('rupture', save_id=save_id))


@app.route('/setup/oauth', methods=['GET', 'POST'])
def oauth_setup():
    """Initial setup page when no OAuth providers are configured."""
    if get_enabled_providers() and not session.get('user'):
        return redirect(url_for('login'))
    if request.method == 'POST':
        data = {
            'name': request.form.get('name', '').strip() or 'Default Provider',
            'provider_type': request.form.get('provider_type', 'oidc'),
            'issuer_url': request.form.get('issuer_url', '').strip().rstrip('/'),
            'client_id': request.form.get('client_id', '').strip(),
            'client_secret': request.form.get('client_secret', '').strip(),
            'discovery_url': request.form.get('discovery_url', '').strip(),
            'scopes': request.form.get('scopes', 'openid email profile').strip(),
            'logout_url': request.form.get('logout_url', '').strip(),
            'app_slug': request.form.get('app_slug', '').strip(),
            'groups_claim': request.form.get('groups_claim', 'groups').strip(),
        }
        if data['client_id'] and data['client_secret']:
            add_provider(data)
            return redirect(url_for('login'))
    return render_template('oauth_setup.html')

@app.route('/project_setup', methods=['GET', 'POST'])
@login_required
def project_setup():
    portfolios = load_portfolios()
    companies = load_companies()
    governing_codes = [
        '49 CFR Part 192 (Gas)',
        '49 CFR Part 195 (Liquids)',
        'ASME B31.4 (Liquids)',
        'ASME B31.8 (Gas)',
        'Other',
    ]
    # Build spreads_by_portfolio from existing saves
    saves = load_all_saves()
    spreads_by_portfolio = {}
    for s in saves:
        pid = s.get('portfolio_id') or s.get('project_info', {}).get('portfolio_id')
        spread = (s.get('project_info') or {}).get('spread', '').strip()
        if pid and spread:
            spreads_by_portfolio.setdefault(pid, [])
            if spread not in spreads_by_portfolio[pid]:
                spreads_by_portfolio[pid].append(spread)

    if request.method == 'POST':
        pi = {
            'governing_code':     request.form.get('governing_code', '').strip(),
            'owner_company':      (request.form.get('owner_company_other') or request.form.get('owner_company', '')).strip(),
            'portfolio_id':       request.form.get('portfolio_id', '').strip() or None,  # Note: __new__ is cleared by JS; guard below handles no-JS
            'spread':             request.form.get('spread', '').strip(),
            'testing_contractor': request.form.get('testing_contractor', '').strip(),
            'company_approver': {
                'name':  request.form.get('approver_name', '').strip(),
                'phone': request.form.get('approver_phone', '').strip(),
                'email': request.form.get('approver_email', '').strip(),
            },
            'contractor_rep': {
                'name':  request.form.get('rep_name', '').strip(),
                'phone': request.form.get('rep_phone', '').strip(),
                'email': request.form.get('rep_email', '').strip(),
            },
        }
        # Guard: JS clears __new__ on submit, but handle no-JS case
        if pi['portfolio_id'] == '__new__':
            pi['portfolio_id'] = None
        new_pf_name = request.form.get('new_portfolio_name', '').strip()
        if new_pf_name and not pi['portfolio_id']:
            new_pf_id = str(uuid.uuid4())[:8]
            upsert_portfolio(new_pf_id, new_pf_name, pi.get('owner_company') or None)
            pi['portfolio_id'] = new_pf_id
        session['project_info'] = pi
        if pi.get('portfolio_id'):
            p = session.get('params', {})
            p['portfolio_id'] = pi['portfolio_id']
            session['params'] = p
        next_page = request.form.get('next', 'mapping')
        log_action('PROJECT_SETUP', f'company="{pi["owner_company"]}" code="{pi["governing_code"]}"')
        if next_page == 'results':
            return redirect(url_for('results'))
        session.pop('save_id', None)
        return redirect(url_for('mapping'))
    next_page = request.args.get('next', 'mapping')
    return render_template('project_setup.html', pi=session.get('project_info', {}),
                           portfolios=portfolios, companies=companies,
                           governing_codes=governing_codes, next_page=next_page,
                           spreads_by_portfolio=spreads_by_portfolio)

@app.route('/set_mode/demo')
@login_required
def set_demo():
    session['params'] = {
        'start': 1200495, 'end': 1218848, 'od': 42,
        'min_p': 1850, 'cfm': 12000, 'fill_gpm': 800, 'dewater_gpm': 600,
        'test_site': 1218848, 'dewater_site': 1218848, 'fill_site': 1200495, 'smys_threshold': 104,
        'min_excess': 25, 'window_upper': 50, 'grade': 'X70'
    }
    session['file_path'] = DEMO_FILE
    session['project_info'] = {
        'governing_code': '49 CFR Part 192', 'owner_company': 'Demo Company',
        'portfolio_id': None, 'testing_contractor': 'PES',
        'company_approver': {'name': '', 'phone': '', 'email': ''},
        'contractor_rep': {'name': '', 'phone': '', 'email': ''},
    }
    session.pop('save_id', None)
    session.pop('col_map', None)
    return redirect(url_for('mapping'))

@app.route('/mapping', methods=['GET', 'POST'])
@login_required
def mapping():
    if request.method == 'POST':
        if 'file' in request.files:
            file = request.files['file']
            if file.filename != '':
                # Per-session upload path to avoid concurrent user conflicts
                raw_sub = session.get('user', {}).get('sub') or str(uuid.uuid4())[:8]
                session_id = re.sub(r'[^a-zA-Z0-9_-]', '_', str(raw_sub))[:64]
                upload_path = os.path.join(UPLOADS_DIR, f'{session_id}.xlsx')
                file.save(upload_path)
                session['file_path'] = upload_path
                # Clear station-dependent params when new file is uploaded
                if 'params' in session:
                    for key in ['start', 'end', 'test_site', 'dewater_site', 'fill_site']:
                        session['params'].pop(key, None)

        col_station = request.form.get('col_station')
        col_elev = request.form.get('col_elev')
        col_wt = request.form.get('col_wt')

        if col_station and col_elev and col_wt:
            session.pop('save_id', None)
            selected_sheet = request.form.get('sheet_name', '0')
            try:
                selected_sheet = int(selected_sheet)
            except (ValueError, TypeError):
                pass  # keep as string sheet name
            new_col_map = {
                'station': col_station,
                'elev': col_elev,
                'wt': col_wt,
                'sheet': selected_sheet,
            }
            if col_wt == '__constant__':
                try:
                    new_col_map['wt_constant'] = float(request.form.get('wt_constant', 0.5))
                except (ValueError, TypeError):
                    new_col_map['wt_constant'] = 0.5
            session['col_map'] = new_col_map

            # Pre-populate params based on data after column mapping
            file_path = session.get('file_path', DEMO_FILE)
            try:
                logic = PipelineApp(file_path, sheet_name=selected_sheet)
            except Exception as e:
                return render_template('mapping.html', p=session.get('params', {}),
                                       preview=None, columns=[],
                                       upload_error=f"Could not read file: {e}")
            data = logic.full_df
            col_sta = session['col_map']['station']
            # Parse station column to numeric before finding min/max
            # (handles string stations like "1200+50" that would sort lexicographically)
            parsed_stations = data[col_sta].apply(lambda v: parse_station(v))
            min_sta = float(parsed_stations.min())
            max_sta = float(parsed_stations.max())

            p = session.get('params', {})

            # Set defaults if not present
            defaults = {
                'od': 42,
                'min_p': 1850,
                'cfm': 12000,
                'fill_gpm': 800,
                'dewater_gpm': 600,
                'smys_threshold': 104,

                'min_excess': 25,
                'window_upper': 50,
                'grade': 'X70'
            }
            for k, v in defaults.items():
                if k not in p:
                    p[k] = v

            # Update from form if provided
            if request.form.get('grade'):
                p['grade'] = request.form['grade']
            if request.form.get('nps'):
                nps_val = request.form['nps']
                od_val = nps_to_od(nps_val)
                if od_val:
                    p['nps'] = nps_val
                    p['od'] = od_val
            if request.form.get('min_p'):
                p['min_p'] = float(request.form['min_p'])

            # Set station ranges if not already set
            if 'start' not in p:
                p['start'] = min_sta
            if 'end' not in p:
                p['end'] = max_sta
            if 'test_site' not in p:
                p['test_site'] = max_sta
            if 'dewater_site' not in p:
                p['dewater_site'] = max_sta

            # Pre-populate fill_site to the start of the section if not already set
            if 'fill_site' not in p:
                p['fill_site'] = p['start']

            session['params'] = p

            return redirect(url_for('results'))

    file_path = session.get('file_path', DEMO_FILE)
    current_sheet = session.get('col_map', {}).get('sheet', 0)
    try:
        sheet_names = get_sheet_names(file_path)
    except Exception:
        sheet_names = []
    try:
        logic = PipelineApp(file_path, sheet_name=current_sheet)
        preview_html, columns = logic.get_preview()
    except Exception as e:
        preview_html, columns = None, []
        upload_error = f"Could not read file: {e}"
    else:
        upload_error = None

    # Auto-guess columns only if user hasn't already mapped them
    col_map = session.get('col_map', {})
    if columns and not col_map.get('station'):
        guesses = guess_columns(columns)
    else:
        guesses = {}

    p = session.get('params', {})
    return render_template('mapping.html', p=p, preview=preview_html, columns=columns,
                           upload_error=upload_error, sheet_names=sheet_names,
                           current_sheet=current_sheet, guesses=guesses)

def guess_columns(columns):
    """Return best-guess column names for station, elevation, wall thickness."""
    lower = [c.lower() for c in columns]
    guesses = {'station': None, 'elev': None, 'wt': None}

    # Station: prefer "station" in name, fall back to "sta"
    for priority in [['station'], ['sta']]:
        for i, lc in enumerate(lower):
            if any(kw in lc for kw in priority):
                guesses['station'] = columns[i]
                break
        if guesses['station']:
            break

    # Elevation: prefer exact "elevation", then "elev"
    for priority in [['elevation'], ['elev']]:
        for i, lc in enumerate(lower):
            if any(kw in lc for kw in priority):
                guesses['elev'] = columns[i]
                break
        if guesses['elev']:
            break

    # Wall thickness: prefer "wall thick", then "wt", then "thickness"
    for priority in [['wall thick'], ['wall_thick'], ['wt'], ['thickness']]:
        for i, lc in enumerate(lower):
            if any(kw in lc for kw in priority):
                guesses['wt'] = columns[i]
                break
        if guesses['wt']:
            break

    return guesses

@app.route('/mapping/sheet_preview')
@login_required
def sheet_preview():
    """AJAX endpoint: return preview HTML + column list for a given sheet."""
    file_path = session.get('file_path', DEMO_FILE)
    sheet = request.args.get('sheet', '0')
    try:
        sheet = int(sheet)
    except (ValueError, TypeError):
        pass
    try:
        logic = PipelineApp(file_path, sheet_name=sheet)
        preview_html, columns = logic.get_preview()
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    guesses = guess_columns(columns)
    return jsonify({'preview': preview_html, 'columns': columns, 'guesses': guesses})

@app.route('/results', methods=['GET', 'POST'])
@login_required
def results():
    p = session.get('params', {})
    col_map = session.get('col_map')
    if not col_map: return redirect(url_for('mapping'))

    if request.method == 'POST':
        form_dict = request.form.to_dict()
        for key in ['start', 'end', 'test_site', 'dewater_site', 'fill_site']:
            if key in form_dict:
                value = form_dict[key]
                if value and value != 'None':
                    try:
                        form_dict[key] = parse_station(value)
                    except:
                        form_dict[key] = p.get(key)  # Fallback to session
                else:
                    form_dict[key] = p.get(key)  # Fallback to session

        # Handle NPS → OD conversion
        if form_dict.get('nps'):
            od_val = nps_to_od(form_dict['nps'])
            if od_val:
                form_dict['nps'] = form_dict['nps']
                form_dict['od'] = od_val

        # Handle numeric fields to convert strings to floats, fallback if invalid or empty
        numeric_keys = ['fill_gpm', 'dewater_gpm', 'cfm', 'min_p', 'min_excess', 'window_upper', 'override_prepack', 'override_vent', 'override_gauge_lower', 'smys_threshold', 'unrestrained_length', 'head_factor', 'gauge_elevation']
        for key in numeric_keys:
            if key in form_dict:
                value = form_dict[key].strip() if form_dict[key] else ''
                if value:
                    try:
                        form_dict[key] = float(value)
                    except ValueError:
                        form_dict[key] = p.get(key)  # Fallback to previous if invalid
                else:
                    if key in ['override_prepack', 'override_vent', 'override_gauge_lower', 'unrestrained_length', 'gauge_elevation']:
                        form_dict[key] = None  # Clear to default when blank
                    else:
                        form_dict[key] = p.get(key)  # Keep previous if empty for non-overrides

        # Reject negative values for fields that must be non-negative
        for key in ['fill_gpm', 'dewater_gpm', 'cfm', 'min_p', 'smys_threshold', 'min_excess', 'window_upper', 'head_factor']:
            val = form_dict.get(key)
            if val is not None and isinstance(val, (int, float)) and val < 0:
                form_dict[key] = p.get(key)

        p.update(form_dict)
        session['params'] = p

    # Always ensure fill_site is set
    if 'fill_site' not in p and 'start' in p:
        p['fill_site'] = p['start']
        session['params'] = p

    # Infer NPS from OD for sessions saved before NPS was added
    if 'nps' not in p and 'od' in p:
        inferred = od_to_nps(p['od'])
        if inferred:
            p['nps'] = inferred
            session['params'] = p

    # Validate required params before calculation
    required_keys = ['od', 'min_p', 'start', 'end', 'test_site']
    missing = [k for k in required_keys if k not in p or p[k] is None]
    if missing:
        return redirect(url_for('mapping'))

    try:
        file_path = session.get('file_path', DEMO_FILE)
        grade = p.get('grade', 'X70')
        if grade not in grade_smys:
            raise ValueError(f"Unknown pipe grade '{grade}'. Valid grades: {', '.join(grade_smys.keys())}")
        smys = grade_smys[grade]
        head_factor = float(p.get('head_factor', 0.433))
        sheet = col_map.get('sheet', 0) if col_map else 0
        app_logic = PipelineApp(file_path, od=get_od(p), smys=smys, head_factor=head_factor, _df=get_cached_df(file_path, sheet=sheet))
        sec = Section(app_logic, p, col_map)

        # Moved prepack_time calc here
        atm = 14.7
        v_ft3 = sec.volume_gal / 7.4805
        compression_ratio = ((sec.prepack_psi + atm) / atm) - 1
        added_ft3 = v_ft3 * compression_ratio * 1.2  # Safety
        prepack_minutes = math.ceil(added_ft3 / float(p['cfm'])) if p.get('cfm') and float(p['cfm']) > 0 else None
        prepack_time = f"{prepack_minutes // 60}:{prepack_minutes % 60:02d}" if prepack_minutes is not None else None

        # Calculate vent_gallons
        vent_gallons = (sec.vent_gallons_total if hasattr(sec, 'vent_gallons_total') else sec.cum_gal_at_vent) if hasattr(sec, 'cum_gal_at_vent') and sec.cum_gal_at_vent is not None else sec.volume_gal

        max_smys_row = sec.table_data.loc[sec.table_data['Percent_SMYS'].idxmax()]
        max_smys_pct = max_smys_row['Percent_SMYS']
        max_smys_station = max_smys_row['Station']

        plot1, plot2 = app_logic.generate_plot(sec.table_data, min_test=float(p['min_p']) if p.get('min_p') else None, params=p, gauge_lower=sec.gauge_lower, gauge_upper=sec.gauge_upper, prepack_time=prepack_time, sec=sec, smys_threshold_pct=float(p.get('smys_threshold', 104)))
        fill_minutes = math.ceil(sec.volume_gal / float(p['fill_gpm'])) if p.get('fill_gpm') and float(p['fill_gpm']) > 0 else None
        fill_time = f"{fill_minutes // 60}:{fill_minutes % 60:02d}" if fill_minutes is not None else None
        dewater_minutes = math.ceil(sec.volume_gal / float(p['dewater_gpm'])) if p.get('dewater_gpm') and float(p['dewater_gpm']) > 0 else None
        dewater_time = f"{dewater_minutes // 60}:{dewater_minutes % 60:02d}" if dewater_minutes is not None else None
        fill_time_first = fill_time_second = None
        if hasattr(sec, 'fill_vol_second') and sec.fill_vol_second > 0 and p.get('fill_gpm') and float(p['fill_gpm']) > 0:
            gpm = float(p['fill_gpm'])
            m1 = math.ceil(sec.fill_vol_first / gpm)
            m2 = math.ceil(sec.fill_vol_second / gpm)
            fill_time_first  = f"{m1 // 60}:{m1 % 60:02d}"
            fill_time_second = f"{m2 // 60}:{m2 % 60:02d}"

        portfolios = load_portfolios()
        save_id = session.get('save_id')
        # Load version info for the current save if one is loaded
        current_save = db_load_save(save_id) if save_id else None
        # When inside a saved analysis, only show that analysis in the sidebar list
        if save_id and current_save:
            saves = [current_save]
        else:
            saves = load_all_saves()
        # Squeeze volume: gallons to pressurize from 0 to target gauge
        squeeze_vol = None
        try:
            pts = sec.points.sort_values('Station').reset_index(drop=True)
            total_len = sum(abs(pts.loc[i+1,'Station'] - pts.loc[i,'Station']) for i in range(len(pts)-1))
            weighted_wt = sum(abs(pts.loc[i+1,'Station'] - pts.loc[i,'Station']) * (pts.loc[i,'WT'] + pts.loc[i+1,'WT']) / 2 for i in range(len(pts)-1))
            avg_wt = weighted_wt / total_len if total_len > 0 else None
            if avg_wt and total_len > 0:
                od = get_od(p)
                pipe_id = od - 2 * avg_wt
                C_water = 1 / 300000
                unrestrained = float(p.get('unrestrained_length') or 0)
                unrestrained = min(unrestrained, total_len)
                restrained = total_len - unrestrained
                restraint_factor = (restrained * 0.75 + unrestrained * 1.0) / total_len
                C_pipe = restraint_factor * pipe_id / (2 * avg_wt * 30000000)
                slope = sec.volume_gal * (C_water + C_pipe)
                squeeze_vol = round(slope * sec.target_gauge, 1)
        except Exception:
            squeeze_vol = None

        save_error = request.args.get('save_error')
        restored_version = request.args.get('restored_version', type=int)
        min_bound_violations = sec.min_bound_violations
        smys_bound_violations = sec.smys_bound_violations
        # Persist param changes and notify other viewers on every POST
        if request.method == 'POST' and save_id and current_save:
            current_save['params'] = p
            try:
                write_save(current_save)
                socketio.emit('results_updated', {'save_id': save_id}, room=f'results_{save_id}')
            except Exception:
                pass
        return render_template('results.html', sec=sec, p=p, fill_time=fill_time, dew_time=dewater_time, prepack_time=prepack_time, fill_time_first=fill_time_first, fill_time_second=fill_time_second, plot1_json=json.loads(plot1), plot2_json=json.loads(plot2), vent_gallons=vent_gallons, max_smys_pct=max_smys_pct, max_smys_station=max_smys_station, portfolios=portfolios, save_id=save_id, saves=saves, current_save=current_save, save_error=save_error, restored_version=restored_version, squeeze_vol=squeeze_vol, min_bound_violations=min_bound_violations, smys_bound_violations=smys_bound_violations)
    except ValueError as ve:
        from markupsafe import escape
        return f"Input Error: {escape(str(ve))} (Check station formats or numeric values)", 400
    except Exception as e:
        app.logger.exception("Calculation error")
        return "A calculation error occurred. Check your inputs and try again.", 500

@app.route('/save', methods=['POST'])
@login_required
def save_analysis():
    p = session.get('params', {})
    col_map = session.get('col_map')
    if not col_map or not p:
        return redirect(url_for('welcome'))

    # Update name and notes from form
    name = request.form.get('analysis_name', '').strip()
    notes = request.form.get('notes', '').strip()
    if name:
        p['analysis_name'] = name
    p['notes'] = notes
    session['params'] = p

    portfolio_id = request.form.get('portfolio_id', '').strip() or None
    if not portfolio_id:
        return redirect(url_for('results', save_error='1'))

    overwrite_id = request.form.get('overwrite_id', '').strip()
    if overwrite_id and not _SAVE_ID_RE.match(overwrite_id):
        return redirect(url_for('results', save_error='1'))
    file_path = session.get('file_path', DEMO_FILE)

    old_test_data = old_test_history = None
    if overwrite_id:
        # Overwrite existing save — keep same id and data file path, push old state to history
        save_id = overwrite_id
        old = db_load_save(save_id)
        existing_data_file = None
        old_history = []
        old_version = 1
        if old:
            existing_data_file = old.get('file_path')
            old_history = old.get('history', [])
            old_version = old.get('version', 1)
            # Preserve test execution data so analysis updates don't wipe it
            old_test_data    = old.get('test_data')
            old_test_history = old.get('test_history')
            # Snapshot the old version into history
            old_history.append({
                'version': old_version,
                'timestamp': old.get('timestamp'),
                'notes': old.get('notes', ''),
                'params': old.get('params', {}),
                'col_map': old.get('col_map'),
                'project_info': old.get('project_info'),
            })

        saved_file = existing_data_file or file_path
        if file_path and file_path != DEMO_FILE and os.path.exists(file_path):
            if not existing_data_file or not os.path.exists(existing_data_file):
                saved_file = os.path.join(SAVES_DIR, f'{save_id}_data.xlsx')
                shutil.copy(file_path, saved_file)

        new_version = old_version + 1
        history = old_history
    else:
        save_id = str(uuid.uuid4())[:8]
        saved_file = file_path
        if file_path and file_path != DEMO_FILE and os.path.exists(file_path):
            saved_file = os.path.join(SAVES_DIR, f'{save_id}_data.xlsx')
            shutil.copy(file_path, saved_file)
        new_version = 1
        history = []

    p['portfolio_id'] = portfolio_id
    session['params'] = p

    # Auto-assign portfolio to user if they don't already have access
    user_sub = session.get('user', {}).get('sub')
    if user_sub and portfolio_id:
        add_portfolio_to_user(user_sub, portfolio_id)

    # Keep project_info.portfolio_id in sync
    pi = session.get('project_info', {})
    pi['portfolio_id'] = portfolio_id
    session['project_info'] = pi

    save_data = {
        'id': save_id,
        'version': new_version,
        'name': p.get('analysis_name') or 'Untitled Analysis',
        'notes': p.get('notes', ''),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'portfolio_id': portfolio_id,
        'project_info': pi,
        'params': p,
        'col_map': col_map,
        'file_path': saved_file,
        'history': history,
        'owner_sub': session.get('user', {}).get('sub'),
    }

    # Re-attach test execution data when overwriting so it survives analysis updates
    if overwrite_id:
        if old_test_data:
            save_data['test_data'] = old_test_data
        if old_test_history:
            save_data['test_history'] = old_test_history

    write_save(save_data)
    socketio.emit('results_updated', {'save_id': save_id}, room=f'results_{save_id}')

    action = 'SAVE_VERSION' if overwrite_id else 'SAVE_NEW'
    log_action(action, f'id={save_id} name="{save_data["name"]}" v{new_version}')
    session['save_id'] = save_id
    return redirect(url_for('results'))

@app.route('/load/<save_id>')
@login_required
def load_save(save_id):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        return "Save not found.", 404
    check_portfolio_access(data)
    log_action('LOAD', f'id={save_id} name="{data.get("name", "")}"')
    if 'params' not in data or 'col_map' not in data or 'file_path' not in data:
        return "Save file is incomplete or corrupted.", 400
    if not validate_file_path(data['file_path']):
        return "Save references an invalid file path.", 400
    session['params'] = data['params']
    session['col_map'] = data['col_map']
    session['file_path'] = data['file_path']
    session['save_id'] = save_id
    session['project_info'] = data.get('project_info', {})
    return redirect(url_for('results'))

@app.route('/load/<save_id>/version/<int:version_num>')
@login_required
def load_version(save_id, version_num):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        return "Save not found.", 404
    # Find the history entry for this version
    entry = next((h for h in data.get('history', []) if h['version'] == version_num), None)
    if not entry:
        return "Version not found.", 404
    if 'col_map' not in data or 'file_path' not in data:
        return "Save file is incomplete or corrupted.", 400
    if not validate_file_path(data['file_path']):
        return "Save references an invalid file path.", 400
    check_portfolio_access(data)
    log_action('LOAD_VERSION', f'id={save_id} name="{data.get("name", "")}" v{version_num}')
    # Load historical params but keep the current save's col_map and file_path
    session['params'] = entry.get('params', {})
    session['col_map'] = entry.get('col_map') or data['col_map']
    session['file_path'] = data['file_path']
    session['save_id'] = save_id
    session['project_info'] = entry.get('project_info') or data.get('project_info', {})
    return redirect(url_for('results', restored_version=version_num))

@app.route('/delete/<save_id>', methods=['POST'])
@login_required
def delete_save(save_id):
    validate_save_id(save_id)
    data_file = os.path.join(SAVES_DIR, f'{save_id}_data.xlsx')
    data = db_load_save(save_id)
    name = ''
    if data:
        check_portfolio_access(data)
        name = data.get('name', '')
        db_delete_save(save_id)
    if os.path.exists(data_file):
        os.remove(data_file)
    log_action('DELETE', f'id={save_id} name="{name}"')
    referrer = request.referrer
    if referrer and referrer.startswith(request.host_url):
        return redirect(referrer)
    return redirect(url_for('welcome'))

@app.route('/print')
@login_required
def print_view():
    p = session.get('params', {})
    col_map = session.get('col_map')
    paper_size = request.args.get('paper_size', '8.5x11')
    if paper_size not in ('8.5x11', '11x17'):
        paper_size = '8.5x11'
    orientation = request.args.get('orientation', 'portrait')
    if orientation not in ('portrait', 'landscape'):
        orientation = 'portrait'

    if not col_map or not p:
        return "No data available for printing."

    try:
        file_path = session.get('file_path', DEMO_FILE)
        grade = p.get('grade', 'X70')
        if grade not in grade_smys:
            raise ValueError(f"Unknown pipe grade '{grade}'. Valid grades: {', '.join(grade_smys.keys())}")
        smys = grade_smys[grade]
        head_factor = float(p.get('head_factor', 0.433))
        sheet = col_map.get('sheet', 0) if col_map else 0
        app_logic = PipelineApp(file_path, od=get_od(p), smys=smys, head_factor=head_factor, _df=get_cached_df(file_path, sheet=sheet))
        sec = Section(app_logic, p, col_map)

        atm = 14.7
        v_ft3 = sec.volume_gal / 7.4805
        compression_ratio = ((sec.prepack_psi + atm) / atm) - 1
        added_ft3 = v_ft3 * compression_ratio * 1.2
        prepack_minutes = math.ceil(added_ft3 / float(p['cfm'])) if p.get('cfm') and float(p['cfm']) > 0 else None
        prepack_time = f"{prepack_minutes // 60}:{prepack_minutes % 60:02d}" if prepack_minutes is not None else None

        vent_gallons = (sec.vent_gallons_total if hasattr(sec, 'vent_gallons_total') else sec.cum_gal_at_vent) if hasattr(sec, 'cum_gal_at_vent') and sec.cum_gal_at_vent is not None else sec.volume_gal

        max_smys_row = sec.table_data.loc[sec.table_data['Percent_SMYS'].idxmax()]
        max_smys_pct = max_smys_row['Percent_SMYS']
        max_smys_station = max_smys_row['Station']

        plot1, plot2 = app_logic.generate_plot(sec.table_data, min_test=float(p['min_p']) if p.get('min_p') else None, params=p, gauge_lower=sec.gauge_lower, gauge_upper=sec.gauge_upper, prepack_time=prepack_time, sec=sec, static=True, smys_threshold_pct=float(p.get('smys_threshold', 104)))
        fill_minutes = math.ceil(sec.volume_gal / float(p['fill_gpm'])) if p.get('fill_gpm') and float(p['fill_gpm']) > 0 else None
        fill_time = f"{fill_minutes // 60}:{fill_minutes % 60:02d}" if fill_minutes is not None else None
        dewater_minutes = math.ceil(sec.volume_gal / float(p['dewater_gpm'])) if p.get('dewater_gpm') and float(p['dewater_gpm']) > 0 else None
        dewater_time = f"{dewater_minutes // 60}:{dewater_minutes % 60:02d}" if dewater_minutes is not None else None
        fill_time_first = fill_time_second = None
        if hasattr(sec, 'fill_vol_second') and sec.fill_vol_second > 0 and p.get('fill_gpm') and float(p['fill_gpm']) > 0:
            gpm = float(p['fill_gpm'])
            m1 = math.ceil(sec.fill_vol_first / gpm)
            m2 = math.ceil(sec.fill_vol_second / gpm)
            fill_time_first  = f"{m1 // 60}:{m1 % 60:02d}"
            fill_time_second = f"{m2 // 60}:{m2 % 60:02d}"

        min_bound_violations = sec.min_bound_violations
        smys_bound_violations = sec.smys_bound_violations
        return render_template('print.html', sec=sec, p=p, fill_time=fill_time, dew_time=dewater_time, prepack_time=prepack_time, fill_time_first=fill_time_first, fill_time_second=fill_time_second, plot1=plot1, plot2=plot2, vent_gallons=vent_gallons, paper_size=paper_size, orientation=orientation, max_smys_pct=max_smys_pct, max_smys_station=max_smys_station, min_bound_violations=min_bound_violations, smys_bound_violations=smys_bound_violations, pi=session.get('project_info', {}))
    except Exception as e:
        app.logger.exception("Print view error")
        return "Error generating print view. Check your inputs and try again.", 500

@app.route('/pv/<save_id>')
@login_required
def pv_plot(save_id):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        return "Save not found.", 404
    check_portfolio_access(data)
    p = data.get('params', {})
    total_volume_gal = None
    avg_wt = None
    min_wt = None
    try:
        file_path = data.get('file_path', DEMO_FILE)
        pv_col_map = data.get('col_map', {})
        smys = grade_smys.get(p.get('grade', 'X70'), 70000)
        head_factor = float(p.get('head_factor', 0.433))
        sheet = pv_col_map.get('sheet', 0)
        app_logic = PipelineApp(file_path, od=get_od(p), smys=smys, head_factor=head_factor, _df=get_cached_df(file_path, sheet=sheet))
        sec = Section(app_logic, p, pv_col_map)
        total_volume_gal = round(sec.volume_gal, 1)
        # Length-weighted average wall thickness (same weighting as volume calc)
        pts = sec.points.sort_values('Station').reset_index(drop=True)
        total_len = 0.0
        weighted_wt = 0.0
        for i in range(len(pts) - 1):
            seg_len = abs(pts.loc[i + 1, 'Station'] - pts.loc[i, 'Station'])
            seg_wt = (pts.loc[i, 'WT'] + pts.loc[i + 1, 'WT']) / 2
            weighted_wt += seg_len * seg_wt
            total_len += seg_len
        if total_len > 0:
            avg_wt = round(weighted_wt / total_len, 4)
        min_wt = round(float(pts['WT'].min()), 4)
        total_length_ft = round(sec.length, 0)
        target_gauge = sec.target_gauge
        gauge_lower = sec.gauge_lower
        gauge_upper = sec.gauge_upper
    except Exception:
        total_length_ft = None
        target_gauge = None
        gauge_lower = None
        gauge_upper = None
    smys = grade_smys.get(p.get('grade', 'X70'), 70000)
    portfolios = load_portfolios()
    pf_name = next((pf['name'] for pf in portfolios if pf['id'] == data.get('portfolio_id')), None)
    return render_template('pv.html',
        save_id=save_id,
        save_name=data.get('name', 'Untitled'),
        portfolio_name=pf_name,
        p=p,
        total_volume_gal=total_volume_gal,
        avg_wt=avg_wt,
        min_wt=min_wt,
        total_length_ft=total_length_ft,
        smys=smys,
        target_gauge=target_gauge,
        gauge_lower=gauge_lower,
        gauge_upper=gauge_upper,
        pv_data=data.get('pv_data', {}),
        unrestrained_length=p.get('unrestrained_length') or 0,
        from_exec=request.args.get('from') == 'exec',
    )

@app.route('/pv/<save_id>/save', methods=['POST'])
@login_required
def pv_save(save_id):
    validate_save_id(save_id)
    # Limit request body size for PV data (5 MB max)
    if request.content_length and request.content_length > 5 * 1024 * 1024:
        return jsonify({'error': 'Payload too large'}), 413
    data = db_load_save(save_id)
    if data is None:
        return jsonify({'error': 'Save not found'}), 404
    check_portfolio_access(data)
    payload = request.get_json()
    if payload is None:
        return jsonify({'error': 'Invalid or missing JSON body'}), 400
    # Basic schema validation
    if not isinstance(payload.get('readings'), list):
        return jsonify({'error': 'Missing or invalid readings array'}), 400
    payload['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    data['pv_data'] = payload
    write_save(data)
    rows = len(payload.get('readings', []))
    log_action('PV_SAVE', f'id={save_id} name="{data.get("name", "")}" rows={rows}')
    return jsonify({'status': 'ok'})

@app.route('/test/<save_id>')
@login_required
def test_execution(save_id):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        return "Save not found.", 404
    check_portfolio_access(data)
    p = data.get('params', {})
    avg_wt = None
    k_factor = None
    target_gauge = gauge_lower = gauge_upper = None
    total_length_ft = None
    test_site_elev = high_elev = low_elev = None
    try:
        file_path = data.get('file_path', DEMO_FILE)
        col_map = data.get('col_map', {})
        smys = grade_smys.get(p.get('grade', 'X70'), 70000)
        head_factor = float(p.get('head_factor', 0.433))
        sheet = col_map.get('sheet', 0)
        app_logic = PipelineApp(file_path, od=get_od(p), smys=smys, head_factor=head_factor,
                                _df=get_cached_df(file_path, sheet=sheet))
        sec = Section(app_logic, p, col_map)
        pts = sec.points.sort_values('Station').reset_index(drop=True)
        total_len = 0.0; weighted_wt = 0.0
        for i in range(len(pts) - 1):
            seg_len = abs(float(pts.loc[i+1, 'Station']) - float(pts.loc[i, 'Station']))
            seg_wt  = (float(pts.loc[i, 'WT']) + float(pts.loc[i+1, 'WT'])) / 2
            weighted_wt += seg_len * seg_wt
            total_len   += seg_len
        if total_len > 0:
            avg_wt = round(weighted_wt / total_len, 4)
        total_length_ft = round(sec.length, 0)
        target_gauge = sec.target_gauge
        gauge_lower  = sec.gauge_lower
        gauge_upper  = sec.gauge_upper
        if avg_wt:
            k_factor = temp_correction_factor(get_od(p), avg_wt)
        # Elevations for MAOP certification
        test_site_station = float(p.get('test_site', 0))
        dists = (pts['Station'] - test_site_station).abs()
        test_site_elev = round(float(pts.loc[dists.idxmin(), 'Elevation']), 1)
        high_elev = round(float(pts['Elevation'].max()), 1)
        low_elev  = round(float(pts['Elevation'].min()), 1)
    except Exception:
        pass
    gauge_elev = float(p['gauge_elevation']) if p.get('gauge_elevation') is not None else test_site_elev
    portfolios = load_portfolios()
    pf_name = next((pf['name'] for pf in portfolios if pf['id'] == data.get('portfolio_id')), None)
    td = data.get('test_data', {})
    test_attempt = len(data.get('test_history', [])) + 1
    return render_template('test_exec.html',
        save_id=save_id,
        save_name=data.get('name', 'Untitled'),
        portfolio_name=pf_name,
        p=p,
        avg_wt=avg_wt,
        total_length_ft=total_length_ft,
        k_factor=k_factor,
        target_gauge=target_gauge,
        gauge_lower=gauge_lower,
        gauge_upper=gauge_upper,
        test_data=td,
        test_status=td.get('status'),
        test_attempt=test_attempt,
        test_site_elev=test_site_elev,
        gauge_elev=gauge_elev,
        high_elev=high_elev,
        low_elev=low_elev,
        governing_code=data.get('project_info', {}).get('governing_code', ''),
        pv_data=data.get('pv_data', {}),
        equipment_data=data.get('equipment_data', {}),
    )


@app.route('/test/<save_id>/save', methods=['POST'])
@login_required
def test_save(save_id):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        return jsonify({'error': 'Save not found'}), 404
    check_portfolio_access(data)
    payload = request.get_json()
    if payload is None:
        return jsonify({'error': 'Invalid JSON'}), 400
    if not isinstance(payload.get('readings'), list):
        return jsonify({'error': 'Missing readings array'}), 400
    if data.get('test_data', {}).get('status') in ('pass', 'fail'):
        return jsonify({'error': 'Test is finalized', 'code': 'finalized'}), 409
    payload['last_updated'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    data['test_data'] = payload
    write_save(data)
    socketio.emit('readings_updated', {
        'readings':     payload.get('readings', []),
        'last_updated': payload['last_updated'],
        'client_id':    payload.get('client_id'),
    }, room=f'test_{save_id}')
    log_action('TEST_SAVE', f'id={save_id} rows={len(payload["readings"])}')
    return jsonify({'status': 'ok'})


@app.route('/test/<save_id>/finalize', methods=['POST'])
@login_required
def test_finalize(save_id):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        return jsonify({'error': 'Save not found'}), 404
    check_portfolio_access(data)
    payload = request.get_json()
    status = (payload or {}).get('status')
    if status not in ('pass', 'fail'):
        return jsonify({'error': 'Invalid status'}), 400
    td = data.get('test_data', {})
    if td.get('status') in ('pass', 'fail'):
        return jsonify({'error': 'Already finalized'}), 409
    td['status'] = status
    td['finalized_at'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    data['test_data'] = td
    write_save(data)
    log_action('TEST_FINALIZE', f'id={save_id} status={status}')
    return jsonify({'status': 'ok'})


@app.route('/test/<save_id>/unlock', methods=['POST'])
@admin_required
def test_unlock(save_id):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        return jsonify({'error': 'Save not found'}), 404
    check_portfolio_access(data)
    td = data.get('test_data', {})
    if td.get('status') not in ('pass', 'fail'):
        return jsonify({'error': 'Test is not finalized'}), 409
    td.pop('status', None)
    td.pop('finalized_at', None)
    data['test_data'] = td
    write_save(data)
    log_action('TEST_UNLOCK', f'id={save_id} by={session["user"].get("email")}')
    return jsonify({'status': 'ok'})


@app.route('/test/<save_id>/pv/unlink', methods=['POST'])
@login_required
def test_pv_unlink(save_id):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        return jsonify({'error': 'Save not found'}), 404
    check_portfolio_access(data)
    if data.get('test_data', {}).get('status') in ('pass', 'fail'):
        return jsonify({'error': 'Test is finalized — unlock before unlinking PV data'}), 409
    data.pop('pv_data', None)
    write_save(data)
    log_action('PV_UNLINK', f'id={save_id}')
    return jsonify({'status': 'ok'})


@app.route('/test/<save_id>/retest', methods=['POST'])
@login_required
def test_retest(save_id):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        return jsonify({'error': 'Save not found'}), 404
    check_portfolio_access(data)
    td = data.get('test_data', {})
    if td.get('status') != 'fail':
        return jsonify({'error': 'Can only retest a failed test'}), 400
    history = data.get('test_history', [])
    history.insert(0, td)
    data['test_history'] = history
    data['test_data'] = {
        'readings': [],
        'test_date': None,
        'timezone': td.get('timezone'),
        'last_updated': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
    }
    write_save(data)
    log_action('TEST_RETEST', f'id={save_id} attempt={len(history) + 1}')
    return jsonify({'status': 'ok'})


@app.route('/equipment/<save_id>', methods=['GET', 'POST'])
@login_required
def equipment_setup(save_id):
    validate_save_id(save_id)
    data = db_load_save(save_id)
    if data is None:
        return "Save not found.", 404
    check_portfolio_access(data)

    INSTRUMENT_TYPES = [
        ('Pressure Recorder', 'rec',    '%'),
        ('Pressure Gauge',    'gauge',  '%'),
        ('Deadweights',       'dw',     '%'),
        ('Ambient Temp',      'amb',    '°F'),
        ('Pipe Temp',         'pipe',   '°F'),
        ('Ground Temp',       'ground', '°F'),
    ]

    if request.method == 'POST':
        eq = {}
        for _label, key, unit in INSTRUMENT_TYPES:
            eq[key] = {
                'primary': {
                    'serial':        request.form.get(f'{key}_pri_serial', '').strip(),
                    'cal_date':      request.form.get(f'{key}_pri_cal_date', '').strip(),
                    'exp_date':      request.form.get(f'{key}_pri_exp_date', '').strip(),
                    'accuracy':      request.form.get(f'{key}_pri_accuracy', '').strip(),
                    'accuracy_unit': request.form.get(f'{key}_pri_accuracy_unit', unit),
                    'station':       request.form.get(f'{key}_pri_station', '').strip(),
                    'skip':          bool(request.form.get(f'{key}_pri_skip')),
                },
                'secondary': {
                    'serial':        request.form.get(f'{key}_sec_serial', '').strip(),
                    'cal_date':      request.form.get(f'{key}_sec_cal_date', '').strip(),
                    'exp_date':      request.form.get(f'{key}_sec_exp_date', '').strip(),
                    'accuracy':      request.form.get(f'{key}_sec_accuracy', '').strip(),
                    'accuracy_unit': request.form.get(f'{key}_sec_accuracy_unit', unit),
                    'station':       request.form.get(f'{key}_sec_station', '').strip(),
                    'skip':          bool(request.form.get(f'{key}_sec_skip')),
                },
            }
        data['equipment_data'] = eq
        write_save(data)
        log_action('EQUIPMENT_SAVE', f'id={save_id}')
        return redirect(url_for('test_execution', save_id=save_id))

    portfolios = load_portfolios()
    pf_name = next((pf['name'] for pf in portfolios if pf['id'] == data.get('portfolio_id')), None)
    eq = data.get('equipment_data', {})
    return render_template('equipment.html',
        save_id=save_id,
        save_name=data.get('name', 'Untitled'),
        portfolio_name=pf_name,
        eq=eq,
        instrument_types=INSTRUMENT_TYPES,
    )


@socketio.on('join_test')
def handle_join(data):
    save_id = data.get('save_id', '')
    validate_save_id(save_id)
    join_room(f'test_{save_id}')

@socketio.on('leave_test')
def handle_leave(data):
    save_id = data.get('save_id', '')
    leave_room(f'test_{save_id}')

@socketio.on('join_results')
def handle_join_results(data):
    save_id = data.get('save_id', '')
    validate_save_id(save_id)
    join_room(f'results_{save_id}')

@socketio.on('leave_results')
def handle_leave_results(data):
    save_id = data.get('save_id', '')
    leave_room(f'results_{save_id}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='PES Hydrotest Tool')
    parser.add_argument('--dev', action='store_true', help='Dev mode: use ref/ folder for reference docs')
    args = parser.parse_args()

    if args.dev:
        DEV_MODE = True
        DOCS_DIR = os.path.join(_APP_DIR, 'ref')
        logger.info('DEV MODE — reference docs: ref/')

    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
