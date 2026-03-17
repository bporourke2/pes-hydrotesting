from flask import Flask, render_template, request, session, redirect, url_for, jsonify, abort
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
from logic import PipelineApp, Section, parse_station, station_format

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

load_dotenv()

_APP_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
Compress(app)

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

SAVES_DIR = os.path.join(_APP_DIR, 'saves')
UPLOADS_DIR = os.path.join(_APP_DIR, 'uploads')
DEMO_FILE = os.path.join(_APP_DIR, 'data', 'Testdata.xlsx')
os.makedirs(SAVES_DIR, exist_ok=True)
os.makedirs(UPLOADS_DIR, exist_ok=True)

grade_smys = {
    'B': 35000,
    'X42': 42000,
    'X52': 52000,
    'X60': 60000,
    'X65': 65000,
    'X70': 70000,
    'X80': 80000
}

PORTFOLIOS_FILE = os.path.join(SAVES_DIR, '_portfolios.json')
COMPANIES_FILE  = os.path.join(SAVES_DIR, '_companies.json')

def load_portfolios():
    if os.path.exists(PORTFOLIOS_FILE):
        try:
            with open(PORTFOLIOS_FILE) as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
        except Exception:
            # Backup corrupted file before returning empty
            backup = PORTFOLIOS_FILE + '.corrupt'
            if not os.path.exists(backup):
                try:
                    shutil.copy(PORTFOLIOS_FILE, backup)
                except Exception:
                    pass
            logging.warning(f"Corrupted {PORTFOLIOS_FILE} — backed up to .corrupt")
    return []

def save_portfolios(portfolios):
    safe_write_json(PORTFOLIOS_FILE, portfolios)

def load_companies():
    if os.path.exists(COMPANIES_FILE):
        try:
            with open(COMPANIES_FILE) as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
        except Exception:
            backup = COMPANIES_FILE + '.corrupt'
            if not os.path.exists(backup):
                try:
                    shutil.copy(COMPANIES_FILE, backup)
                except Exception:
                    pass
            logging.warning(f"Corrupted {COMPANIES_FILE} — backed up to .corrupt")
    return []

def save_companies(companies):
    safe_write_json(COMPANIES_FILE, companies)

def load_all_saves():
    saves = []
    if os.path.exists(SAVES_DIR):
        for fname in sorted(os.listdir(SAVES_DIR), reverse=True):
            if fname.endswith('.json') and not fname.startswith('_'):
                try:
                    with open(os.path.join(SAVES_DIR, fname)) as f:
                        saves.append(json.load(f))
                except Exception:
                    pass
    return saves

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
    # tree_raw[company][pf_id][spread] = [saves]
    tree_raw = OrderedDict()
    for s in saves:
        pi = s.get('project_info') or {}
        company = pi.get('owner_company', '').strip() or 'No Company'
        pf_id   = s.get('portfolio_id') or '__none__'
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
            portfolios.append({
                'id': new_id,
                'name': name,
                'company': request.form.get('company', '').strip() or None,
            })
            save_portfolios(portfolios)
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
    for pf in portfolios:
        if pf['id'] == portfolio_id:
            pf['name']    = request.form.get('name', pf['name']).strip() or pf['name']
            pf['company'] = request.form.get('company', '').strip() or None
            break
    save_portfolios(portfolios)
    return redirect(url_for('settings'))

@app.route('/portfolio/delete/<portfolio_id>', methods=['POST'])
@admin_required
def portfolio_delete(portfolio_id):
    validate_save_id(portfolio_id)
    portfolios = load_portfolios()
    portfolios = [p for p in portfolios if p['id'] != portfolio_id]
    save_portfolios(portfolios)
    # Unassign any saves that belonged to this portfolio
    for fname in os.listdir(SAVES_DIR):
        if fname.endswith('.json') and not fname.startswith('_'):
            fpath = os.path.join(SAVES_DIR, fname)
            try:
                with open(fpath) as f:
                    save = json.load(f)
                if save.get('portfolio_id') == portfolio_id:
                    save['portfolio_id'] = None
                    safe_write_json(fpath, save)
            except Exception:
                pass
    # Remove portfolio from all user access lists
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
                           users=load_users())

@app.route('/settings/company/add', methods=['POST'])
@login_required
def company_add():
    name = request.form.get('name', '').strip()
    if name:
        companies = load_companies()
        if name not in companies:
            companies.append(name)
            companies.sort(key=str.lower)
            save_companies(companies)
            log_action('COMPANY_ADD', name)
    return redirect(url_for('settings'))

@app.route('/settings/company/delete', methods=['POST'])
@admin_required
def company_delete():
    name = request.form.get('name', '').strip()
    if name:
        companies = load_companies()
        companies = [c for c in companies if c != name]
        save_companies(companies)
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
            new_pf = {'id': str(uuid.uuid4())[:8], 'name': new_pf_name, 'company': pi.get('owner_company') or None}
            portfolios.append(new_pf)
            save_portfolios(portfolios)
            pi['portfolio_id'] = new_pf['id']
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
        'test_site': 1218848, 'dewater_site': 1218848, 'smys_threshold': 104, 'fill_direction': '1',
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
            session['col_map'] = {
                'station': col_station,
                'elev': col_elev,
                'wt': col_wt,
                'sheet': selected_sheet,
            }

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
                'fill_direction': '1',
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
            if request.form.get('od'):
                p['od'] = float(request.form['od'])
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

            # Pre-populate fill_site as the lowest station (min_sta), but respect fill_direction
            if p['fill_direction'] == '1':
                p['fill_site'] = min(p['start'], p['end'])  # Lowest assuming start < end, but general
            else:
                p['fill_site'] = max(p['start'], p['end'])

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
        for key in ['start', 'end', 'test_site', 'dewater_site']:
            if key in form_dict:
                value = form_dict[key]
                if value and value != 'None':
                    try:
                        form_dict[key] = parse_station(value)
                    except:
                        form_dict[key] = p.get(key)  # Fallback to session
                else:
                    form_dict[key] = p.get(key)  # Fallback to session

        # Handle numeric fields to convert strings to floats, fallback if invalid or empty
        numeric_keys = ['fill_gpm', 'dewater_gpm', 'cfm', 'od', 'min_p', 'min_excess', 'window_upper', 'override_prepack', 'override_vent', 'smys_threshold', 'unrestrained_length', 'head_factor']
        for key in numeric_keys:
            if key in form_dict:
                value = form_dict[key].strip() if form_dict[key] else ''
                if value:
                    try:
                        form_dict[key] = float(value)
                    except ValueError:
                        form_dict[key] = p.get(key)  # Fallback to previous if invalid
                else:
                    if key in ['override_prepack', 'override_vent', 'unrestrained_length']:
                        form_dict[key] = None  # Clear to default when blank
                    else:
                        form_dict[key] = p.get(key)  # Keep previous if empty for non-overrides

        # Reject negative values for fields that must be non-negative
        for key in ['fill_gpm', 'dewater_gpm', 'cfm', 'od', 'min_p', 'smys_threshold', 'min_excess', 'window_upper', 'head_factor']:
            val = form_dict.get(key)
            if val is not None and isinstance(val, (int, float)) and val < 0:
                form_dict[key] = p.get(key)

        # Auto-set fill_site based on direction (overrides pre-populated value)
        if 'fill_direction' in form_dict:
            s = form_dict.get('start', p.get('start'))
            e = form_dict.get('end', p.get('end'))
            if s is not None and e is not None:
                if form_dict['fill_direction'] == '1':
                    form_dict['fill_site'] = min(s, e)
                else:
                    form_dict['fill_site'] = max(s, e)
            elif form_dict['fill_direction'] == '1':
                form_dict['fill_site'] = s
            else:
                form_dict['fill_site'] = e
        p.update(form_dict)
        session['params'] = p

    # Always ensure fill_site is set (for GET or if missing after POST)
    if 'fill_site' not in p and 'fill_direction' in p and 'start' in p and 'end' in p:
        if p['fill_direction'] == '1':
            p['fill_site'] = min(p['start'], p['end'])
        else:
            p['fill_site'] = max(p['start'], p['end'])
        session['params'] = p  # Persist the update

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
        app_logic = PipelineApp(file_path, od=float(p["od"]), smys=smys, head_factor=head_factor, _df=get_cached_df(file_path, sheet=sheet))
        sec = Section(app_logic, p, col_map)

        # Moved prepack_time calc here
        atm = 14.7
        v_ft3 = sec.volume_gal / 7.4805
        compression_ratio = ((sec.prepack_psi + atm) / atm) - 1
        added_ft3 = v_ft3 * compression_ratio * 1.2  # Safety
        prepack_minutes = math.ceil(added_ft3 / float(p['cfm'])) if p.get('cfm') and float(p['cfm']) > 0 else None
        prepack_time = f"{prepack_minutes // 60}:{prepack_minutes % 60:02d}" if prepack_minutes is not None else None

        # Calculate vent_gallons
        vent_gallons = sec.cum_gal_at_vent if hasattr(sec, 'cum_gal_at_vent') and sec.cum_gal_at_vent is not None else sec.volume_gal

        max_smys_row = sec.table_data.loc[sec.table_data['Percent_SMYS'].idxmax()]
        max_smys_pct = max_smys_row['Percent_SMYS']
        max_smys_station = max_smys_row['Station']

        plot1, plot2 = app_logic.generate_plot(sec.table_data, min_test=float(p['min_p']) if p.get('min_p') else None, params=p, gauge_lower=sec.gauge_lower, gauge_upper=sec.gauge_upper, prepack_time=prepack_time, sec=sec, smys_threshold_pct=float(p.get('smys_threshold', 104)))
        fill_minutes = math.ceil(sec.volume_gal / float(p['fill_gpm'])) if p.get('fill_gpm') and float(p['fill_gpm']) > 0 else None
        fill_time = f"{fill_minutes // 60}:{fill_minutes % 60:02d}" if fill_minutes is not None else None
        dewater_minutes = math.ceil(sec.volume_gal / float(p['dewater_gpm'])) if p.get('dewater_gpm') and float(p['dewater_gpm']) > 0 else None
        dewater_time = f"{dewater_minutes // 60}:{dewater_minutes % 60:02d}" if dewater_minutes is not None else None

        portfolios = load_portfolios()
        save_id = session.get('save_id')
        saves = load_all_saves()
        # Load version info for the current save if one is loaded
        current_save = None
        if save_id:
            sf = os.path.join(SAVES_DIR, f'{save_id}.json')
            if os.path.exists(sf):
                with open(sf) as f:
                    current_save = json.load(f)
        # Squeeze volume: gallons to pressurize from 0 to target gauge
        squeeze_vol = None
        try:
            pts = sec.points.sort_values('Station').reset_index(drop=True)
            total_len = sum(abs(pts.loc[i+1,'Station'] - pts.loc[i,'Station']) for i in range(len(pts)-1))
            weighted_wt = sum(abs(pts.loc[i+1,'Station'] - pts.loc[i,'Station']) * (pts.loc[i,'WT'] + pts.loc[i+1,'WT']) / 2 for i in range(len(pts)-1))
            avg_wt = weighted_wt / total_len if total_len > 0 else None
            if avg_wt and total_len > 0:
                od = float(p['od'])
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
        return render_template('results.html', sec=sec, p=p, fill_time=fill_time, dew_time=dewater_time, prepack_time=prepack_time, plot1_json=json.loads(plot1), plot2_json=json.loads(plot2), vent_gallons=vent_gallons, max_smys_pct=max_smys_pct, max_smys_station=max_smys_station, portfolios=portfolios, save_id=save_id, saves=saves, current_save=current_save, save_error=save_error, restored_version=restored_version, squeeze_vol=squeeze_vol, min_bound_violations=min_bound_violations, smys_bound_violations=smys_bound_violations)
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

    if overwrite_id:
        # Overwrite existing save — keep same id and data file path, push old state to history
        save_id = overwrite_id
        existing_file = os.path.join(SAVES_DIR, f'{save_id}.json')
        existing_data_file = None
        old_history = []
        old_version = 1
        if os.path.exists(existing_file):
            with open(existing_file) as f:
                old = json.load(f)
            existing_data_file = old.get('file_path')
            old_history = old.get('history', [])
            old_version = old.get('version', 1)
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

    safe_write_json(os.path.join(SAVES_DIR, f'{save_id}.json'), save_data)

    action = 'SAVE_VERSION' if overwrite_id else 'SAVE_NEW'
    log_action(action, f'id={save_id} name="{save_data["name"]}" v{new_version}')
    session['save_id'] = save_id
    return redirect(url_for('results'))

@app.route('/load/<save_id>')
@login_required
def load_save(save_id):
    validate_save_id(save_id)
    save_file = os.path.join(SAVES_DIR, f'{save_id}.json')
    if not os.path.exists(save_file):
        return "Save not found.", 404
    with open(save_file) as f:
        data = json.load(f)
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
    save_file = os.path.join(SAVES_DIR, f'{save_id}.json')
    if not os.path.exists(save_file):
        return "Save not found.", 404
    with open(save_file) as f:
        data = json.load(f)
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
    save_file = os.path.join(SAVES_DIR, f'{save_id}.json')
    data_file = os.path.join(SAVES_DIR, f'{save_id}_data.xlsx')
    name = ''
    if os.path.exists(save_file):
        with open(save_file) as f:
            data = json.load(f)
        check_portfolio_access(data)
        name = data.get('name', '')
        os.remove(save_file)
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
        app_logic = PipelineApp(file_path, od=float(p["od"]), smys=smys, head_factor=head_factor, _df=get_cached_df(file_path, sheet=sheet))
        sec = Section(app_logic, p, col_map)

        atm = 14.7
        v_ft3 = sec.volume_gal / 7.4805
        compression_ratio = ((sec.prepack_psi + atm) / atm) - 1
        added_ft3 = v_ft3 * compression_ratio * 1.2
        prepack_minutes = math.ceil(added_ft3 / float(p['cfm'])) if p.get('cfm') and float(p['cfm']) > 0 else None
        prepack_time = f"{prepack_minutes // 60}:{prepack_minutes % 60:02d}" if prepack_minutes is not None else None

        vent_gallons = sec.cum_gal_at_vent if hasattr(sec, 'cum_gal_at_vent') and sec.cum_gal_at_vent is not None else sec.volume_gal

        max_smys_row = sec.table_data.loc[sec.table_data['Percent_SMYS'].idxmax()]
        max_smys_pct = max_smys_row['Percent_SMYS']
        max_smys_station = max_smys_row['Station']

        plot1, plot2 = app_logic.generate_plot(sec.table_data, min_test=float(p['min_p']) if p.get('min_p') else None, params=p, gauge_lower=sec.gauge_lower, gauge_upper=sec.gauge_upper, prepack_time=prepack_time, sec=sec, static=True, smys_threshold_pct=float(p.get('smys_threshold', 104)))
        fill_minutes = math.ceil(sec.volume_gal / float(p['fill_gpm'])) if p.get('fill_gpm') and float(p['fill_gpm']) > 0 else None
        fill_time = f"{fill_minutes // 60}:{fill_minutes % 60:02d}" if fill_minutes is not None else None
        dewater_minutes = math.ceil(sec.volume_gal / float(p['dewater_gpm'])) if p.get('dewater_gpm') and float(p['dewater_gpm']) > 0 else None
        dewater_time = f"{dewater_minutes // 60}:{dewater_minutes % 60:02d}" if dewater_minutes is not None else None

        min_bound_violations = sec.min_bound_violations
        smys_bound_violations = sec.smys_bound_violations
        return render_template('print.html', sec=sec, p=p, fill_time=fill_time, dew_time=dewater_time, prepack_time=prepack_time, plot1=plot1, plot2=plot2, vent_gallons=vent_gallons, paper_size=paper_size, orientation=orientation, max_smys_pct=max_smys_pct, max_smys_station=max_smys_station, min_bound_violations=min_bound_violations, smys_bound_violations=smys_bound_violations, pi=session.get('project_info', {}))
    except Exception as e:
        app.logger.exception("Print view error")
        return "Error generating print view. Check your inputs and try again.", 500

@app.route('/pv/<save_id>')
@login_required
def pv_plot(save_id):
    validate_save_id(save_id)
    save_file = os.path.join(SAVES_DIR, f'{save_id}.json')
    if not os.path.exists(save_file):
        return "Save not found.", 404
    with open(save_file) as f:
        data = json.load(f)
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
        app_logic = PipelineApp(file_path, od=float(p["od"]), smys=smys, head_factor=head_factor, _df=get_cached_df(file_path, sheet=sheet))
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
        target_gauge = round(sec.target_gauge, 0)
        gauge_lower = round(sec.gauge_lower, 0)
        gauge_upper = round(sec.gauge_upper, 0)
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
    )

@app.route('/pv/<save_id>/save', methods=['POST'])
@login_required
def pv_save(save_id):
    validate_save_id(save_id)
    # Limit request body size for PV data (5 MB max)
    if request.content_length and request.content_length > 5 * 1024 * 1024:
        return jsonify({'error': 'Payload too large'}), 413
    save_file = os.path.join(SAVES_DIR, f'{save_id}.json')
    if not os.path.exists(save_file):
        return jsonify({'error': 'Save not found'}), 404
    with open(save_file) as f:
        data = json.load(f)
    check_portfolio_access(data)
    payload = request.get_json()
    if payload is None:
        return jsonify({'error': 'Invalid or missing JSON body'}), 400
    # Basic schema validation
    if not isinstance(payload.get('readings'), list):
        return jsonify({'error': 'Missing or invalid readings array'}), 400
    payload['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    data['pv_data'] = payload
    safe_write_json(save_file, data)
    rows = len(payload.get('readings', []))
    log_action('PV_SAVE', f'id={save_id} name="{data.get("name", "")}" rows={rows}')
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
