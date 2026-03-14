from flask import Flask, render_template, request, session, redirect, url_for, jsonify
from flask_compress import Compress
from werkzeug.middleware.proxy_fix import ProxyFix
from authlib.integrations.flask_client import OAuth
from dotenv import load_dotenv
from functools import wraps
import logging
import math
import json
import uuid
import shutil
import os
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

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
Compress(app)

# Survey DataFrame cache: {(file_path, mtime): DataFrame}
_df_cache = {}

def get_cached_df(file_path):
    try:
        mtime = os.path.getmtime(file_path)
    except OSError:
        return None
    key = (file_path, mtime)
    if key not in _df_cache:
        if len(_df_cache) > 20:
            _df_cache.clear()
        import pandas as pd
        _df_cache[key] = pd.read_excel(file_path, sheet_name=0)
    return _df_cache[key]
app.secret_key = os.environ.get('SECRET_KEY', 'hydrotest_v2_key_dev_only')

# --- Authentik OIDC ---
_client_id     = os.environ.get('AUTHENTIK_CLIENT_ID')
_client_secret = os.environ.get('AUTHENTIK_CLIENT_SECRET')
_app_slug      = os.environ.get('AUTHENTIK_APP_SLUG', 'hydrotest')
_authentik_base = 'https://auth.thebrendan.online'

oauth = OAuth(app)
oauth.register(
    name='authentik',
    client_id=_client_id,
    client_secret=_client_secret,
    server_metadata_url=f'{_authentik_base}/application/o/{_app_slug}/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'},
)

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user'):
            session['next'] = request.url
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

@app.route('/login')
def login():
    callback_url = url_for('auth_callback', _external=True)
    return oauth.authentik.authorize_redirect(callback_url)

@app.route('/auth/callback')
def auth_callback():
    token = oauth.authentik.authorize_access_token()
    userinfo = token.get('userinfo') or oauth.authentik.userinfo()
    session['user'] = {
        'sub':   userinfo.get('sub'),
        'email': userinfo.get('email'),
        'name':  userinfo.get('name') or userinfo.get('preferred_username'),
    }
    next_url = session.pop('next', None)
    log_action('LOGIN', session['user'].get('email', ''))
    return redirect(next_url or url_for('welcome'))

@app.route('/logout')
def logout():
    log_action('LOGOUT')
    session.clear()
    # Redirect to Authentik end-session endpoint
    return redirect(f'{_authentik_base}/application/o/{_app_slug}/end-session/')

app.jinja_env.filters['station_format'] = station_format

SAVES_DIR = 'saves'
os.makedirs(SAVES_DIR, exist_ok=True)

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
                return json.load(f)
        except Exception:
            pass
    return []

def save_portfolios(portfolios):
    with open(PORTFOLIOS_FILE, 'w') as f:
        json.dump(portfolios, f)

def load_companies():
    if os.path.exists(COMPANIES_FILE):
        try:
            with open(COMPANIES_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return []

def save_companies(companies):
    with open(COMPANIES_FILE, 'w') as f:
        json.dump(companies, f)

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
        portfolios.append({
            'id': str(uuid.uuid4())[:8],
            'name': name,
            'company': request.form.get('company', '').strip() or None,
        })
        save_portfolios(portfolios)
    next_page = request.form.get('next', 'welcome')
    return redirect(url_for(next_page))

@app.route('/portfolio/edit/<portfolio_id>', methods=['POST'])
@login_required
def portfolio_edit(portfolio_id):
    portfolios = load_portfolios()
    for pf in portfolios:
        if pf['id'] == portfolio_id:
            pf['name']    = request.form.get('name', pf['name']).strip() or pf['name']
            pf['company'] = request.form.get('company', '').strip() or None
            break
    save_portfolios(portfolios)
    return redirect(url_for('settings'))

@app.route('/portfolio/delete/<portfolio_id>', methods=['POST'])
@login_required
def portfolio_delete(portfolio_id):
    portfolios = load_portfolios()
    portfolios = [p for p in portfolios if p['id'] != portfolio_id]
    save_portfolios(portfolios)
    # Unassign any saves that belonged to this portfolio
    for save in load_all_saves():
        if save.get('portfolio_id') == portfolio_id:
            save['portfolio_id'] = None
            with open(os.path.join(SAVES_DIR, f"{save['id']}.json"), 'w') as f:
                json.dump(save, f)
    next_page = request.args.get('next', 'welcome')
    return redirect(url_for(next_page))

@app.route('/settings', methods=['GET'])
@login_required
def settings():
    return render_template('settings.html', portfolios=load_portfolios(), companies=load_companies())

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
@login_required
def company_delete():
    name = request.form.get('name', '').strip()
    if name:
        companies = load_companies()
        companies = [c for c in companies if c != name]
        save_companies(companies)
        log_action('COMPANY_DELETE', name)
    return redirect(url_for('settings'))

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
            'portfolio_id':       request.form.get('portfolio_id', '').strip() or None,
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
    session['file_path'] = 'data/Testdata.xlsx'
    session['project_info'] = {
        'governing_code': '49 CFR Part 192', 'owner_company': 'Demo Company',
        'portfolio_id': None, 'testing_contractor': 'PES',
        'company_approver': {'name': '', 'phone': '', 'email': ''},
        'contractor_rep': {'name': '', 'phone': '', 'email': ''},
    }
    session.pop('save_id', None)
    return redirect(url_for('mapping'))

@app.route('/mapping', methods=['GET', 'POST'])
@login_required
def mapping():
    if request.method == 'POST':
        if 'file' in request.files:
            file = request.files['file']
            if file.filename != '':
                file.save('uploaded.xlsx')
                session['file_path'] = 'uploaded.xlsx'
                # Clear station-dependent params when new file is uploaded
                if 'params' in session:
                    for key in ['start', 'end', 'test_site', 'dewater_site', 'fill_site']:
                        session['params'].pop(key, None)

        col_station = request.form.get('col_station')
        col_elev = request.form.get('col_elev')
        col_wt = request.form.get('col_wt')

        if col_station and col_elev and col_wt:
            session.pop('save_id', None)
            session['col_map'] = {
                'station': col_station,
                'elev': col_elev,
                'wt': col_wt
            }

            # Pre-populate params based on data after column mapping
            file_path = session.get('file_path', 'data/Testdata.xlsx')
            logic = PipelineApp(file_path)
            data = logic.full_df
            col_sta = session['col_map']['station']
            min_sta = parse_station(data[col_sta].min())
            max_sta = parse_station(data[col_sta].max())

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

    file_path = session.get('file_path', 'data/Testdata.xlsx')
    logic = PipelineApp(file_path)
    preview_html, columns = logic.get_preview()

    p = session.get('params', {})
    return render_template('mapping.html', p=p, preview=preview_html, columns=columns)

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
        numeric_keys = ['fill_gpm', 'dewater_gpm', 'cfm', 'od', 'min_p', 'min_excess', 'window_upper', 'override_prepack', 'override_vent', 'smys_threshold', 'unrestrained_length']
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

        # Auto-set fill_site based on direction (overrides pre-populated value)
        if 'fill_direction' in form_dict:
            if form_dict['fill_direction'] == '1':
                form_dict['fill_site'] = form_dict.get('start', p.get('start'))
            else:
                form_dict['fill_site'] = form_dict.get('end', p.get('end'))
        p.update(form_dict)
        session['params'] = p

    # Always ensure fill_site is set (for GET or if missing after POST)
    if 'fill_site' not in p and 'fill_direction' in p and 'start' in p and 'end' in p:
        if p['fill_direction'] == '1':
            p['fill_site'] = p['start']
        else:
            p['fill_site'] = p['end']
        session['params'] = p  # Persist the update

    try:
        file_path = session.get('file_path', 'data/Testdata.xlsx')
        smys = grade_smys.get(p.get('grade', 'X70'), 70000)
        app_logic = PipelineApp(file_path, od=float(p["od"]), smys=smys, _df=get_cached_df(file_path))
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

        plot1, plot2 = app_logic.generate_plot(sec.table_data, min_test=float(p['min_p']) if p.get('min_p') else None, params=p, gauge_lower=sec.gauge_lower, gauge_upper=sec.gauge_upper, prepack_time=prepack_time, sec=sec)
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
        return f"Input Error: {ve} (Check station formats or numeric values)"
    except Exception as e:
        return f"Calculation Error: {e}"

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
    file_path = session.get('file_path', 'data/Testdata.xlsx')

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
            })

        saved_file = existing_data_file or file_path
        if file_path and file_path not in ('data/Testdata.xlsx',) and os.path.exists(file_path):
            if not existing_data_file or not os.path.exists(existing_data_file):
                saved_file = os.path.join(SAVES_DIR, f'{save_id}_data.xlsx')
                shutil.copy(file_path, saved_file)

        new_version = old_version + 1
        history = old_history
    else:
        save_id = str(uuid.uuid4())[:8]
        saved_file = file_path
        if file_path and file_path not in ('data/Testdata.xlsx',) and os.path.exists(file_path):
            saved_file = os.path.join(SAVES_DIR, f'{save_id}_data.xlsx')
            shutil.copy(file_path, saved_file)
        new_version = 1
        history = []

    p['portfolio_id'] = portfolio_id
    session['params'] = p

    save_data = {
        'id': save_id,
        'version': new_version,
        'name': p.get('analysis_name') or 'Untitled Analysis',
        'notes': p.get('notes', ''),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'portfolio_id': portfolio_id,
        'project_info': session.get('project_info', {}),
        'params': p,
        'col_map': col_map,
        'file_path': saved_file,
        'history': history,
    }

    with open(os.path.join(SAVES_DIR, f'{save_id}.json'), 'w') as f:
        json.dump(save_data, f)

    action = 'SAVE_VERSION' if overwrite_id else 'SAVE_NEW'
    log_action(action, f'id={save_id} name="{save_data["name"]}" v{new_version}')
    session['save_id'] = save_id
    return redirect(url_for('results'))

@app.route('/load/<save_id>')
@login_required
def load_save(save_id):
    save_file = os.path.join(SAVES_DIR, f'{save_id}.json')
    if not os.path.exists(save_file):
        return "Save not found.", 404
    with open(save_file) as f:
        data = json.load(f)
    log_action('LOAD', f'id={save_id} name="{data.get("name", "")}"')
    session['params'] = data['params']
    session['col_map'] = data['col_map']
    session['file_path'] = data['file_path']
    session['save_id'] = save_id
    session['project_info'] = data.get('project_info', {})
    return redirect(url_for('results'))

@app.route('/load/<save_id>/version/<int:version_num>')
@login_required
def load_version(save_id, version_num):
    save_file = os.path.join(SAVES_DIR, f'{save_id}.json')
    if not os.path.exists(save_file):
        return "Save not found.", 404
    with open(save_file) as f:
        data = json.load(f)
    # Find the history entry for this version
    entry = next((h for h in data.get('history', []) if h['version'] == version_num), None)
    if not entry:
        return "Version not found.", 404
    log_action('LOAD_VERSION', f'id={save_id} name="{data.get("name", "")}" v{version_num}')
    # Load historical params but keep the current save's col_map and file_path
    session['params'] = entry['params']
    session['col_map'] = data['col_map']
    session['file_path'] = data['file_path']
    session['save_id'] = save_id
    session['project_info'] = data.get('project_info', {})
    return redirect(url_for('results', restored_version=version_num))

@app.route('/delete/<save_id>', methods=['POST'])
@login_required
def delete_save(save_id):
    save_file = os.path.join(SAVES_DIR, f'{save_id}.json')
    data_file = os.path.join(SAVES_DIR, f'{save_id}_data.xlsx')
    name = ''
    if os.path.exists(save_file):
        with open(save_file) as f:
            name = json.load(f).get('name', '')
        os.remove(save_file)
    if os.path.exists(data_file):
        os.remove(data_file)
    log_action('DELETE', f'id={save_id} name="{name}"')
    return redirect(request.referrer or url_for('welcome'))

@app.route('/print')
@login_required
def print_view():
    p = session.get('params', {})
    col_map = session.get('col_map')
    paper_size = request.args.get('paper_size', '8.5x11')
    orientation = request.args.get('orientation', 'portrait')

    if not col_map or not p:
        return "No data available for printing."

    try:
        file_path = session.get('file_path', 'data/Testdata.xlsx')
        smys = grade_smys.get(p.get('grade', 'X70'), 70000)
        app_logic = PipelineApp(file_path, od=float(p["od"]), smys=smys, _df=get_cached_df(file_path))
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

        plot1, plot2 = app_logic.generate_plot(sec.table_data, min_test=float(p['min_p']) if p.get('min_p') else None, params=p, gauge_lower=sec.gauge_lower, gauge_upper=sec.gauge_upper, prepack_time=prepack_time, sec=sec, static=True)
        fill_minutes = math.ceil(sec.volume_gal / float(p['fill_gpm'])) if p.get('fill_gpm') and float(p['fill_gpm']) > 0 else None
        fill_time = f"{fill_minutes // 60}:{fill_minutes % 60:02d}" if fill_minutes is not None else None
        dewater_minutes = math.ceil(sec.volume_gal / float(p['dewater_gpm'])) if p.get('dewater_gpm') and float(p['dewater_gpm']) > 0 else None
        dewater_time = f"{dewater_minutes // 60}:{dewater_minutes % 60:02d}" if dewater_minutes is not None else None

        return render_template('print.html', sec=sec, p=p, fill_time=fill_time, dew_time=dewater_time, prepack_time=prepack_time, plot1=plot1, plot2=plot2, vent_gallons=vent_gallons, paper_size=paper_size, orientation=orientation, max_smys_pct=max_smys_pct, max_smys_station=max_smys_station)
    except Exception as e:
        return f"Error generating print view: {e}"

@app.route('/pv/<save_id>')
@login_required
def pv_plot(save_id):
    save_file = os.path.join(SAVES_DIR, f'{save_id}.json')
    if not os.path.exists(save_file):
        return "Save not found.", 404
    with open(save_file) as f:
        data = json.load(f)
    p = data.get('params', {})
    total_volume_gal = None
    avg_wt = None
    try:
        file_path = data.get('file_path', 'data/Testdata.xlsx')
        smys = grade_smys.get(p.get('grade', 'X70'), 70000)
        app_logic = PipelineApp(file_path, od=float(p["od"]), smys=smys, _df=get_cached_df(file_path))
        sec = Section(app_logic, p, data.get('col_map', {}))
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
    save_file = os.path.join(SAVES_DIR, f'{save_id}.json')
    if not os.path.exists(save_file):
        return jsonify({'error': 'Save not found'}), 404
    with open(save_file) as f:
        data = json.load(f)
    payload = request.get_json()
    payload['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    data['pv_data'] = payload
    with open(save_file, 'w') as f:
        json.dump(data, f)
    rows = len(payload.get('readings', []))
    log_action('PV_SAVE', f'id={save_id} name="{data.get("name", "")}" rows={rows}')
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
