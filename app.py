from flask import Flask, render_template, request, session, redirect, url_for, jsonify
import math
import json
import uuid
import shutil
import os
from datetime import datetime
from logic import PipelineApp, Section, parse_station, station_format

app = Flask(__name__)
app.secret_key = "hydrotest_v2_key"

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
def welcome():
    saves = load_all_saves()
    portfolios = load_portfolios()
    pf_map = {pf['id']: pf['name'] for pf in portfolios}
    # Group saves by portfolio
    grouped = []
    for pf in portfolios:
        group = [s for s in saves if s.get('portfolio_id') == pf['id']]
        if group:
            grouped.append({'id': pf['id'], 'name': pf['name'], 'saves': group})
    ungrouped = [s for s in saves if not s.get('portfolio_id')]
    return render_template('welcome.html', saves=saves, portfolios=portfolios, grouped=grouped, ungrouped=ungrouped)

@app.route('/portfolio/create', methods=['POST'])
def portfolio_create():
    name = request.form.get('name', '').strip()
    if name:
        portfolios = load_portfolios()
        portfolios.append({'id': str(uuid.uuid4())[:8], 'name': name})
        save_portfolios(portfolios)
    next_page = request.form.get('next', 'welcome')
    return redirect(url_for(next_page))

@app.route('/portfolio/delete/<portfolio_id>', methods=['POST'])
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
    return redirect(url_for('welcome'))

@app.route('/set_mode/demo')
def set_demo():
    session['params'] = {
        'start': 1200495, 'end': 1218848, 'od': 42,
        'min_p': 1850, 'cfm': 12000, 'fill_gpm': 800, 'dewater_gpm': 600,
        'test_site': 1218848, 'dewater_site': 1218848, 'smys_threshold': 104, 'fill_direction': '1',
        'min_excess': 25, 'window_upper': 50, 'grade': 'X70'
    }
    session['file_path'] = 'data/Testdata.xlsx'
    session.pop('save_id', None)
    return redirect(url_for('mapping'))

@app.route('/mapping', methods=['GET', 'POST'])
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
        numeric_keys = ['fill_gpm', 'dewater_gpm', 'cfm', 'od', 'min_p', 'min_excess', 'window_upper', 'override_prepack', 'override_vent', 'smys_threshold']
        for key in numeric_keys:
            if key in form_dict:
                value = form_dict[key].strip() if form_dict[key] else ''
                if value:
                    try:
                        form_dict[key] = float(value)
                    except ValueError:
                        form_dict[key] = p.get(key)  # Fallback to previous if invalid
                else:
                    if key in ['override_prepack', 'override_vent']:
                        form_dict[key] = None  # Clear override if blank
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
        app_logic = PipelineApp(file_path, od=float(p['od']), smys=smys)
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
        return render_template('results.html', sec=sec, p=p, fill_time=fill_time, dew_time=dewater_time, prepack_time=prepack_time, plot1=plot1, plot2=plot2, vent_gallons=vent_gallons, max_smys_pct=max_smys_pct, max_smys_station=max_smys_station, portfolios=portfolios, save_id=save_id, saves=saves)
    except ValueError as ve:
        return f"Input Error: {ve} (Check station formats or numeric values)"
    except Exception as e:
        return f"Calculation Error: {e}"

@app.route('/save', methods=['POST'])
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

    overwrite_id = request.form.get('overwrite_id', '').strip()
    file_path = session.get('file_path', 'data/Testdata.xlsx')

    if overwrite_id:
        # Overwrite existing save — keep same id and data file path
        save_id = overwrite_id
        existing_file = os.path.join(SAVES_DIR, f'{save_id}.json')
        existing_data_file = None
        if os.path.exists(existing_file):
            with open(existing_file) as f:
                old = json.load(f)
            existing_data_file = old.get('file_path')

        # Only re-copy if the current file_path differs from existing saved file
        saved_file = existing_data_file or file_path
        if file_path and file_path not in ('data/Testdata.xlsx',) and os.path.exists(file_path):
            if not existing_data_file or not os.path.exists(existing_data_file):
                saved_file = os.path.join(SAVES_DIR, f'{save_id}_data.xlsx')
                shutil.copy(file_path, saved_file)
    else:
        save_id = str(uuid.uuid4())[:8]
        saved_file = file_path
        if file_path and file_path not in ('data/Testdata.xlsx',) and os.path.exists(file_path):
            saved_file = os.path.join(SAVES_DIR, f'{save_id}_data.xlsx')
            shutil.copy(file_path, saved_file)

    portfolio_id = request.form.get('portfolio_id', '').strip() or None
    p['portfolio_id'] = portfolio_id
    session['params'] = p

    save_data = {
        'id': save_id,
        'name': p.get('analysis_name') or 'Untitled Analysis',
        'notes': p.get('notes', ''),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'portfolio_id': portfolio_id,
        'params': p,
        'col_map': col_map,
        'file_path': saved_file
    }

    with open(os.path.join(SAVES_DIR, f'{save_id}.json'), 'w') as f:
        json.dump(save_data, f)

    session['save_id'] = save_id
    return redirect(url_for('results'))

@app.route('/load/<save_id>')
def load_save(save_id):
    save_file = os.path.join(SAVES_DIR, f'{save_id}.json')
    if not os.path.exists(save_file):
        return "Save not found.", 404
    with open(save_file) as f:
        data = json.load(f)
    session['params'] = data['params']
    session['col_map'] = data['col_map']
    session['file_path'] = data['file_path']
    session['save_id'] = save_id
    return redirect(url_for('results'))

@app.route('/delete/<save_id>', methods=['POST'])
def delete_save(save_id):
    save_file = os.path.join(SAVES_DIR, f'{save_id}.json')
    data_file = os.path.join(SAVES_DIR, f'{save_id}_data.xlsx')
    if os.path.exists(save_file):
        os.remove(save_file)
    if os.path.exists(data_file):
        os.remove(data_file)
    return redirect(request.referrer or url_for('welcome'))

@app.route('/print')
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
        app_logic = PipelineApp(file_path, od=float(p['od']), smys=smys)
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
