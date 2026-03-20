"""
One-time migration: JSON files → SQLite (saves/hydro.db).

Run once:
    python migrate.py

Safe to re-run — existing DB rows are skipped (not overwritten).
The original JSON files are left in place as backups.
"""

import json
import os
import sys

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
SAVES_DIR  = os.path.join(_APP_DIR, 'saves')
CONFIG_DIR = os.path.join(_APP_DIR, 'config')

# Import DB helpers (this also creates all tables via get_engine()).
from db import (
    db_session, db_get_user, db_upsert_user, db_set_user_role,
    db_add_portfolio_to_user,
    Portfolio, Company, Save, SaveHistory, TestRun, PVPlot, RuptureAnalysis, User,
    get_engine,
)
from sqlalchemy.orm import sessionmaker


def _j(val, default=None):
    if val is None:
        return default
    return val


def migrate_companies():
    companies_file = os.path.join(SAVES_DIR, '_companies.json')
    if not os.path.exists(companies_file):
        print('  no _companies.json — skipping')
        return
    with open(companies_file) as f:
        companies = json.load(f)
    Session = sessionmaker(bind=get_engine())
    sess = Session()
    try:
        n = 0
        for name in companies:
            if not sess.query(Company).filter_by(name=name).first():
                sess.add(Company(name=name))
                n += 1
        sess.commit()
        print(f'  companies: {n} inserted ({len(companies) - n} already exist)')
    finally:
        sess.close()


def migrate_portfolios():
    pf_file = os.path.join(SAVES_DIR, '_portfolios.json')
    if not os.path.exists(pf_file):
        print('  no _portfolios.json — skipping')
        return
    with open(pf_file) as f:
        portfolios = json.load(f)
    Session = sessionmaker(bind=get_engine())
    sess = Session()
    try:
        n = 0
        for pf in portfolios:
            if not sess.get(Portfolio, pf['id']):
                sess.add(Portfolio(id=pf['id'], name=pf['name'], company=pf.get('company')))
                n += 1
        sess.commit()
        print(f'  portfolios: {n} inserted ({len(portfolios) - n} already exist)')
    finally:
        sess.close()


def migrate_users():
    users_file = os.path.join(CONFIG_DIR, 'users.json')
    if not os.path.exists(users_file):
        print('  no users.json — skipping')
        return
    with open(users_file) as f:
        users = json.load(f)
    Session = sessionmaker(bind=get_engine())
    sess = Session()
    try:
        n = 0
        for u in users:
            sub = u.get('sub')
            if not sub:
                continue
            if sess.query(User).filter_by(sub=sub).first():
                continue
            row = User(
                sub=sub,
                email=u.get('email'),
                name=u.get('name'),
                role=u.get('role'),
                provider_id=u.get('provider_id'),
                groups=json.dumps(u.get('groups', [])),
                first_login=u.get('first_login'),
                last_login=u.get('last_login'),
                role_locked=bool(u.get('role_locked', False)),
                portfolio_ids=json.dumps(u.get('portfolio_ids', [])),
            )
            sess.add(row)
            n += 1
        sess.commit()
        print(f'  users: {n} inserted ({len(users) - n} already exist)')
    finally:
        sess.close()


def migrate_saves():
    Session = sessionmaker(bind=get_engine())
    sess = Session()
    inserted = skipped = errors = 0
    try:
        for fname in sorted(os.listdir(SAVES_DIR)):
            if not fname.endswith('.json') or fname.startswith('_'):
                continue
            save_id = fname[:-5]  # strip .json
            fpath = os.path.join(SAVES_DIR, fname)
            try:
                with open(fpath) as f:
                    d = json.load(f)
            except Exception as e:
                print(f'  WARN: could not read {fname}: {e}')
                errors += 1
                continue

            if sess.query(Save).filter_by(id=save_id).count():
                skipped += 1
                continue

            # Core save row — flush immediately so child FKs resolve
            s = Save(
                id=save_id,
                version=d.get('version', 1),
                name=d.get('name', ''),
                notes=d.get('notes', ''),
                timestamp=d.get('timestamp'),
                portfolio_id=d.get('portfolio_id'),
                owner_sub=d.get('owner_sub'),
                file_path=d.get('file_path'),
                params=json.dumps(d.get('params', {})),
                col_map=json.dumps(d.get('col_map', {})),
                project_info=json.dumps(d.get('project_info', {})),
            )
            sess.add(s)
            sess.flush()  # ensure Save PK exists before inserting children

            # History
            for h in d.get('history', []):
                sess.add(SaveHistory(
                    save_id=save_id,
                    version=h.get('version', 1),
                    timestamp=h.get('timestamp'),
                    notes=h.get('notes', ''),
                    params=json.dumps(h.get('params', {})),
                    col_map=json.dumps(h.get('col_map', {})),
                    project_info=json.dumps(h.get('project_info', {})),
                ))

            # Rupture analyses
            for ra in d.get('rupture_analyses', []):
                sess.add(RuptureAnalysis(
                    id=ra['id'],
                    save_id=save_id,
                    label=ra.get('label', ''),
                    mode=ra.get('mode', 'single'),
                    rup_station=json.dumps(ra.get('rup_station', '')),
                    specific_weight=ra.get('specific_weight', 62.4),
                    saved_at=ra.get('saved_at'),
                    saved_by=ra.get('saved_by'),
                    results=json.dumps(ra.get('results', {})),
                    modified_at=ra.get('modified_at'),
                    modified_by=ra.get('modified_by'),
                ))

            # PV data
            pv = d.get('pv_data')
            if pv:
                sess.add(PVPlot(
                    save_id=save_id,
                    pump=json.dumps(pv.get('pump', {})),
                    unrestrained_length=pv.get('unrestrained_length', 0),
                    readings=json.dumps(pv.get('readings', [])),
                    last_updated=pv.get('last_updated'),
                ))

            # Current test run
            td = d.get('test_data')
            if td:
                sess.add(TestRun(
                    save_id=save_id,
                    is_current=True,
                    readings=json.dumps(td.get('readings', [])),
                    test_date=td.get('test_date'),
                    timezone=td.get('timezone'),
                    install_date=td.get('install_date'),
                    converted=td.get('converted'),
                    cert_class=td.get('cert_class'),
                    status=td.get('status'),
                    finalized_at=td.get('finalized_at'),
                    last_updated=td.get('last_updated'),
                ))

            # Historical test runs
            for h in d.get('test_history', []):
                sess.add(TestRun(
                    save_id=save_id,
                    is_current=False,
                    readings=json.dumps(h.get('readings', [])),
                    test_date=h.get('test_date'),
                    timezone=h.get('timezone'),
                    install_date=h.get('install_date'),
                    converted=h.get('converted'),
                    cert_class=h.get('cert_class'),
                    status=h.get('status'),
                    finalized_at=h.get('finalized_at'),
                    last_updated=h.get('last_updated'),
                ))

            inserted += 1

        sess.commit()
        print(f'  saves: {inserted} inserted, {skipped} already existed, {errors} errors')
    except Exception:
        sess.rollback()
        raise
    finally:
        sess.close()


def main():
    print('Running migration: JSON → SQLite')
    print(f'  DB: {os.path.join(SAVES_DIR, "hydro.db")}')
    print()
    print('Companies...')
    migrate_companies()
    print('Portfolios...')
    migrate_portfolios()
    print('Users...')
    migrate_users()
    print('Saves...')
    migrate_saves()
    print()
    print('Done.')


if __name__ == '__main__':
    main()
