"""
SQLAlchemy models and DB access helpers.

Database: saves/hydro.db (SQLite)

All "save dict" helpers preserve the same dict shape that app.py has always
used, so routes need minimal changes: replace open/json.load with load_save()
and replace safe_write_json with write_save().
"""

import json
import os
from contextlib import contextmanager

from sqlalchemy import (
    Boolean, Column, Float, ForeignKey, Integer, String, Text,
    create_engine, event,
)
from sqlalchemy.orm import declarative_base, sessionmaker

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_APP_DIR, 'saves', 'hydro.db')

Base = declarative_base()

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class User(Base):
    __tablename__ = 'users'
    id            = Column(Integer, primary_key=True)
    sub           = Column(String, unique=True, nullable=False, index=True)
    email         = Column(String)
    name          = Column(String)
    role          = Column(String)
    provider_id   = Column(String)
    groups        = Column(Text)          # JSON array
    first_login   = Column(String)
    last_login    = Column(String)
    role_locked   = Column(Boolean, default=False)
    portfolio_ids = Column(Text)          # JSON array


class Portfolio(Base):
    __tablename__ = 'portfolios'
    id      = Column(String, primary_key=True)
    name    = Column(String, nullable=False)
    company = Column(String)


class Company(Base):
    __tablename__ = 'companies'
    id   = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)


class Save(Base):
    __tablename__ = 'saves'
    id           = Column(String, primary_key=True)
    version      = Column(Integer, default=1)
    name         = Column(String)
    notes        = Column(Text)
    timestamp    = Column(String)
    portfolio_id = Column(String)
    owner_sub    = Column(String)
    file_path    = Column(String)
    params       = Column(Text)   # JSON
    col_map      = Column(Text)   # JSON
    project_info = Column(Text)   # JSON


class SaveHistory(Base):
    __tablename__ = 'save_history'
    id           = Column(Integer, primary_key=True)
    save_id      = Column(String, ForeignKey('saves.id', ondelete='CASCADE'), nullable=False, index=True)
    version      = Column(Integer, nullable=False)
    timestamp    = Column(String)
    notes        = Column(Text)
    params       = Column(Text)   # JSON
    col_map      = Column(Text)   # JSON
    project_info = Column(Text)   # JSON


class TestRun(Base):
    __tablename__ = 'test_runs'
    id           = Column(Integer, primary_key=True)
    save_id      = Column(String, ForeignKey('saves.id', ondelete='CASCADE'), nullable=False, index=True)
    test_date    = Column(String)
    timezone     = Column(String)
    install_date = Column(String)
    converted    = Column(Boolean)
    cert_class   = Column(String)
    status       = Column(String)
    finalized_at = Column(String)
    last_updated = Column(String)
    is_current   = Column(Boolean, default=True)
    readings     = Column(Text)   # JSON array


class PVPlot(Base):
    __tablename__ = 'pv_plots'
    id                  = Column(Integer, primary_key=True)
    save_id             = Column(String, ForeignKey('saves.id', ondelete='CASCADE'), nullable=False, index=True)
    pump                = Column(Text)    # JSON pump config
    unrestrained_length = Column(Float, default=0)
    readings            = Column(Text)    # JSON array
    last_updated        = Column(String)


class RuptureAnalysis(Base):
    __tablename__ = 'rupture_analyses'
    id             = Column(String, primary_key=True)
    save_id        = Column(String, ForeignKey('saves.id', ondelete='CASCADE'), nullable=False, index=True)
    label          = Column(String)
    mode           = Column(String)
    rup_station    = Column(Text)   # JSON (string or list)
    specific_weight = Column(Float, default=62.4)
    saved_at       = Column(String)
    saved_by       = Column(String)
    results        = Column(Text)   # JSON
    modified_at    = Column(String)
    modified_by    = Column(String)


# ---------------------------------------------------------------------------
# Engine / session factory
# ---------------------------------------------------------------------------

_engine = None
_SessionFactory = None


def get_engine():
    global _engine
    if _engine is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _engine = create_engine(
            f'sqlite:///{DB_PATH}',
            connect_args={'check_same_thread': False},
        )

        @event.listens_for(_engine, 'connect')
        def _on_connect(dbapi_con, _):
            dbapi_con.execute('PRAGMA journal_mode=WAL')
            dbapi_con.execute('PRAGMA foreign_keys=ON')

        Base.metadata.create_all(_engine)
    return _engine


def _factory():
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=get_engine())
    return _SessionFactory


@contextmanager
def db_session():
    Session = _factory()
    sess = Session()
    try:
        yield sess
        sess.commit()
    except Exception:
        sess.rollback()
        raise
    finally:
        sess.close()


# ---------------------------------------------------------------------------
# Save helpers — maintain dict shape identical to the old JSON files
# ---------------------------------------------------------------------------

def _j(text, default=None):
    """Decode JSON column, returning default on None/error."""
    if text is None:
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def _save_row_to_dict(s):
    """Convert a Save ORM row to the flat dict used throughout app.py."""
    return {
        'id':           s.id,
        'version':      s.version,
        'name':         s.name or '',
        'notes':        s.notes or '',
        'timestamp':    s.timestamp,
        'portfolio_id': s.portfolio_id,
        'owner_sub':    s.owner_sub,
        'file_path':    s.file_path,
        'params':       _j(s.params, {}),
        'col_map':      _j(s.col_map, {}),
        'project_info': _j(s.project_info, {}),
    }


def load_save(save_id):
    """Return the full save dict (with history, test data, pv, ruptures), or None."""
    with db_session() as sess:
        s = sess.get(Save, save_id)
        if s is None:
            return None
        data = _save_row_to_dict(s)

        data['history'] = [
            {
                'version':      h.version,
                'timestamp':    h.timestamp,
                'notes':        h.notes or '',
                'params':       _j(h.params, {}),
                'col_map':      _j(h.col_map, {}),
                'project_info': _j(h.project_info, {}),
            }
            for h in sess.query(SaveHistory)
                          .filter_by(save_id=save_id)
                          .order_by(SaveHistory.version)
                          .all()
        ]

        data['rupture_analyses'] = [
            {
                'id':             ra.id,
                'label':          ra.label,
                'mode':           ra.mode,
                'rup_station':    _j(ra.rup_station, ''),
                'specific_weight': ra.specific_weight,
                'saved_at':       ra.saved_at,
                'saved_by':       ra.saved_by,
                'results':        _j(ra.results, {}),
                'modified_at':    ra.modified_at,
                'modified_by':    ra.modified_by,
            }
            for ra in sess.query(RuptureAnalysis).filter_by(save_id=save_id).all()
        ]

        pv = sess.query(PVPlot).filter_by(save_id=save_id).first()
        if pv:
            data['pv_data'] = {
                'pump':               _j(pv.pump, {}),
                'unrestrained_length': pv.unrestrained_length,
                'readings':           _j(pv.readings, []),
                'last_updated':       pv.last_updated,
            }

        current = sess.query(TestRun).filter_by(save_id=save_id, is_current=True).first()
        if current:
            data['test_data'] = {
                'readings':     _j(current.readings, []),
                'test_date':    current.test_date,
                'timezone':     current.timezone,
                'install_date': current.install_date,
                'converted':    current.converted,
                'cert_class':   current.cert_class,
                'status':       current.status,
                'finalized_at': current.finalized_at,
                'last_updated': current.last_updated,
            }

        data['test_history'] = [
            {
                'readings':     _j(r.readings, []),
                'test_date':    r.test_date,
                'timezone':     r.timezone,
                'install_date': r.install_date,
                'converted':    r.converted,
                'cert_class':   r.cert_class,
                'status':       r.status,
                'finalized_at': r.finalized_at,
                'last_updated': r.last_updated,
            }
            for r in sess.query(TestRun)
                         .filter_by(save_id=save_id, is_current=False)
                         .order_by(TestRun.id.desc())
                         .all()
        ]

        return data


def load_all_saves():
    """Return list of save dicts (metadata only — no test data/PV/ruptures).
    Used by welcome page and project_setup to build the hierarchy tree."""
    with db_session() as sess:
        rows = sess.query(Save).order_by(Save.timestamp.desc()).all()
        result = []
        for s in rows:
            d = _save_row_to_dict(s)
            d['history'] = []
            d['rupture_analyses'] = []
            result.append(d)
        return result


def write_save(data):
    """Full upsert of a save dict into the DB.

    Keys absent from data are preserved (e.g. rupture_analyses not included
    in data → existing rows are kept rather than deleted).
    """
    save_id = data['id']
    with db_session() as sess:
        s = sess.get(Save, save_id)
        if s is None:
            s = Save(id=save_id)
            sess.add(s)
        s.version      = data.get('version', 1)
        s.name         = data.get('name', '')
        s.notes        = data.get('notes', '')
        s.timestamp    = data.get('timestamp')
        s.portfolio_id = data.get('portfolio_id')
        s.owner_sub    = data.get('owner_sub')
        s.file_path    = data.get('file_path')
        s.params       = json.dumps(data.get('params', {}))
        s.col_map      = json.dumps(data.get('col_map', {}))
        s.project_info = json.dumps(data.get('project_info', {}))

        # History — replace only when provided
        if 'history' in data:
            sess.query(SaveHistory).filter_by(save_id=save_id).delete()
            for h in data['history']:
                sess.add(SaveHistory(
                    save_id=save_id,
                    version=h.get('version', 1),
                    timestamp=h.get('timestamp'),
                    notes=h.get('notes', ''),
                    params=json.dumps(h.get('params', {})),
                    col_map=json.dumps(h.get('col_map', {})),
                    project_info=json.dumps(h.get('project_info', {})),
                ))

        # Rupture analyses — replace only when provided
        if 'rupture_analyses' in data:
            sess.query(RuptureAnalysis).filter_by(save_id=save_id).delete()
            for ra in data['rupture_analyses']:
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

        # PV data — replace only when provided
        if 'pv_data' in data:
            sess.query(PVPlot).filter_by(save_id=save_id).delete()
            pv = data['pv_data']
            if pv:
                sess.add(PVPlot(
                    save_id=save_id,
                    pump=json.dumps(pv.get('pump', {})),
                    unrestrained_length=pv.get('unrestrained_length', 0),
                    readings=json.dumps(pv.get('readings', [])),
                    last_updated=pv.get('last_updated'),
                ))

        # Test runs — replace only when provided
        if 'test_data' in data or 'test_history' in data:
            sess.query(TestRun).filter_by(save_id=save_id).delete()
            td = data.get('test_data')
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
            for h in data.get('test_history', []):
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


def delete_save(save_id):
    """Delete a save and all related rows from the DB."""
    with db_session() as sess:
        # Cascaded via FK ondelete, but delete children explicitly for SQLite
        sess.query(SaveHistory).filter_by(save_id=save_id).delete()
        sess.query(RuptureAnalysis).filter_by(save_id=save_id).delete()
        sess.query(PVPlot).filter_by(save_id=save_id).delete()
        sess.query(TestRun).filter_by(save_id=save_id).delete()
        sess.query(Save).filter_by(id=save_id).delete()


def clear_save_portfolio(portfolio_id):
    """Null out portfolio_id on all saves that reference a deleted portfolio."""
    with db_session() as sess:
        sess.query(Save).filter_by(portfolio_id=portfolio_id).update({'portfolio_id': None})


# ---------------------------------------------------------------------------
# Portfolio helpers
# ---------------------------------------------------------------------------

def load_portfolios():
    with db_session() as sess:
        return [
            {'id': p.id, 'name': p.name, 'company': p.company}
            for p in sess.query(Portfolio).order_by(Portfolio.name).all()
        ]


def save_portfolios(portfolios):
    """Full replace of the portfolios table from a list of dicts."""
    with db_session() as sess:
        existing_ids = {p.id for p in sess.query(Portfolio).all()}
        new_ids = {pf['id'] for pf in portfolios}
        for pid in existing_ids - new_ids:
            sess.query(Portfolio).filter_by(id=pid).delete()
        for pf in portfolios:
            p = sess.get(Portfolio, pf['id'])
            if p is None:
                p = Portfolio(id=pf['id'])
                sess.add(p)
            p.name    = pf['name']
            p.company = pf.get('company')


def upsert_portfolio(portfolio_id, name, company=None):
    with db_session() as sess:
        p = sess.get(Portfolio, portfolio_id)
        if p is None:
            p = Portfolio(id=portfolio_id)
            sess.add(p)
        p.name    = name
        p.company = company


def delete_portfolio(portfolio_id):
    with db_session() as sess:
        sess.query(Portfolio).filter_by(id=portfolio_id).delete()


# ---------------------------------------------------------------------------
# Company helpers
# ---------------------------------------------------------------------------

def load_companies():
    with db_session() as sess:
        return [c.name for c in sess.query(Company).order_by(Company.name).all()]


def save_companies(companies):
    """Full replace of the companies table from a list of name strings."""
    with db_session() as sess:
        sess.query(Company).delete()
        for name in companies:
            sess.add(Company(name=name))


def add_company(name):
    with db_session() as sess:
        if not sess.query(Company).filter_by(name=name).first():
            sess.add(Company(name=name))


def remove_company(name):
    with db_session() as sess:
        sess.query(Company).filter_by(name=name).delete()


# ---------------------------------------------------------------------------
# User helpers (mirror user_store.py interface but DB-backed)
# ---------------------------------------------------------------------------

def _user_row_to_dict(u):
    return {
        'sub':          u.sub,
        'email':        u.email,
        'name':         u.name,
        'role':         u.role,
        'provider_id':  u.provider_id,
        'groups':       _j(u.groups, []),
        'first_login':  u.first_login,
        'last_login':   u.last_login,
        'role_locked':  bool(u.role_locked),
        'portfolio_ids': _j(u.portfolio_ids, []),
    }


def db_load_users():
    with db_session() as sess:
        return [_user_row_to_dict(u) for u in sess.query(User).all()]


def db_get_user(sub):
    with db_session() as sess:
        u = sess.query(User).filter_by(sub=sub).first()
        return _user_row_to_dict(u) if u else None


def db_upsert_user(sub, email, name, groups, provider_id, role, now, role_locked=False):
    """Create or update user. Returns the user dict."""
    with db_session() as sess:
        u = sess.query(User).filter_by(sub=sub).first()
        if u is None:
            u = User(sub=sub, first_login=now)
            sess.add(u)
        u.email       = email or u.email
        u.name        = name or u.name
        u.groups      = json.dumps(groups or [])
        u.provider_id = provider_id
        u.last_login  = now
        if not u.role_locked:
            if role:
                u.role = role
        # role_locked only set on explicit admin override
        if role_locked:
            u.role_locked = True
        sess.flush()
        return _user_row_to_dict(u)


def db_set_user_role(sub, role, lock=True):
    with db_session() as sess:
        u = sess.query(User).filter_by(sub=sub).first()
        if u is None:
            return None
        u.role = role
        u.role_locked = lock
        sess.flush()
        return _user_row_to_dict(u)


def db_delete_user(sub):
    with db_session() as sess:
        sess.query(User).filter_by(sub=sub).delete()


def db_is_first_user():
    with db_session() as sess:
        return sess.query(User).count() == 0


def db_set_user_portfolios(sub, portfolio_ids):
    with db_session() as sess:
        u = sess.query(User).filter_by(sub=sub).first()
        if u is None:
            return None
        u.portfolio_ids = json.dumps(portfolio_ids)
        sess.flush()
        return _user_row_to_dict(u)


def db_add_portfolio_to_user(sub, portfolio_id):
    with db_session() as sess:
        u = sess.query(User).filter_by(sub=sub).first()
        if u is None:
            return None
        pids = _j(u.portfolio_ids, [])
        if portfolio_id not in pids:
            pids.append(portfolio_id)
            u.portfolio_ids = json.dumps(pids)
        sess.flush()
        return _user_row_to_dict(u)


def db_remove_portfolio_from_all_users(portfolio_id):
    with db_session() as sess:
        for u in sess.query(User).all():
            pids = _j(u.portfolio_ids, [])
            if portfolio_id in pids:
                u.portfolio_ids = json.dumps([p for p in pids if p != portfolio_id])


# Initialise the DB immediately on import so tables exist before any request.
get_engine()
