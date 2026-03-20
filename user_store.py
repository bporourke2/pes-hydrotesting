"""
User store — backed by SQLite via db.py.

Public interface is unchanged from the JSON-file version so app.py imports
continue to work without modification.
"""

from datetime import datetime

from db import (
    db_load_users, db_get_user, db_upsert_user, db_set_user_role,
    db_delete_user, db_is_first_user,
    db_set_user_portfolios, db_add_portfolio_to_user,
    db_remove_portfolio_from_all_users,
)

VALID_ROLES  = ('hydro', 'hydro-admin')
ADMIN_GROUPS = {'hydro-admin'}
USER_GROUPS  = {'hydro', 'hydro-admin'}


def load_users():
    return db_load_users()


def get_user(sub):
    return db_get_user(sub)


def resolve_role(groups):
    if not groups:
        return None
    group_set = {g.lower() for g in groups}
    if group_set & ADMIN_GROUPS:
        return 'hydro-admin'
    if group_set & USER_GROUPS:
        return 'hydro'
    return None


def upsert_user(sub, email, name, groups, provider_id):
    """Create or update a user record on login. Returns the user dict."""
    now  = datetime.now().strftime('%Y-%m-%d %H:%M')
    role = resolve_role(groups)
    return db_upsert_user(sub, email, name, groups, provider_id, role, now)


def set_user_role(sub, role, lock=True):
    if role not in VALID_ROLES:
        return None
    return db_set_user_role(sub, role, lock)


def delete_user(sub):
    db_delete_user(sub)


def is_admin(user_session):
    if not user_session:
        return False
    sub = user_session.get('sub')
    if not sub:
        return False
    user = get_user(sub)
    return user is not None and user.get('role') == 'hydro-admin'


def has_access(user_session):
    if not user_session:
        return False
    sub = user_session.get('sub')
    if not sub:
        return False
    user = get_user(sub)
    return user is not None and user.get('role') in VALID_ROLES


def is_first_user():
    return db_is_first_user()


# --- Portfolio access ---

def get_user_portfolio_ids(user_session):
    """Return list of portfolio IDs the user can access, or None for admins."""
    if not user_session:
        return []
    if user_session.get('role') == 'hydro-admin':
        return None  # admin sees everything
    sub = user_session.get('sub')
    if not sub:
        return []
    user = get_user(sub)
    if not user:
        return []
    return user.get('portfolio_ids', [])


def set_user_portfolios(sub, portfolio_ids):
    return db_set_user_portfolios(sub, portfolio_ids)


def add_portfolio_to_user(sub, portfolio_id):
    return db_add_portfolio_to_user(sub, portfolio_id)


def remove_portfolio_from_all_users(portfolio_id):
    db_remove_portfolio_from_all_users(portfolio_id)
