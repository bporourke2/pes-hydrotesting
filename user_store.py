"""
User store — persisted to config/users.json.

Each user record:
{
    "sub":          "unique-id-from-provider",
    "email":        "user@example.com",
    "name":         "Display Name",
    "role":         "hydro" | "hydro-admin",
    "provider_id":  "abc12345",
    "groups":       ["hydro", "hydro-admin"],   # raw groups from OIDC
    "last_login":   "2026-03-16 13:00",
    "first_login":  "2026-03-16 13:00",
}

Roles:
  hydro       — standard user (can run analyses, save, view PV plots)
  hydro-admin — admin (all standard + settings, oauth config, user management)
"""

import json
import os
import tempfile
from datetime import datetime

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(_APP_DIR, 'config')
USERS_FILE = os.path.join(CONFIG_DIR, 'users.json')

os.makedirs(CONFIG_DIR, exist_ok=True)

VALID_ROLES = ('hydro', 'hydro-admin')
ADMIN_GROUPS = {'hydro-admin'}
USER_GROUPS = {'hydro', 'hydro-admin'}


def _safe_write(filepath, data):
    dirname = os.path.dirname(filepath)
    fd, tmp = tempfile.mkstemp(suffix='.json', dir=dirname)
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, filepath)
    except Exception:
        os.unlink(tmp)
        raise


def load_users():
    if not os.path.exists(USERS_FILE):
        return []
    with open(USERS_FILE) as f:
        return json.load(f)


def save_users(users):
    _safe_write(USERS_FILE, users)


def get_user(sub):
    return next((u for u in load_users() if u['sub'] == sub), None)


def resolve_role(groups):
    """Determine role from OIDC group list. Returns role string or None if no matching group."""
    if not groups:
        return None
    group_set = set(g.lower() for g in groups) if groups else set()
    if group_set & ADMIN_GROUPS:
        return 'hydro-admin'
    if group_set & USER_GROUPS:
        return 'hydro'
    return None


def upsert_user(sub, email, name, groups, provider_id):
    """Create or update a user record on login. Returns the user dict."""
    users = load_users()
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    role = resolve_role(groups)

    existing = next((u for u in users if u['sub'] == sub), None)
    if existing:
        existing['email'] = email or existing.get('email')
        existing['name'] = name or existing.get('name')
        existing['groups'] = groups or existing.get('groups', [])
        existing['provider_id'] = provider_id
        existing['last_login'] = now
        # Update role from groups (provider is source of truth), but allow manual override
        if not existing.get('role_locked'):
            existing['role'] = role or existing.get('role')
        save_users(users)
        return existing
    else:
        user = {
            'sub': sub,
            'email': email,
            'name': name,
            'role': role,
            'provider_id': provider_id,
            'groups': groups or [],
            'first_login': now,
            'last_login': now,
            'role_locked': False,
        }
        users.append(user)
        save_users(users)
        return user


def set_user_role(sub, role, lock=True):
    """Manually set a user's role (admin override). Lock prevents OIDC groups from overwriting."""
    if role not in VALID_ROLES:
        return None
    users = load_users()
    for u in users:
        if u['sub'] == sub:
            u['role'] = role
            u['role_locked'] = lock
            save_users(users)
            return u
    return None


def delete_user(sub):
    users = load_users()
    users = [u for u in users if u['sub'] != sub]
    save_users(users)


def is_admin(user_session):
    """Check if the session user has admin role."""
    if not user_session:
        return False
    sub = user_session.get('sub')
    if not sub:
        return False
    user = get_user(sub)
    return user is not None and user.get('role') == 'hydro-admin'


def has_access(user_session):
    """Check if the session user has any valid role (hydro or hydro-admin)."""
    if not user_session:
        return False
    sub = user_session.get('sub')
    if not sub:
        return False
    user = get_user(sub)
    return user is not None and user.get('role') in VALID_ROLES


def is_first_user():
    """True if no users exist yet (first user gets auto-promoted to admin)."""
    return len(load_users()) == 0


# --- Portfolio access ---

def get_user_portfolio_ids(user_session):
    """Return list of portfolio IDs the user can access, or None for admins (meaning all)."""
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
    """Set the list of portfolio IDs a user can access."""
    users = load_users()
    for u in users:
        if u['sub'] == sub:
            u['portfolio_ids'] = portfolio_ids
            save_users(users)
            return u
    return None


def add_portfolio_to_user(sub, portfolio_id):
    """Add a single portfolio to a user's access list (idempotent)."""
    users = load_users()
    for u in users:
        if u['sub'] == sub:
            pids = u.get('portfolio_ids', [])
            if portfolio_id not in pids:
                pids.append(portfolio_id)
                u['portfolio_ids'] = pids
                save_users(users)
            return u
    return None


def remove_portfolio_from_all_users(portfolio_id):
    """Remove a deleted portfolio from all user access lists."""
    users = load_users()
    changed = False
    for u in users:
        pids = u.get('portfolio_ids', [])
        if portfolio_id in pids:
            u['portfolio_ids'] = [p for p in pids if p != portfolio_id]
            changed = True
    if changed:
        save_users(users)
