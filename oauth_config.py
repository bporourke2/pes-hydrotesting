"""
OAuth provider configuration — persisted to config/oauth_providers.json.

Each provider is a dict:
{
    "id":            "8-char hex",
    "name":          "Authentik",           # display name
    "provider_type": "oidc",               # oidc | authentik (convenience preset)
    "issuer_url":    "https://auth.example.com",
    "client_id":     "...",
    "client_secret": "...",
    "discovery_url": "https://auth.example.com/.well-known/openid-configuration",
    "scopes":        "openid email profile",
    "logout_url":    "https://auth.example.com/end-session/",  # optional
    "enabled":       true,
    "is_default":    true,
}
"""

import json
import os
import uuid
import tempfile

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(_APP_DIR, 'config')
PROVIDERS_FILE = os.path.join(CONFIG_DIR, 'oauth_providers.json')

os.makedirs(CONFIG_DIR, exist_ok=True)


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


def load_providers():
    """Return list of provider dicts."""
    if not os.path.exists(PROVIDERS_FILE):
        return []
    with open(PROVIDERS_FILE) as f:
        return json.load(f)


def save_providers(providers):
    _safe_write(PROVIDERS_FILE, providers)


def get_provider(provider_id):
    return next((p for p in load_providers() if p['id'] == provider_id), None)


def get_default_provider():
    providers = load_providers()
    # Return the one marked default, or the first enabled one
    for p in providers:
        if p.get('is_default') and p.get('enabled', True):
            return p
    for p in providers:
        if p.get('enabled', True):
            return p
    return None


def get_enabled_providers():
    return [p for p in load_providers() if p.get('enabled', True)]


def add_provider(data):
    providers = load_providers()
    provider = {
        'id': uuid.uuid4().hex[:8],
        'name': data['name'],
        'provider_type': data.get('provider_type', 'oidc'),
        'issuer_url': data.get('issuer_url', '').rstrip('/'),
        'client_id': data['client_id'],
        'client_secret': data['client_secret'],
        'discovery_url': data.get('discovery_url', ''),
        'scopes': data.get('scopes', 'openid email profile'),
        'logout_url': data.get('logout_url', ''),
        'enabled': True,
        'is_default': len(providers) == 0,  # first provider becomes default
        'groups_claim': data.get('groups_claim', 'groups'),
    }
    # Auto-fill discovery URL if not provided
    if not provider['discovery_url'] and provider['issuer_url']:
        if provider['provider_type'] == 'authentik':
            slug = data.get('app_slug', '')
            if slug:
                provider['discovery_url'] = f"{provider['issuer_url']}/application/o/{slug}/.well-known/openid-configuration"
                provider['logout_url'] = provider['logout_url'] or f"{provider['issuer_url']}/application/o/{slug}/end-session/"
                provider['app_slug'] = slug
        else:
            provider['discovery_url'] = f"{provider['issuer_url']}/.well-known/openid-configuration"
    providers.append(provider)
    save_providers(providers)
    return provider


def update_provider(provider_id, data):
    providers = load_providers()
    for p in providers:
        if p['id'] == provider_id:
            for key in ('name', 'provider_type', 'issuer_url', 'client_id', 'client_secret',
                        'discovery_url', 'scopes', 'logout_url', 'enabled', 'app_slug', 'groups_claim'):
                if key in data:
                    p[key] = data[key]
            # Re-derive discovery_url if cleared
            if not p.get('discovery_url') and p.get('issuer_url'):
                issuer = p['issuer_url'].rstrip('/')
                if p.get('provider_type') == 'authentik' and p.get('app_slug'):
                    p['discovery_url'] = f"{issuer}/application/o/{p['app_slug']}/.well-known/openid-configuration"
                    p['logout_url'] = p.get('logout_url') or f"{issuer}/application/o/{p['app_slug']}/end-session/"
                else:
                    p['discovery_url'] = f"{issuer}/.well-known/openid-configuration"
            save_providers(providers)
            return p
    return None


def delete_provider(provider_id):
    providers = load_providers()
    providers = [p for p in providers if p['id'] != provider_id]
    # If we removed the default, promote the first enabled one
    if providers and not any(p.get('is_default') for p in providers):
        for p in providers:
            if p.get('enabled', True):
                p['is_default'] = True
                break
    save_providers(providers)


def set_default_provider(provider_id):
    providers = load_providers()
    for p in providers:
        p['is_default'] = (p['id'] == provider_id)
    save_providers(providers)


def migrate_from_env():
    """One-time migration: if no providers exist but env vars are set, create one."""
    if load_providers():
        return  # already configured
    client_id = os.environ.get('AUTHENTIK_CLIENT_ID')
    client_secret = os.environ.get('AUTHENTIK_CLIENT_SECRET')
    if not client_id or not client_secret:
        return  # nothing to migrate
    app_slug = os.environ.get('AUTHENTIK_APP_SLUG', 'hydrotest')
    add_provider({
        'name': 'Authentik',
        'provider_type': 'authentik',
        'issuer_url': 'https://auth.thebrendan.online',
        'client_id': client_id,
        'client_secret': client_secret,
        'app_slug': app_slug,
        'scopes': 'openid email profile',
    })
