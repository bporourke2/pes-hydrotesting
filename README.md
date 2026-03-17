# PES Hydrotesting Tool

A Flask web application for pipeline hydrotest engineering analysis. Computes pressure profiles, test windows, filling/dewatering simulations, prepack calculations, and squeeze volumes from Excel survey data.

## Features

### Analysis Engine
- **Pressure profile calculation** using Barlow's formula with configurable SMYS thresholds (supports grades B through X80)
- **Test window optimization** with automatic gauge pressure computation at any test site elevation
- **Violation detection** — flags stations where the lower test bound falls below minimum test pressure or the upper bound exceeds SMYS limits
- **Fill/dewater/prepack time estimates** based on pipeline volume and flow rates
- **Squeeze volume calculation** accounting for water compressibility and pipe expansion (restrained/unrestrained)
- **Configurable head factor** for pressure calculations
- **PV plot execution page** with real-time yield detection, slope deviation analysis, and SPM monitoring

### Interactive & Print Reports
- Interactive Plotly charts for pressure and filling profiles
- Static matplotlib charts for print-quality PDF output
- Print view includes project info, violation banners, and engineering controls
- Configurable paper size (letter/tabloid) and orientation

### Project Management
- **Project setup** — governing code, owner company, portfolio, spread, testing contractor, company approver, contractor representative
- **4-level hierarchy** on home page: Company > Portfolio > Spread > Test Segment
- **Settings page** (admin only) for managing companies, portfolios, OAuth providers, and users
- **Save/version system** — overwrite saves with full version history, restore any prior version
- **Portfolio-company linking** with dynamic filtering on project setup
- **Excel sheet selector** — choose which sheet to import from multi-sheet workbooks

### Authentication & Authorization
- **Multi-provider OAuth/OIDC** — configure multiple identity providers through the UI (Authentik, Entra ID, Okta, etc.)
- **Automatic legacy migration** — existing `.env` Authentik config is migrated to the new provider system on first run
- **First-user auto-promotion** — the first user to log in is automatically granted admin
- **Role-based access control** with two roles:
  - `hydro` — standard user: run analyses, save results, view PV plots, add companies and portfolios
  - `hydro-admin` — admin: all standard permissions plus edit/delete portfolios, delete companies, manage OAuth providers, manage users and roles
- **Portfolio-based access filtering** — standard users only see saves in their assigned portfolios; admins see everything
- **Group-based role resolution** — roles derived from OIDC group claims, with manual admin override option
- **Login denied page** for users without matching OIDC groups

### Security
- Path traversal protection on all save routes (8-char hex ID validation)
- Atomic JSON writes to prevent data corruption
- Session cookie hardening (SameSite, HttpOnly, Secure)
- HTML-escaped user inputs in templates
- Upload isolation per user session
- Input validation on all engineering parameters (OD, WT, grade, station ranges)

## Setup

### Requirements
- Python 3.12+
- An OIDC-compatible identity provider (Authentik, Entra ID, Okta, etc.)

### Installation

```bash
git clone https://github.com/bporourke2/pes-hydrotesting.git
cd pes-hydrotesting
pip install -r requirements.txt
```

### Configuration

Create a `.env` file with a secret key:

```env
SECRET_KEY=your-secret-key-here
SESSION_COOKIE_SECURE=true
```

OAuth providers are configured through the web UI on first launch — no `.env` variables needed for OIDC. Legacy Authentik `.env` variables (`AUTHENTIK_CLIENT_ID`, `AUTHENTIK_CLIENT_SECRET`, `AUTHENTIK_APP_SLUG`) are automatically migrated if present.

### Running

```bash
python app.py
```

Or via systemd (see `/etc/systemd/system/hydrotest.service`):

```bash
systemctl start hydrotest
```

The app runs on port 5000.

## Usage

1. **OAuth Setup** — on first launch, configure an identity provider through the setup wizard
2. **Login** — authenticate via your configured provider; the first user is auto-promoted to admin
3. **Project Setup** — select governing code, owner company, portfolio, and spread
4. **Upload & Map** — upload an Excel survey file, select the sheet, and map Station, Elevation, and Wall Thickness columns
5. **Results** — adjust engineering parameters (test site, flow rates, SMYS threshold, test window) and view pressure profiles
6. **Save** — save the analysis to a portfolio with versioning support
7. **PV Plot** — record pressure-volume readings during test execution for yield monitoring
8. **Print** — generate a print-ready report with all project info and violation flags

## Project Structure

```
app.py                  # Flask application (routes, session, saves)
logic.py                # Engineering calculations (Section, PipelineApp)
oauth_config.py         # Multi-provider OAuth/OIDC configuration
user_store.py           # User management, roles, and portfolio access
templates/
  welcome.html          # Home page with company/portfolio tree
  project_setup.html    # Project info form (company, portfolio, spread)
  mapping.html          # Excel upload and column mapping
  results.html          # Analysis results with interactive charts
  print.html            # Print-optimized report
  pv.html               # PV plot execution page
  settings.html         # Admin: companies, portfolios, OAuth, users
  oauth_setup.html      # First-run OAuth provider setup wizard
  login_select.html     # Multi-provider login selection
  login_denied.html     # Access denied (no matching OIDC group)
config/
  oauth_providers.json  # OAuth provider definitions (auto-created)
  users.json            # User records and role assignments (auto-created)
saves/
  _portfolios.json      # Portfolio definitions
  _companies.json       # Company list
data/
  Testdata.xlsx         # Demo survey data
static/
  peslogo.png           # Logo
  plotly-2.35.2.min.js  # Bundled Plotly library
```

## Data Files
- `data/Testdata.xlsx` — sample 42" X70 pipeline survey data
- `data/mvp_spread_h_v5.xlsx` — Mountain Valley Pipeline Spread H survey

## License
MIT
