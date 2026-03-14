# PES Hydrotesting Tool

A Flask web application for pipeline hydrotest engineering analysis. Computes pressure profiles, test windows, filling/dewatering simulations, prepack calculations, and squeeze volumes from Excel survey data.

## Features

### Analysis Engine
- **Pressure profile calculation** using Barlow's formula with configurable SMYS thresholds (supports grades B through X80)
- **Test window optimization** with automatic gauge pressure computation at any test site elevation
- **Violation detection** — flags stations where the lower test bound falls below minimum test pressure or the upper bound exceeds SMYS limits
- **Fill/dewater/prepack time estimates** based on pipeline volume and flow rates
- **Squeeze volume calculation** accounting for water compressibility and pipe expansion (restrained/unrestrained)
- **PV plot execution page** with real-time yield detection, slope deviation analysis, and SPM monitoring

### Interactive & Print Reports
- Interactive Plotly charts for pressure and filling profiles
- Static matplotlib charts for print-quality PDF output
- Print view includes project info, violation banners, and engineering controls
- Configurable paper size (letter/tabloid) and orientation

### Project Management
- **Project setup** — governing code, owner company, portfolio, spread, testing contractor, company approver, contractor representative
- **4-level hierarchy** on home page: Company > Portfolio > Spread > Test Segment
- **Settings page** for managing owner companies and portfolios
- **Save/version system** — overwrite saves with full version history, restore any prior version
- **Portfolio-company linking** with dynamic filtering on project setup

### Security
- Authentik OIDC authentication
- Path traversal protection on all save routes (8-char hex ID validation)
- Atomic JSON writes to prevent data corruption
- Session cookie hardening (SameSite, HttpOnly, Secure)
- HTML-escaped user inputs in templates
- Upload isolation per user session
- Input validation on all engineering parameters (OD, WT, grade, station ranges)

## Setup

### Requirements
- Python 3.12+
- An Authentik instance for OIDC authentication (or modify `login_required` for local dev)

### Installation

```bash
git clone https://github.com/bporourke2/pes-hydrotesting.git
cd pes-hydrotesting
pip install -r requirements.txt
```

### Configuration

Create a `.env` file:

```env
SECRET_KEY=your-secret-key-here
AUTHENTIK_CLIENT_ID=your-client-id
AUTHENTIK_CLIENT_SECRET=your-client-secret
AUTHENTIK_APP_SLUG=hydrotest
SESSION_COOKIE_SECURE=true
```

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

1. **Project Setup** — select governing code, owner company, portfolio, and spread
2. **Upload & Map** — upload an Excel survey file and map Station, Elevation, and Wall Thickness columns
3. **Results** — adjust engineering parameters (test site, flow rates, SMYS threshold, test window) and view pressure profiles
4. **Save** — save the analysis to a portfolio with versioning support
5. **PV Plot** — record pressure-volume readings during test execution for yield monitoring
6. **Print** — generate a print-ready report with all project info and violation flags

## Project Structure

```
app.py                  # Flask application (routes, session, saves)
logic.py                # Engineering calculations (Section, PipelineApp)
templates/
  welcome.html          # Home page with company/portfolio tree
  project_setup.html    # Project info form (company, portfolio, spread)
  mapping.html          # Excel upload and column mapping
  results.html          # Analysis results with interactive charts
  print.html            # Print-optimized report
  pv.html               # PV plot execution page
  settings.html         # Company and portfolio management
saves/                  # JSON save files and data copies
  _portfolios.json      # Portfolio definitions
  _companies.json       # Company list
data/
  Testdata.xlsx         # Demo survey data
static/
  peslogo.png           # Logo
```

## Data Files
- `data/Testdata.xlsx` — sample 42" X70 pipeline survey data
- `data/mvp_spread_h_v5.xlsx` — Mountain Valley Pipeline Spread H survey

## License
MIT
