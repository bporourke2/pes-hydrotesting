# PES Hydrotesting Tool

A Flask web application for hydrotest engineering analysis, including pressure profiles, filling simulations, and data mapping from Excel surveys.

## Features
- Upload Excel data (e.g., Testdata.xlsx) for analysis.
- Demo mode with sample data.
- Interactive plots for pressure and filling profiles.
- Sidebar controls for engineering parameters (e.g., stations, pressures, flow rates).
- Results table with SMYS checks and warnings.

## Setup
1. Clone the repo: `git clone https://github.com/bporourke2/pes-hydrotesting.git`
2. Install dependencies: `pip install -r requirements.txt`
3. Place `peslogo.png` in the `static/` folder (or use a placeholder).
4. Run the app: `python app.py`
5. Open http://127.0.0.1:5000/ in your browser.

## Usage
- Start at the welcome page: Upload new data or use demo.
- Map columns in mapping.html.
- Adjust parameters and view results in results.html.

## Data Files
- `data/Testdata.xlsx`: Sample survey data.
- `data/mvp_spread_h_v5.xlsx`: Additional spreadsheet (original: MVP SPREAD H v5.xlsx).

## Notes
- Requires Python 3.12+.
- Debug mode is enabled in app.py for development.
- Ensure Excel files have the expected structure (e.g., columns for Station, Elevation, Wall Thickness).
- For production, disable debug and secure the secret key.

## License
MIT
