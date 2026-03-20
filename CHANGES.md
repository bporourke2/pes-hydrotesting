# Changelog

## [Unreleased] — feature/test-execution-pv-rupture

### New Features

#### Test Execution Page (`/test/<save_id>`)
- New live test recording page accessible from any saved analysis
- **Readings log** captures time, ambient/pipe/ground temperatures, pressure, and comments per interval
- **Temperature-corrected pressure** computed via empirical linear regression (pipe temp vs. pressure) from test start
- **Deviation tracking** — actual pressure minus temperature-corrected expected pressure; sustained negative values flag potential leaks
- **Halliburton volume balance** (Eq. III–VIII) computes ΔV per interval accounting for water compressibility, pipe hoop strain, and thermal expansion of water and steel
- **Cumulative unaccounted volume loss** subtracts intentional bleed volumes so only true leakage accumulates
- **Leak detection threshold** — configurable alert level (default −10 psi); rows color-coded green/amber/red
- **Event types** with row highlighting: Test Start, Leak Check, Pressure Bleed, Start PV Plot, End PV Plot, Test End
- **Live pressure & temperature chart** (Plotly) with time axis derived from test date and timezone
- **MAOP Certification panel** (49 CFR 192 / 195) — computes certified MAOP from test pressure, install date, class location, converted-pipeline flag, and governing code; dynamically selects applicable table and provisions
- **Pass / Fail finalization** — locks the record; all inputs become read-only
- **Admin unlock** — admins can remove finalized status and re-open the record for editing
- **Test retest** — failed tests are archived to history; a fresh attempt begins on the same segment
- **Auto-save** on every table entry (1.2 s debounce); manual Save Now button also available
- **Class certification persists** across saves — cert class, install date, and converted-pipeline checkbox are included in the save payload and restored on reload
- Multi-attempt badge shown when a prior test has been archived

#### PV Plot ↔ Test Execution Integration
- **Start PV Plot button** in the test execution sidebar opens a modal prompting for the plot start time (pre-populated with current wall-clock time)
- Confirming the modal auto-inserts a `pv_start` row into the readings log at the correct chronological position (before any row with a later time), then navigates to the PV plot page
- **Chronological row insertion** — both `pv_start` (on start) and `pv_end` (on return) are inserted by comparing HH:MM against existing rows rather than always appending
- PV page detects `?from=exec` and switches its back link to **"← Return to Execution"**
- A prominent **Return to Execution** button on the PV page saves PV data then computes the end time from the PV start time plus the sum of all recorded interval seconds; navigates back with `?pv_end=HH:MM`
- On return, the execution page auto-inserts a `pv_end` row at the computed time and auto-saves; the `?pv_end` URL param is then cleaned from the browser history
- When PV data exists, the sidebar shows a summary (reading count, pump config, last updated) and an **Open PV Plot** button that carries the original `pv_start` row time through to the PV page for correct end-time calculation on subsequent returns
- **Unlink PV Plot** — removes `pv_data` from the save file; blocked when the test is finalized

#### Rupture Analysis Page (`/rupture/<save_id>`)
- New hydraulic rupture / fluid release analysis page accessible from saved analyses
- Calculates drained pipe length and release volumes (ft³, gal, bbl) for single or multiple simultaneous rupture points
- Physics: atmospheric pressure at the rupture opening creates hydraulic head; fluid drains from segments below the threshold elevation
- Tracks **upstream main**, **upstream pocket**, **downstream main**, and **downstream pocket** drainage zones independently, matching spreadsheet methodology
- **Multi-rupture** mode — union of drained segments across all rupture points with attributed volumes per rupture (no double-counting)
- Configurable fluid specific weight (default 62.4 lb/ft³ fresh water)
- Saved analyses can be named and reloaded; stored in the save JSON under `rupture_analyses`
- Results include rupture elevation, atmospheric pressure at elevation, hydraulic head, threshold elevation, pipe ID, and percentage of section volume released

#### NPS / OD Lookup (`logic.py`)
- Full **API 5L Nominal Pipe Size table** (NPS 0.125" through NPS 65") added to `logic.py` as `NPS_OD`
- `nps_to_od(nps)` — returns outside diameter in inches for a given NPS designation
- `od_to_nps(od)` — reverse-lookup NPS key from a measured OD value
- `get_od(params)` helper in `app.py` resolves OD preferring NPS lookup, falling back to stored numeric OD
- **NPS dropdown selector** replaces the raw OD text field in the results page parameter panel; covers all standard API 5L sizes with OD shown for each option

#### Wall Thickness Constant Support (`logic.py`, `mapping.html`)
- Column mapping now supports `wt = '__constant__'` mode when survey data does not include a wall thickness column
- User specifies a constant WT value (`wt_constant`); `Section` and `build_wt_column()` apply it uniformly across all stations
- Prevents errors on files without a WT column while still producing valid SMYS and volume calculations

#### Temperature Correction Factor (`logic.py`)
- `temp_correction_factor(od, avg_wt)` computes **K (psi/°F)** from first principles — constant-volume thin-wall constraint accounting for water bulk modulus, steel thermal expansion, and pipe hoop compliance
- K factor displayed in the test execution sidebar and used for temperature-corrected pressure calculations

#### Multi-Rupture Analysis (`logic.py`)
- `multi_rupture_analysis()` runs independent single-rupture analyses for each input station, unions the drained flags, and attributes each drained segment to the nearest claiming rupture to prevent double-counting
- Combined total release volumes reported alongside per-rupture attributed volumes

---

### Improvements

#### Results Page
- NPS selector with full API 5L table replaces the free-text OD input field
- All parameter labels now have **tooltip definitions** (hover `?`) explaining engineering significance: Fill GPM, Dewater GPM, Pre-pack CFM, Min Test Pressure, Test Site Station, Dewater Site Station, Test Window Lower Buffer, Test Window Size, Override Pre-pack/Vent, Pipe Grade, SMYS Threshold, Fill Direction, Head Factor, Unrestrained Footage

#### Tooltip System
- New `static/tooltips.css` provides a consistent hover-tooltip component used across results, test execution, and PV plot pages
- Tooltips positioned above the element by default; `.tt-c` variant centers above

#### Company Tree (Welcome Page)
- Company name now falls back to the portfolio's stored company when `project_info.owner_company` is blank, preventing orphaned saves from appearing under "No Company"
- Logo on welcome page is now a clickable home link

#### Settings Page
- Logo is now a clickable home link
- Scroll-to-anchor behavior corrected to handle all `#section` anchors, not only `#oauth`
- Dev-mode **Reference Documentation** section — admins can upload PDF, DOCX, TXT, MD, XLSX, and CSV files for AI reference; upload and delete supported

#### PV Plot Page
- `saveData()` refactored to accept an optional `onComplete` callback, enabling save-then-navigate flows
- Back link dynamically switches between "← Back to Analysis" and "← Return to Execution" based on `?from=exec` referrer param

---

### Bug Fixes

- **Wall thickness column validation** — `Section.__init__` now distinguishes `__constant__` WT from a missing column, eliminating false-positive "column not found" errors
- **Station parsing with commas** — `parse_station()` strips comma thousands-separators before splitting on `+`
- **Duplicate station prevention** — `Section` deduplicates stations (keeps first occurrence) before merging to prevent cartesian product explosions
- **Inside diameter guard** — raises a clear error if `OD − 2×WT ≤ 0` at any station
- **SMYS threshold validation** — raises immediately if threshold ≤ 0 before division
- **PV modal overlay click** — clicking outside the Start PV Plot modal dismisses it
- **Test execution locked inputs** — `cert-class` select correctly disabled in locked state alongside date/timezone/threshold fields
- **Admin role persistence** — `role_locked: true` set on first-user auto-promotion and manual role assignments to prevent OIDC group updates from overwriting the role on subsequent logins
- **Settings anchor scroll** — all `#anchor` fragments now scroll correctly, not only `#oauth`
