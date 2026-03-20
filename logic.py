import pandas as pd
import numpy as np
import math
import io
import base64
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import plotly.graph_objects as go  # Added for interactive plots

# API 5L Nominal Pipe Sizes → Outside Diameter (inches)
NPS_OD = {
    '0.125': 0.405, '0.25': 0.540, '0.375': 0.675, '0.5': 0.840,
    '0.75': 1.050, '1': 1.315, '1.25': 1.660, '1.5': 1.900,
    '2': 2.375, '2.5': 2.875, '3': 3.500, '3.5': 4.000,
    '4': 4.500, '5': 5.563, '6': 6.625, '8': 8.625,
    '10': 10.750, '12': 12.750, '14': 14.000, '16': 16.000,
    '18': 18.000, '20': 20.000, '22': 22.000, '24': 24.000,
    '26': 26.000, '28': 28.000, '30': 30.000, '32': 32.000,
    '34': 34.000, '36': 36.000, '38': 38.000, '40': 40.000,
    '42': 42.000, '44': 44.000, '46': 46.000, '48': 48.000,
    '52': 52.000, '56': 56.000, '60': 60.000, '65': 65.000,
}

def nps_to_od(nps):
    """Return OD (inches) for a given NPS designation per API 5L, or None."""
    return NPS_OD.get(str(nps))

def od_to_nps(od):
    """Reverse-lookup NPS key for a given OD value, or None."""
    od = float(od)
    for k, v in NPS_OD.items():
        if abs(v - od) < 0.001:
            return k
    return None

def parse_station(st):
    st = str(st).replace(',', '')  # Handle commas
    if '+' in st:
        parts = st.split('+')
        if len(parts) != 2:
            raise ValueError(f"Invalid station format '{st}' — expected format like '1200+50'")
        sign = -1 if (float(parts[0]) < 0 or parts[0].lstrip().startswith('-')) else 1
        return float(parts[0]) * 100 + sign * float(parts[1])
    else:
        return float(st)

def station_format(x):
    x = float(x)
    sign = '-' if x < 0 else ''
    x = abs(x)
    sta = int(x // 100)
    rem = x % 100
    if rem == int(rem):
        return f"{sign}{sta}+{int(rem):02d}"
    else:
        return f"{sign}{sta}+{rem:05.2f}"

class PipelineApp:
    def __init__(self, survey_file, od=42, smys=70000, test_percent=1.04, head_factor=0.433, _df=None, sheet_name=0):
        self.od = od
        self.smys = smys
        self.test_percent = test_percent
        self.head_factor = head_factor
        self.survey_file = survey_file
        self.full_df = _df if _df is not None else pd.read_excel(survey_file, sheet_name=sheet_name)

    def get_preview(self):
        from markupsafe import escape
        df_preview = self.full_df.head(10).copy()
        # Sanitize column names to prevent XSS via |safe rendering
        df_preview.columns = [str(escape(c)) for c in df_preview.columns]
        safe_columns = [str(escape(c)) for c in self.full_df.columns]
        return df_preview.to_html(classes='preview-table'), safe_columns

    def generate_plot(self, df, min_test=None, params=None, gauge_lower=None, gauge_upper=None, prepack_time=None, sec=None, vent_time=None, static=False, smys_threshold_pct=None):
        # Data cleaning to ensure numeric values and no NaNs for Plotly
        df = df.copy()
        for col in ['Station', 'Elevation', 'Local_P', 'SMYS_Limit', 'Lower_Bound_P', 'Upper_Bound_P', 'Prepack_Profile', 'Req_Back_P']:
            if col in df:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['Station', 'Elevation'])
        df['Station_Formatted'] = df['Station'].apply(station_format)  # Recreate after cleaning

        if not static:
            fig = go.Figure()

            # Elevation on left y-axis
            fig.add_trace(go.Scatter(x=df['Station'], y=df['Elevation'], name='Elevation (ft)', line=dict(color='#101820'), yaxis='y1', customdata=df['Station_Formatted'], hovertemplate='Station: %{customdata}<br>Elevation: %{y:.1f} ft<extra></extra>'))

            # Pressures on right y-axis
            fig.add_trace(go.Scatter(x=df['Station'], y=df['Local_P'], name='Target Test Pressure (psig)', line=dict(color='green'), yaxis='y2', customdata=df['Station_Formatted'], hovertemplate='Station: %{customdata}<br>Target Test Pressure: %{y:.0f} psig<extra></extra>'))
            fig.add_trace(go.Scatter(x=df['Station'], y=df['SMYS_Limit'], name=f'SMYS Threshold ({int(smys_threshold_pct or self.test_percent*100)}%)', line=dict(color='#d62728', dash='dash'), yaxis='y2', customdata=df['Station_Formatted'], hovertemplate='Station: %{customdata}<br>SMYS Limit: %{y:.0f} psig<extra></extra>'))

            # Red shading for test window exceeding SMYS (from Upper_Bound_P down to SMYS_Limit)
            exceed_mask = df['Upper_Bound_P'] > df['SMYS_Limit']
            fig.add_trace(go.Scatter(x=df['Station'][exceed_mask], y=df['Upper_Bound_P'][exceed_mask], fill=None, mode='lines', line=dict(color='red', width=0), showlegend=False, yaxis='y2'))
            fig.add_trace(go.Scatter(x=df['Station'][exceed_mask], y=df['SMYS_Limit'][exceed_mask], fill='tonexty', mode='lines', fillcolor='rgba(255,0,0,0.3)', line=dict(color='red', width=0), name='Test Window Exceeds SMYS', yaxis='y2'))

            # Fill below min test (red shaded) — use Lower_Bound_P to match violation detection
            below_mask = df['Lower_Bound_P'] < min_test
            fig.add_trace(go.Scatter(x=df['Station'][below_mask], y=df['Lower_Bound_P'][below_mask], fill=None, mode='lines', line=dict(color='red', width=0), showlegend=False, yaxis='y2'))
            fig.add_trace(go.Scatter(x=df['Station'][below_mask], y=[min_test] * len(df['Station'][below_mask]), fill='tonexty', mode='lines', fillcolor='rgba(255,0,0,0.3)', line=dict(color='red', width=0), name='Below Min Test', yaxis='y2'))

            if min_test is not None:
                fig.add_hline(y=min_test, line=dict(color='red', dash='dash'), annotation_text='Min Test Pressure', yref='y2')

            if 'Lower_Bound_P' in df.columns:
                fig.add_trace(go.Scatter(x=df['Station'], y=df['Lower_Bound_P'], name='Test Window Minimum', line=dict(color='orange', dash='dash'), yaxis='y2', customdata=df['Station_Formatted'], hovertemplate='Station: %{customdata}<br>Test Window Minimum: %{y:.0f} psig<extra></extra>'))
            if 'Upper_Bound_P' in df.columns:
                fig.add_trace(go.Scatter(x=df['Station'], y=df['Upper_Bound_P'], name='Test Window Maximum', line=dict(color='orange', dash='dash'), yaxis='y2', customdata=df['Station_Formatted'], hovertemplate='Station: %{customdata}<br>Test Window Maximum: %{y:.0f} psig<extra></extra>'))

            fig.update_layout(
                title='Pressure Profile',
                xaxis=dict(title='Station', rangeslider=dict(visible=True)),
                yaxis=dict(title='Elevation (ft)', side='left'),
                yaxis2=dict(title='Pressure (psig)', side='right', overlaying='y', autorange=True),
                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
                hovermode='x unified',
                dragmode='zoom',
                height=600,
                margin=dict(l=50, r=50, t=50, b=50)
            )

            sta_min = df['Station'].min()
            sta_max = df['Station'].max()
            step = 1000
            tickvals = list(range(int(sta_min // step) * step, int(sta_max) + step, step))
            tickvals = [t for t in tickvals if sta_min <= t <= sta_max]
            ticktext = [station_format(t) for t in tickvals]
            fig.update_xaxes(tickvals=tickvals, ticktext=ticktext)

            # Build marker list: fill site, test site, high point, low point
            markers = []
            if params and not df.empty:
                for site_key, label, color in [('fill_site', 'Fill Site', 'green'), ('test_site', 'Test Site', 'blue')]:
                    val = params.get(site_key)
                    if val is not None:
                        row = df.iloc[(df['Station'] - parse_station(val)).abs().argsort()[:1]]
                        if not row.empty:
                            markers.append((row['Station'].values[0], row['Elevation'].values[0], label, color))
                high_row = df.loc[df['Elevation'].idxmax()]
                low_row = df.loc[df['Elevation'].idxmin()]
                markers.append((high_row['Station'], high_row['Elevation'], 'High Point', '#9b59b6'))
                markers.append((low_row['Station'], low_row['Elevation'], 'Low Point', '#e67e22'))

            def add_plotly_markers(fig, markers):
                if not markers:
                    return
                sta_range = max(df['Station'].max() - df['Station'].min(), 1)
                sta_min = df['Station'].min()
                tol = sta_range * 0.005
                y_levels = [0.97, 0.88, 0.79, 0.70]
                groups = []
                for i in range(len(markers)):
                    placed = False
                    for g in groups:
                        if abs(markers[g[0]][0] - markers[i][0]) < tol:
                            g.append(i)
                            placed = True
                            break
                    if not placed:
                        groups.append([i])
                for g in groups:
                    for rank, idx in enumerate(g):
                        x, elev, label, color, *extra = markers[idx]
                        sublabel = extra[0] if extra else f'{station_format(x)}, {elev:.1f} ft'
                        y_pos = y_levels[min(rank, len(y_levels) - 1)]
                        xanchor = 'right' if x > sta_min + sta_range * 0.75 else 'left'
                        fig.add_vline(x=x, line=dict(color=color, dash='dash', width=1))
                        fig.add_annotation(
                            x=x, y=y_pos, xref='x', yref='paper',
                            text=f'{label}<br>({sublabel})',
                            showarrow=False, xanchor=xanchor,
                            font=dict(color=color, size=10),
                            bgcolor='rgba(255,255,255,0.8)',
                            bordercolor=color, borderwidth=1, borderpad=3
                        )

            add_plotly_markers(fig, markers)

            plot1 = fig.to_json(engine='json')

            # Second plot - Filling Profile
            fig2 = go.Figure()

            # Elevation on left
            fig2.add_trace(go.Scatter(x=df['Station'], y=df['Elevation'], name='Elevation (ft)', line=dict(color='#101820'), yaxis='y1', customdata=df['Station_Formatted'], hovertemplate='Station: %{customdata}<br>Elevation: %{y:.1f} ft<extra></extra>'))

            # Prepack Profile on right
            fig2.add_trace(go.Scatter(x=df['Station'], y=df['Prepack_Profile'], name='Prepack Profile (psig)', line=dict(color='#EAAA00'), yaxis='y2', customdata=df['Station_Formatted'], hovertemplate='Station: %{customdata}<br>Prepack: %{y:.0f} psig<extra></extra>'))

            # Required Backpressure
            fig2.add_trace(go.Scatter(x=df['Station'], y=df['Req_Back_P'], name='Required Backpressure (psig)', line=dict(color='green'), yaxis='y2', customdata=df['Station_Formatted'], hovertemplate='Station: %{customdata}<br>Required Backpressure: %{y:.0f} psig<extra></extra>'))

            fig2.update_layout(
                title='Filling Profile',
                xaxis=dict(title='Station', rangeslider=dict(visible=True)),
                yaxis=dict(title='Elevation (ft)', side='left'),
                yaxis2=dict(title='Pressure (psig)', side='right', overlaying='y', autorange=True),
                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
                hovermode='x unified',
                dragmode='zoom',
                height=600,
                margin=dict(l=50, r=50, t=50, b=50)
            )

            fig2.update_xaxes(tickvals=tickvals, ticktext=ticktext)

            # Build fig2-specific extra markers: prepack start and vent threshold station
            fig2_extra = []
            if sec is not None and params and not df.empty:
                fill_val = params.get('fill_site')
                if fill_val is not None:
                    fill_row = df.iloc[(df['Station'] - parse_station(fill_val)).abs().argsort()[:1]]
                    if not fill_row.empty:
                        fig2_extra.append((fill_row['Station'].values[0], fill_row['Elevation'].values[0], f'Pre-pack ({sec.prepack_psi:.0f} psig)', '#EAAA00'))
                if hasattr(sec, 'cum_gal_at_vent') and sec.cum_gal_at_vent is not None and 'Cum_Gal' in df.columns:
                    vent_row = df.iloc[(df['Cum_Gal'] - sec.cum_gal_at_vent).abs().argsort()[:1]]
                    if not vent_row.empty:
                        fig2_extra.append((vent_row['Station'].values[0], vent_row['Elevation'].values[0], f'Vent Threshold ({sec.vent_psi:.0f} psig)', 'red', f'{sec.cum_gal_at_vent:,.0f} gal into filling'))

            add_plotly_markers(fig2, markers + fig2_extra)

            plot2 = fig2.to_json(engine='json')

            return plot1, plot2

        else:
            # Build markers for static plots (same logic as interactive)
            markers = []
            if params and not df.empty:
                for site_key, label, color in [('fill_site', 'Fill Site', 'green'), ('test_site', 'Test Site', 'blue')]:
                    val = params.get(site_key)
                    if val is not None:
                        row = df.iloc[(df['Station'] - parse_station(val)).abs().argsort()[:1]]
                        if not row.empty:
                            markers.append((row['Station'].values[0], row['Elevation'].values[0], label, color))
                high_row = df.loc[df['Elevation'].idxmax()]
                low_row = df.loc[df['Elevation'].idxmin()]
                markers.append((high_row['Station'], high_row['Elevation'], 'High Point', '#9b59b6'))
                markers.append((low_row['Station'], low_row['Elevation'], 'Low Point', '#e67e22'))

            def add_mpl_markers(ax, mkrs=None):
                if mkrs is None:
                    mkrs = markers
                if not mkrs or df.empty:
                    return
                sta_range = max(df['Station'].max() - df['Station'].min(), 1)
                sta_min = df['Station'].min()
                tol = sta_range * 0.005
                # Place labels above the plot (y > 1.0 in axes coordinates)
                y_levels = [1.03, 1.13, 1.23, 1.33]
                groups = []
                for i in range(len(mkrs)):
                    placed = False
                    for g in groups:
                        if abs(mkrs[g[0]][0] - mkrs[i][0]) < tol:
                            g.append(i)
                            placed = True
                            break
                    if not placed:
                        groups.append([i])
                trans = ax.get_xaxis_transform()
                for g in groups:
                    for rank, idx in enumerate(g):
                        x, elev, label, color, *extra = mkrs[idx]
                        sublabel = extra[0] if extra else f'{station_format(x)}, {elev:.1f} ft'
                        y_pos = y_levels[min(rank, len(y_levels) - 1)]
                        ha = 'right' if x > sta_min + sta_range * 0.75 else 'left'
                        ax.axvline(x=x, color=color, linestyle='--', linewidth=1)
                        ax.text(x, y_pos, f'{label}\n({sublabel})',
                                transform=trans, color=color, fontsize=7,
                                verticalalignment='bottom', horizontalalignment=ha,
                                clip_on=False,
                                bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.9, edgecolor=color, linewidth=0.5))

            # Helper to apply station-format x-axis ticks
            def apply_station_xticks(ax):
                sta_min = df['Station'].min()
                sta_max = df['Station'].max()
                step = 1000
                tickvals = list(range(int(sta_min // step) * step, int(sta_max) + step, step))
                tickvals = [t for t in tickvals if sta_min <= t <= sta_max]
                ax.set_xticks(tickvals)
                ax.set_xticklabels([station_format(t) for t in tickvals], rotation=45, ha='right', fontsize=8)

            # Static plots using matplotlib for print
            fig1, ax1 = plt.subplots(figsize=(12, 7))
            fig1.subplots_adjust(bottom=0.22, top=0.68)
            ax2 = ax1.twinx()

            ax1.plot(df['Station'], df['Elevation'], label='Elevation (ft)', color='#101820')
            ax2.plot(df['Station'], df['Local_P'], label='Target Test Pressure (psig)', color='green')
            ax2.plot(df['Station'], df['SMYS_Limit'], label=f'SMYS Threshold ({int(smys_threshold_pct or self.test_percent*100)}%)', color='#d62728', linestyle='--')

            if 'Lower_Bound_P' in df.columns:
                ax2.plot(df['Station'], df['Lower_Bound_P'], label='Test Window Minimum', color='orange', linestyle='--')
            if 'Upper_Bound_P' in df.columns:
                ax2.plot(df['Station'], df['Upper_Bound_P'], label='Test Window Maximum', color='orange', linestyle='--')

            exceed_mask = df['Upper_Bound_P'] > df['SMYS_Limit']
            if exceed_mask.any():
                ax2.fill_between(df['Station'][exceed_mask], df['SMYS_Limit'][exceed_mask], df['Upper_Bound_P'][exceed_mask], color='red', alpha=0.3, label='Test Window Exceeds SMYS')

            if min_test is not None:
                ax2.axhline(y=min_test, color='red', linestyle='--', label='Min Test Pressure')

            add_mpl_markers(ax1)
            apply_station_xticks(ax1)

            ax1.set_xlabel('Station', labelpad=40)
            ax1.set_ylabel('Elevation (ft)')
            ax2.set_ylabel('Pressure (psig)')
            ax1.set_title('Pressure Profile')

            lines, labels = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(lines + lines2, labels + labels2, loc='upper center',
                       bbox_to_anchor=(0.5, -0.35), ncol=3, fontsize=8, framealpha=0.9)

            buf1 = io.BytesIO()
            fig1.savefig(buf1, format='png', bbox_inches='tight', dpi=150)
            buf1.seek(0)
            plot1_static = base64.b64encode(buf1.getvalue()).decode('utf-8')
            plt.close(fig1)

            # Second static plot
            fig2, ax1 = plt.subplots(figsize=(12, 7))
            fig2.subplots_adjust(bottom=0.22, top=0.68)
            ax2 = ax1.twinx()

            ax1.plot(df['Station'], df['Elevation'], label='Elevation (ft)', color='#101820')
            ax2.plot(df['Station'], df['Prepack_Profile'], label='Prepack Profile (psig)', color='#EAAA00')
            ax2.plot(df['Station'], df['Req_Back_P'], label='Required Backpressure (psig)', color='purple', linestyle='--')

            fig2_extra = []
            if sec is not None and params and not df.empty:
                fill_val = params.get('fill_site')
                if fill_val is not None:
                    fill_row = df.iloc[(df['Station'] - parse_station(fill_val)).abs().argsort()[:1]]
                    if not fill_row.empty:
                        fig2_extra.append((fill_row['Station'].values[0], fill_row['Elevation'].values[0], f'Pre-pack ({sec.prepack_psi:.0f} psig)', '#EAAA00'))
                if hasattr(sec, 'cum_gal_at_vent') and sec.cum_gal_at_vent is not None and 'Cum_Gal' in df.columns:
                    vent_row = df.iloc[(df['Cum_Gal'] - sec.cum_gal_at_vent).abs().argsort()[:1]]
                    if not vent_row.empty:
                        fig2_extra.append((vent_row['Station'].values[0], vent_row['Elevation'].values[0], f'Vent Threshold ({sec.vent_psi:.0f} psig)', 'red', f'{sec.cum_gal_at_vent:,.0f} gal into filling'))

            add_mpl_markers(ax1, markers + fig2_extra)
            apply_station_xticks(ax1)

            ax1.set_xlabel('Station', labelpad=40)
            ax1.set_ylabel('Elevation (ft)')
            ax2.set_ylabel('Pressure (psig)')
            ax1.set_title('Filling Profile')

            lines, labels = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(lines + lines2, labels + labels2, loc='upper center',
                       bbox_to_anchor=(0.5, -0.35), ncol=3, fontsize=8, framealpha=0.9)

            buf2 = io.BytesIO()
            fig2.savefig(buf2, format='png', bbox_inches='tight', dpi=150)
            buf2.seek(0)
            plot2_static = base64.b64encode(buf2.getvalue()).decode('utf-8')
            plt.close(fig2)

            return plot1_static, plot2_static

class Section:
    def __init__(self, app, params, col_map):
        if not all(k in params for k in ['start', 'end', 'min_p', 'test_site']):
            raise ValueError("Missing required parameters: start, end, min_p, or test_site")

        # Validate OD > 0 (prevents division by zero in SMYS calc)
        if app.od <= 0:
            raise ValueError("Outer diameter (OD) must be greater than zero.")

        # Validate column mapping exists in DataFrame
        for key in ('station', 'elev'):
            col_name = col_map.get(key)
            if not col_name or col_name not in app.full_df.columns:
                raise ValueError(f"Column '{col_name}' (mapped as {key}) not found in data file. Available columns: {', '.join(app.full_df.columns[:10])}")
        if col_map.get('wt') != '__constant__':
            wt_col_name = col_map.get('wt')
            if not wt_col_name or wt_col_name not in app.full_df.columns:
                raise ValueError(f"Column '{wt_col_name}' (mapped as wt) not found in data file. Available columns: {', '.join(app.full_df.columns[:10])}")

        start = parse_station(params['start'])
        end = parse_station(params['end'])

        # Parse station column to numeric before filtering to avoid lexicographic comparison on strings
        station_col = col_map['station']
        df = app.full_df.copy()
        df['_parsed_sta'] = df[station_col].apply(lambda v: parse_station(v))
        data = df[(df['_parsed_sta'] >= min(start, end)) &
                  (df['_parsed_sta'] <= max(start, end))].copy()
        data.drop(columns=['_parsed_sta'], inplace=True)

        if data.empty:
            raise ValueError("No data found for the specified start/end stations. Check mappings or range.")

        data['Station'] = pd.to_numeric(data[station_col].apply(lambda v: parse_station(v)), errors='coerce')
        data['Elevation'] = pd.to_numeric(data[col_map['elev']], errors='coerce')
        if col_map.get('wt') == '__constant__':
            data['WT'] = float(col_map.get('wt_constant', 0.5))
        else:
            data['WT'] = pd.to_numeric(data[col_map['wt']], errors='coerce')
        data = data.dropna(subset=['Station', 'Elevation', 'WT'])

        # Keep only the columns we need to avoid merge collisions with reserved names
        data = data[['Station', 'Elevation', 'WT']].copy()

        if data.empty:
            raise ValueError("No valid data after filtering NaN values. Check that Station, Elevation, and Wall Thickness columns contain numeric data.")

        # Drop duplicate stations (keeps first occurrence) to prevent merge cartesian products
        data = data.drop_duplicates(subset=['Station'], keep='first')

        # Validate wall thickness > 0 (wt=0 produces meaningless SMYS_Limit=0)
        if (data['WT'] <= 0).any():
            raise ValueError("Wall thickness must be greater than zero at all stations.")

        self.points = data
        self.length = abs(end - start)

        if start == end:
            raise ValueError("Start and end stations are the same — cannot define a test section.")

        # SMYS Threshold input — use local variable to avoid mutating shared app object
        smys_threshold = float(params.get('smys_threshold', 104))
        if smys_threshold <= 0:
            raise ValueError("SMYS threshold must be greater than zero.")
        test_percent = smys_threshold / 100
        self.test_percent = test_percent
        self.min_test_req = float(params['min_p'])
        min_test_req = self.min_test_req
        test_site = parse_station(params['test_site'])
        
        # Testing window
        min_excess = float(params.get('min_excess', 25))
        window_upper = float(params.get('window_upper', 50))
        
        # Calculate target gauge pressure at test site
        target_gauge_at_test_site = min_test_req + min_excess + (window_upper / 2)
        
        # Ensure min pressure is met everywhere 
        # Identify the high point (lowest pressure point)
        high_elev = self.points['Elevation'].max()
        test_row = data.iloc[(data['Station'] - test_site).abs().argsort()[:1]]
        if test_row.empty:
            raise ValueError("Test site station not found in data range.")
        test_elev = test_row['Elevation'].values[0]
        
        # Calculate required gauge pressure at test site to satisfy min_p at high point
        max_head_loss = (test_elev - high_elev) * app.head_factor
        
        # Round lower bound down to nearest 5, then build window off that
        raw_target = target_gauge_at_test_site - max_head_loss
        gauge_lower_floored = math.floor((raw_target - window_upper / 2) / 5) * 5
        # Ensure the highest elevation point still sees at least min_test_req.
        # Floor rounding can eat into the buffer when min_excess is small.
        min_gauge_lower = min_test_req + (high_elev - test_elev) * app.head_factor
        self.gauge_lower = max(gauge_lower_floored, math.ceil(min_gauge_lower / 5) * 5)
        self.gauge_upper = self.gauge_lower + window_upper
        self.target_gauge = self.gauge_lower + window_upper / 2
        
        self.points['Local_P'] = self.target_gauge + (test_elev - self.points['Elevation']) * app.head_factor
        self.points['Lower_Bound_P'] = self.gauge_lower + (test_elev - self.points['Elevation']) * app.head_factor
        self.points['Upper_Bound_P'] = self.gauge_upper + (test_elev - self.points['Elevation']) * app.head_factor
        self.points['SMYS_Limit'] = test_percent * (2 * app.smys * self.points['WT'] / app.od)

        # Flag stations where the lower test bound falls below the minimum test pressure
        below_mask = self.points['Lower_Bound_P'] < min_test_req
        self.min_bound_violations = self.points[below_mask][['Station', 'Lower_Bound_P']].copy() if below_mask.any() else None

        # Flag stations where the upper test bound exceeds the SMYS limit
        above_mask = self.points['Upper_Bound_P'] > self.points['SMYS_Limit']
        self.smys_bound_violations = self.points[above_mask][['Station', 'Upper_Bound_P', 'SMYS_Limit']].copy() if above_mask.any() else None
        
        # Cumulative backpressure logic to account for passed highs
        ascending = params.get('fill_direction') == '1'
        df_sorted = self.points.sort_values('Station', ascending=ascending).reset_index(drop=True)
        cum_max_elev = df_sorted['Elevation'].cummax()
        elev_diff = cum_max_elev - df_sorted['Elevation']
        req_back_p = elev_diff * app.head_factor
        df_sorted['Req_Back_P'] = req_back_p
        self.vent_psi = req_back_p.max()
        
        # Apply override for vent_psi if provided
        override_vent = params.get('override_vent')
        if override_vent is not None:
            self.vent_psi = override_vent
        
        # Validate inside diameter is positive (OD - 2*WT)
        max_wt = self.points['WT'].max()
        if app.od - 2 * max_wt <= 0:
            raise ValueError(f"Pipe inside diameter is zero or negative (OD={app.od}, max WT={max_wt}). Check your OD and wall thickness values.")

        # Calculate cumulative volumes for filled part
        cum_vol = [0.0]
        for i in range(len(df_sorted) - 1):
            dist = abs(df_sorted.loc[i+1, 'Station'] - df_sorted.loc[i, 'Station'])
            wt_avg = (df_sorted.loc[i, 'WT'] + df_sorted.loc[i+1, 'WT']) / 2
            id_in = app.od - 2 * wt_avg
            seg_vol_ft3 = math.pi * (id_in / 24)**2 * dist
            seg_vol_gal = seg_vol_ft3 * 7.4805
            cum_vol.append(cum_vol[-1] + seg_vol_gal)
        total_vol = cum_vol[-1]
        self.volume_gal = total_vol
        
        df_sorted['Cum_Gal'] = cum_vol
        
        atm = 14.7
        v_rem = total_vol - np.array(cum_vol)
        v_rem[v_rem == 0] = 1e-6

        if total_vol == 0:
            self.prepack_psi = 0
            prepack_profile = [0.0] * len(df_sorted)
            self.cum_gal_at_vent = 0.0
            df_sorted['Prepack_Profile'] = prepack_profile
        else:
            req_abs = req_back_p + atm
            min_prepack_abs = np.max(req_abs * v_rem / total_vol)
            self.prepack_psi = min_prepack_abs - atm
            prepack_abs = self.prepack_psi + atm
            prepack_profile = []
            for i in range(len(cum_vol)):
                v_r = v_rem[i]
                p_abs = prepack_abs * total_vol / v_r
                p_gauge = p_abs - atm
                p_gauge = min(p_gauge, self.vent_psi)
                prepack_profile.append(p_gauge)
            df_sorted['Prepack_Profile'] = prepack_profile
            max_vent = self.vent_psi
            min_cum_gal = df_sorted.loc[df_sorted['Prepack_Profile'] == max_vent, 'Cum_Gal'].min()
            self.cum_gal_at_vent = min_cum_gal if not pd.isna(min_cum_gal) else total_vol
        
        # Apply override for prepack_psi if provided (after calculation, so it overrides)
        override_prepack = params.get('override_prepack')
        if override_prepack is not None:
            self.prepack_psi = override_prepack
            prepack_abs = self.prepack_psi + atm
            prepack_profile = []
            for i in range(len(cum_vol)):
                v_r = v_rem[i]
                p_abs = prepack_abs * total_vol / v_r
                p_gauge = p_abs - atm
                p_gauge = min(p_gauge, self.vent_psi)
                prepack_profile.append(p_gauge)
            df_sorted['Prepack_Profile'] = prepack_profile
            max_vent = self.vent_psi
            min_cum_gal = df_sorted.loc[df_sorted['Prepack_Profile'] == max_vent, 'Cum_Gal'].min()
            self.cum_gal_at_vent = min_cum_gal if not pd.isna(min_cum_gal) else total_vol
        
        # Map back Req_Back_P to original
        self.points = self.points.merge(df_sorted[['Station', 'Req_Back_P']], on='Station', how='left')
        
        # Map back to original order
        self.points = self.points.merge(df_sorted[['Station', 'Prepack_Profile', 'Cum_Gal']], on='Station', how='left')
        self.points['Station_Formatted'] = self.points['Station'].apply(station_format)
        self.table_data = self.points

        # Add Percent_SMYS
        self.points['Percent_SMYS'] = (self.points['Local_P'] / (2 * app.smys * self.points['WT'] / app.od)) * 100
        # Sort table data by station so Cum_Gal is monotonically increasing
        self.table_data = self.points.sort_values('Station').reset_index(drop=True)


def rupture_analysis(stations, elevations, rup_station, od, avg_wt, specific_weight=62.4):
    """
    Calculate fluid release volumes following a pipe rupture.

    Physics: When the pipe ruptures at rup_station, fluid drains toward the rupture
    under gravity. In a closed section, atmospheric pressure at the rupture opening
    creates suction that can lift fluid up to a hydraulic head height above the rupture.
    Any point at or above (rupture_elevation + hydraulic_head) forms an "airlock" that
    blocks further drainage — the atmospheric pressure cannot push fluid over that barrier.

    Args:
        stations:        sorted array-like of station values (ft)
        elevations:      array-like of elevations corresponding to stations (ft)
        rup_station:     station of the rupture (ft)
        od:              pipe outside diameter (inches)
        avg_wt:          average wall thickness (inches)
        specific_weight: fluid specific weight (lb/ft³), default 62.4 (water)

    Returns dict with volumes (ft³, gal, bbl), drained lengths, and profile arrays.
    """
    stations = np.array(stations, dtype=float)
    elevations = np.array(elevations, dtype=float)

    # Sort ascending by station
    order = np.argsort(stations)
    stations = stations[order]
    elevations = elevations[order]

    # Snap rupture to nearest survey point
    rup_idx = int(np.argmin(np.abs(stations - rup_station)))
    rup_elev = float(elevations[rup_idx])
    actual_rup_stn = float(stations[rup_idx])

    # Atmospheric pressure at rupture elevation (standard atmosphere formula)
    elev_m = rup_elev * 0.3048
    atm_pa = 101325.0 * (1.0 - 2.25577e-5 * elev_m) ** 5.25588
    atm_psi = atm_pa / 6894.757
    # Hydraulic head: pressure head in feet of fluid (P [lb/ft²] / γ [lb/ft³])
    hydraulic_head = (atm_psi * 144.0) / specific_weight
    threshold = rup_elev + hydraulic_head

    # Pipe geometry
    id_in = od - 2.0 * avg_wt
    id_ft = id_in / 12.0
    pipe_area = math.pi * (id_ft / 2.0) ** 2  # ft²

    n = len(stations)

    # UPSTREAM MAIN: descending-envelope segments above threshold.
    # Replicates the spreadsheet "Length Upstream" logic: a segment at i counts when
    # elev[i] >= threshold AND this row sets a new running-maximum going upstream
    # (i.e., it is on the monotonically-decreasing envelope from local peaks toward the
    # rupture).  Ascending flanks within the above-threshold zone do NOT count because
    # fluid sitting on an uphill slope drains toward the local low point, not the rupture.
    # The first point (i=0, section boundary) is excluded, matching the spreadsheet's
    # up-bound row which produces N/A and prevents that segment from being counted.
    up_upslope = [0.0] * n  # elevation at nearest downstream local peak above threshold
    up_progression = [0.0] * n  # running max of up_upslope from each i to rup_idx
    for i in range(rup_idx - 1, -1, -1):
        if elevations[i] >= threshold and elevations[i] > elevations[i + 1]:
            up_upslope[i] = elevations[i]
        else:
            up_upslope[i] = up_upslope[i + 1]
        up_progression[i] = max(up_upslope[i], up_progression[i + 1])
    upstream_main = 0.0
    for i in range(1, rup_idx):  # skip i=0 (section-start boundary)
        if elevations[i] >= threshold and up_progression[i] > up_progression[i + 1]:
            upstream_main += abs(stations[i + 1] - stations[i])

    # UPSTREAM POCKET: the run between rup_elev and threshold on the first ascending
    # slope encountered going upstream from the rupture.  Find the apex of that slope
    # (first local peak above rup_elev) then count its descending segments toward the
    # rupture while rup_elev <= elev <= threshold.
    first_peak_up = None
    in_above_zone = False
    for i in range(rup_idx - 1, 0, -1):
        if elevations[i] <= rup_elev:
            if in_above_zone:
                break  # left the above-rup_elev zone — stop search
            continue
        in_above_zone = True
        if elevations[i] > elevations[i - 1]:  # apex: going further upstream elev decreases
            first_peak_up = i
            break

    upstream_pocket = 0.0
    if first_peak_up is not None:
        pk_upslope = [0.0] * n
        pk_progression = [0.0] * n
        for i in range(rup_idx - 1, first_peak_up - 1, -1):
            if elevations[i] >= rup_elev and elevations[i] > elevations[i + 1]:
                pk_upslope[i] = elevations[i]
            else:
                pk_upslope[i] = pk_upslope[i + 1]
            pk_progression[i] = max(pk_upslope[i], pk_progression[i + 1])
        for i in range(first_peak_up, rup_idx):
            if rup_elev <= elevations[i] <= threshold and pk_progression[i] > pk_progression[i + 1]:
                upstream_pocket += abs(stations[i + 1] - stations[i])

    upstream_drained = upstream_main + upstream_pocket

    # DOWNSTREAM MAIN: ascending-envelope segments above threshold going downstream.
    # Mirror of upstream main but now segments count when they are on an ascending run
    # away from the rupture that sets new elevation records, and elev > threshold.
    # Seeded at rup_elev; Elev_plot condition (from spreadsheet) requires
    # threshold <= progression < max_downstream_elev.
    max_down_elev = max(elevations[rup_idx:])
    dn_main_prog = [0.0] * n
    dn_main_prog[rup_idx] = rup_elev
    for i in range(rup_idx + 1, n):
        is_main = elevations[i] > threshold and elevations[i] > elevations[i - 1]
        dn_main_prog[i] = max(elevations[i] if is_main else 0.0, dn_main_prog[i - 1])
    downstream_main = 0.0
    for i in range(rup_idx + 1, n):
        if (threshold <= dn_main_prog[i] < max_down_elev and
                dn_main_prog[i] > dn_main_prog[i - 1]):
            downstream_main += abs(stations[i] - stations[i - 1])

    # DOWNSTREAM POCKET: ascending segments between rup_elev and threshold immediately
    # downstream of the rupture.  Seeded at rup_elev; includes the incoming (last
    # upstream) segment at the rupture row itself.
    dn_pocket_prog = [0.0] * n
    dn_pocket_prog[rup_idx] = rup_elev
    for i in range(rup_idx + 1, n):
        is_pocket = elevations[i] > rup_elev and elevations[i] > elevations[i - 1]
        dn_pocket_prog[i] = max(elevations[i] if is_pocket else 0.0, dn_pocket_prog[i - 1])
    downstream_pocket = 0.0
    for i in range(rup_idx, n):
        prev_prog = dn_pocket_prog[i - 1] if i > 0 else 0.0
        if elevations[i] <= threshold and dn_pocket_prog[i] > prev_prog:
            downstream_pocket += abs(stations[i] - stations[i - 1])

    downstream_drained = downstream_main + downstream_pocket

    # Section lengths
    total_len = float(abs(stations[-1] - stations[0]))
    up_len = float(abs(actual_rup_stn - stations[0]))
    down_len = float(abs(stations[-1] - actual_rup_stn))

    def vols(ft3):
        gal = ft3 * 7.4805
        bbl = gal / 42.0
        return round(ft3, 2), round(gal, 1), round(bbl, 2)

    tv = vols(pipe_area * total_len)
    uv = vols(pipe_area * up_len)
    dv = vols(pipe_area * down_len)
    ur = vols(pipe_area * upstream_drained)
    dr = vols(pipe_area * downstream_drained)
    tr_ft3 = ur[0] + dr[0]
    tr = vols(tr_ft3)
    pct = tr[0] / tv[0] if tv[0] > 0 else 0.0

    # Build drain-status array for charting.
    drained_flags = [False] * n
    drained_flags[rup_idx] = True

    # Upstream main
    for i in range(1, rup_idx):
        if elevations[i] >= threshold and up_progression[i] > up_progression[i + 1]:
            drained_flags[i] = True
            drained_flags[i + 1] = True

    # Upstream pocket
    if first_peak_up is not None:
        for i in range(first_peak_up, rup_idx):
            if rup_elev <= elevations[i] <= threshold and pk_progression[i] > pk_progression[i + 1]:
                drained_flags[i] = True
                drained_flags[i + 1] = True

    # Downstream main
    for i in range(rup_idx + 1, n):
        if (threshold <= dn_main_prog[i] < max_down_elev and
                dn_main_prog[i] > dn_main_prog[i - 1]):
            drained_flags[i] = True
            drained_flags[i - 1] = True

    # Downstream pocket
    for i in range(rup_idx, n):
        prev_prog = dn_pocket_prog[i - 1] if i > 0 else 0.0
        if elevations[i] <= threshold and dn_pocket_prog[i] > prev_prog:
            drained_flags[i] = True
            if i > 0:
                drained_flags[i - 1] = True

    drained_stns = [stations[i] for i, d in enumerate(drained_flags) if d]
    drained_lo = float(min(drained_stns)) if drained_stns else None
    drained_hi = float(max(drained_stns)) if drained_stns else None

    return {
        'rup_station': actual_rup_stn,
        'rup_elev': round(rup_elev, 3),
        'atm_psi': round(atm_psi, 4),
        'hydraulic_head_ft': round(hydraulic_head, 3),
        'threshold_elev': round(threshold, 3),
        'pipe_id_in': round(id_in, 3),
        'pipe_area_ft2': round(pipe_area, 4),
        'specific_weight': specific_weight,
        'upstream_drained_ft': round(upstream_drained, 2),
        'downstream_drained_ft': round(downstream_drained, 2),
        'total_drained_ft': round(upstream_drained + downstream_drained, 2),
        'total_vol_ft3': tv[0], 'total_vol_gal': tv[1], 'total_vol_bbl': tv[2],
        'upstream_vol_ft3': uv[0], 'upstream_vol_gal': uv[1], 'upstream_vol_bbl': uv[2],
        'downstream_vol_ft3': dv[0], 'downstream_vol_gal': dv[1], 'downstream_vol_bbl': dv[2],
        'upstream_released_ft3': ur[0], 'upstream_released_gal': ur[1], 'upstream_released_bbl': ur[2],
        'downstream_released_ft3': dr[0], 'downstream_released_gal': dr[1], 'downstream_released_bbl': dr[2],
        'total_released_ft3': tr[0], 'total_released_gal': tr[1], 'total_released_bbl': tr[2],
        'pct_released': round(pct * 100, 2),
        'drained_station_lo': drained_lo,
        'drained_station_hi': drained_hi,
        'stations': stations.tolist(),
        'elevations': elevations.tolist(),
        'drained_flags': drained_flags,
    }


def temp_correction_factor(od, avg_wt):
    """
    Returns K (psi/°F) — pressure change per °F of pipe temperature change for a
    water-filled, sealed steel pipeline.

    Derivation (constant-volume constraint, thin-wall approximation):
        (β_w - 2α_s) ΔT = (C_w + D/2tE) ΔP
    where:
        β_w  = 2.07e-4 /°F   volumetric thermal expansion of water at ~60°F
        α_s  = 6.5e-6  /°F   linear thermal expansion of steel
        C_w  = 3.3e-6  /psi  compressibility of water
        D/(2tE)              hoop compliance of pipe wall (restrained longitudinally)
    """
    E_steel = 30.0e6   # psi
    beta_w  = 2.07e-4  # /°F
    alpha_s = 6.5e-6   # /°F
    C_w     = 3.3e-6   # /psi
    C_pipe  = od / (2.0 * avg_wt * E_steel)
    return round((beta_w - 2.0 * alpha_s) / (C_w + C_pipe), 2)


def multi_rupture_analysis(stations, elevations, rup_stations, od, avg_wt, specific_weight=62.4):
    """
    Calculate combined fluid release from multiple simultaneous rupture points.
    Each rupture is analysed independently; the union of drained segments gives combined volumes.
    """
    stations_arr = np.array(stations, dtype=float)
    elevations_arr = np.array(elevations, dtype=float)
    order = np.argsort(stations_arr)
    stations_arr = stations_arr[order]
    elevations_arr = elevations_arr[order]
    n = len(stations_arr)

    per_rupture = []
    union_drained = [False] * n
    for rs in rup_stations:
        r = rupture_analysis(stations_arr.tolist(), elevations_arr.tolist(), rs, od, avg_wt, specific_weight)
        per_rupture.append(r)
        for i, d in enumerate(r['drained_flags']):
            if d:
                union_drained[i] = True

    pipe_area = per_rupture[0]['pipe_area_ft2']
    total_len = float(abs(stations_arr[-1] - stations_arr[0]))

    def vols(ft3):
        gal = ft3 * 7.4805
        bbl = gal / 42.0
        return round(ft3, 2), round(gal, 1), round(bbl, 2)

    tv = vols(pipe_area * total_len)
    drained_len = 0.0
    for i in range(1, n):
        if union_drained[i] or union_drained[i - 1]:
            drained_len += float(abs(stations_arr[i] - stations_arr[i - 1]))
    tr = vols(pipe_area * drained_len)
    pct = tr[0] / tv[0] if tv[0] > 0 else 0.0

    # Attribute each drained segment to the nearest claiming rupture so that
    # attributed volumes sum exactly to the combined total (no double-counting).
    attributed_ft = [0.0] * len(per_rupture)
    for i in range(1, n):
        if not (union_drained[i] or union_drained[i - 1]):
            continue
        seg_len = float(abs(stations_arr[i] - stations_arr[i - 1]))
        seg_mid = (stations_arr[i] + stations_arr[i - 1]) / 2.0
        claiming = [j for j, r in enumerate(per_rupture)
                    if r['drained_flags'][i] or r['drained_flags'][i - 1]]
        if not claiming:
            continue
        nearest = min(claiming, key=lambda j: abs(per_rupture[j]['rup_station'] - seg_mid))
        attributed_ft[nearest] += seg_len

    for j, r in enumerate(per_rupture):
        av = vols(pipe_area * attributed_ft[j])
        r['attributed_released_ft3'] = av[0]
        r['attributed_released_gal'] = av[1]
        r['attributed_released_bbl'] = av[2]

    return {
        'per_rupture': per_rupture,
        'union_drained_flags': union_drained,
        'rup_stations': [r['rup_station'] for r in per_rupture],
        'total_vol_ft3': tv[0], 'total_vol_gal': tv[1], 'total_vol_bbl': tv[2],
        'total_released_ft3': tr[0], 'total_released_gal': tr[1], 'total_released_bbl': tr[2],
        'pct_released': round(pct * 100, 2),
        'stations': stations_arr.tolist(),
        'elevations': elevations_arr.tolist(),
    }
