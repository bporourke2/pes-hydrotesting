import pandas as pd
import numpy as np
import math
import io
import base64
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import plotly.graph_objects as go  # Added for interactive plots

def parse_station(st):
    st = str(st).replace(',', '')  # Handle commas
    if '+' in st:
        parts = st.split('+')
        return float(parts[0]) * 100 + float(parts[1])
    else:
        return float(st)

def station_format(x):
    x = float(x)
    sta = int(x // 100)
    rem = int(x % 100)
    return f"{sta}+{rem:02d}"

class PipelineApp:
    def __init__(self, survey_file, od=42, smys=70000, test_percent=1.04, head_factor=0.433):
        self.od = od
        self.smys = smys
        self.test_percent = test_percent
        self.head_factor = head_factor
        self.survey_file = survey_file
        # Load from the first sheet
        self.full_df = pd.read_excel(survey_file, sheet_name=0)

    def get_preview(self):
        return self.full_df.head(10).to_html(classes='preview-table'), self.full_df.columns.tolist()

    def generate_plot(self, df, min_test=None, params=None, gauge_lower=None, gauge_upper=None, prepack_time=None, sec=None, vent_time=None, static=False):
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
            fig.add_trace(go.Scatter(x=df['Station'], y=df['SMYS_Limit'], name=f'SMYS Threshold ({int(self.test_percent*100)}%)', line=dict(color='#d62728', dash='dash'), yaxis='y2', customdata=df['Station_Formatted'], hovertemplate='Station: %{customdata}<br>SMYS Limit: %{y:.0f} psig<extra></extra>'))

            # Red shading for test window exceeding SMYS (from Upper_Bound_P down to SMYS_Limit)
            exceed_mask = df['Upper_Bound_P'] > df['SMYS_Limit']
            fig.add_trace(go.Scatter(x=df['Station'][exceed_mask], y=df['Upper_Bound_P'][exceed_mask], fill=None, mode='lines', line=dict(color='red', width=0), showlegend=False, yaxis='y2'))
            fig.add_trace(go.Scatter(x=df['Station'][exceed_mask], y=df['SMYS_Limit'][exceed_mask], fill='tonexty', mode='lines', fillcolor='rgba(255,0,0,0.3)', line=dict(color='red', width=0), name='Test Window Exceeds SMYS', yaxis='y2'))

            # Fill below min test (red shaded)
            below_mask = df['Local_P'] < min_test
            fig.add_trace(go.Scatter(x=df['Station'][below_mask], y=df['Local_P'][below_mask], fill=None, mode='lines', line=dict(color='red', width=0), showlegend=False, yaxis='y2'))
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

            tickvals = list(range(int(df['Station'].min()), int(df['Station'].max()) + 1, 1000))
            ticktext = [f"{int(val // 100)}+{int(val % 100):02d}" for val in tickvals]
            fig.update_xaxes(tickvals=tickvals, ticktext=ticktext)

            fill_site = parse_station(params.get('fill_site'))
            test_site = parse_station(params.get('test_site'))
            if not df.empty:
                closest_fill = df.iloc[(df['Station'] - fill_site).abs().argsort()[:1]]
                closest_test = df.iloc[(df['Station'] - test_site).abs().argsort()[:1]]
                if not closest_fill.empty:
                    fill_sta = closest_fill['Station'].values[0]
                    fill_elev = closest_fill['Elevation'].values[0]
                    fig.add_vline(x=fill_sta, line=dict(color='green', dash='dash'), annotation_text=f'Fill Site (Elev {fill_elev:.1f} ft)', annotation_position='top left')
                if not closest_test.empty:
                    test_sta = closest_test['Station'].values[0]
                    test_elev = closest_test['Elevation'].values[0]
                    fig.add_vline(x=test_sta, line=dict(color='blue', dash='dash'), annotation_text=f'Test Site (Elev {test_elev:.1f} ft)', annotation_position='top left')

            plot1 = fig.to_html(full_html=False, include_plotlyjs=True)

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

            if not df.empty:
                if not closest_fill.empty:
                    fig2.add_vline(x=fill_sta, line=dict(color='green', dash='dash'), annotation_text=f'Fill Site (Elev {fill_elev:.1f} ft)', annotation_position='top left')
                if not closest_test.empty:
                    fig2.add_vline(x=test_sta, line=dict(color='blue', dash='dash'), annotation_text=f'Test Site (Elev {test_elev:.1f} ft)', annotation_position='top left')

            plot2 = fig2.to_html(full_html=False, include_plotlyjs=True)

            return plot1, plot2

        else:
            # Static plots using matplotlib for print
            fig1, ax1 = plt.subplots(figsize=(10, 6))
            ax2 = ax1.twinx()

            ax1.plot(df['Station'], df['Elevation'], label='Elevation (ft)', color='#101820')
            ax2.plot(df['Station'], df['Local_P'], label='Target Test Pressure (psig)', color='green')
            ax2.plot(df['Station'], df['SMYS_Limit'], label=f'SMYS Threshold ({int(self.test_percent*100)}%)', color='#d62728', linestyle='--')

            if 'Lower_Bound_P' in df.columns:
                ax2.plot(df['Station'], df['Lower_Bound_P'], label='Test Window Minimum', color='orange', linestyle='--')
            if 'Upper_Bound_P' in df.columns:
                ax2.plot(df['Station'], df['Upper_Bound_P'], label='Test Window Maximum', color='orange', linestyle='--')

            # Red shading for test window exceeding SMYS in matplotlib
            exceed_mask = df['Upper_Bound_P'] > df['SMYS_Limit']
            if exceed_mask.any():
                ax2.fill_between(df['Station'][exceed_mask], df['SMYS_Limit'][exceed_mask], df['Upper_Bound_P'][exceed_mask], color='red', alpha=0.3, label='Test Window Exceeds SMYS')

            if min_test is not None:
                ax2.axhline(y=min_test, color='red', linestyle='--', label='Min Test Pressure')

            ax1.set_xlabel('Station')
            ax1.set_ylabel('Elevation (ft)')
            ax2.set_ylabel('Pressure (psig)')
            ax1.set_title('Pressure Profile')

            lines, labels = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax2.legend(lines + lines2, labels + labels2, loc='upper left')

            buf1 = io.BytesIO()
            fig1.savefig(buf1, format='png', bbox_inches='tight')
            buf1.seek(0)
            plot1_static = base64.b64encode(buf1.getvalue()).decode('utf-8')
            plt.close(fig1)

            # Second static plot
            fig2, ax1 = plt.subplots(figsize=(10, 6))
            ax2 = ax1.twinx()

            ax1.plot(df['Station'], df['Elevation'], label='Elevation (ft)', color='#101820')
            ax2.plot(df['Station'], df['Prepack_Profile'], label='Prepack Profile (psig)', color='blue')
            ax2.plot(df['Station'], df['Req_Back_P'], label='Required Backpressure (psig)', color='purple', linestyle='--')

            ax1.set_xlabel('Station')
            ax1.set_ylabel('Elevation (ft)')
            ax2.set_ylabel('Pressure (psig)')
            ax1.set_title('Filling Profile')

            lines, labels = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax2.legend(lines + lines2, labels + labels2, loc='upper left')

            buf2 = io.BytesIO()
            fig2.savefig(buf2, format='png', bbox_inches='tight')
            buf2.seek(0)
            plot2_static = base64.b64encode(buf2.getvalue()).decode('utf-8')
            plt.close(fig2)

            return plot1_static, plot2_static

class Section:
    def __init__(self, app, params, col_map):
        if not all(k in params for k in ['start', 'end', 'min_p', 'test_site']):
            raise ValueError("Missing required parameters: start, end, min_p, or test_site")
        
        start = parse_station(params['start'])
        end = parse_station(params['end'])
        data = app.full_df[(app.full_df[col_map['station']] >= min(start, end)) & 
                           (app.full_df[col_map['station']] <= max(start, end))].copy()
        
        if data.empty:
            raise ValueError("No data found for the specified start/end stations. Check mappings or range.")
        
        data['Station'] = pd.to_numeric(data[col_map['station']], errors='coerce')
        data['Elevation'] = pd.to_numeric(data[col_map['elev']], errors='coerce')
        data['WT'] = pd.to_numeric(data[col_map['wt']], errors='coerce')
        data = data.dropna(subset=['Station', 'Elevation', 'WT'])
        self.points = data
        self.length = abs(end - start)
        
        # SMYS Threshold input 
        app.test_percent = float(params.get('smys_threshold', 104)) / 100  # Handle as %
        min_test_req = float(params['min_p'])
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
        
        # Adjust for target
        self.target_gauge = target_gauge_at_test_site - max_head_loss
        self.gauge_lower = self.target_gauge - (window_upper / 2)
        self.gauge_upper = self.target_gauge + (window_upper / 2)
        
        self.points['Local_P'] = self.target_gauge + (test_elev - self.points['Elevation']) * app.head_factor
        self.points['Lower_Bound_P'] = self.gauge_lower + (test_elev - self.points['Elevation']) * app.head_factor
        self.points['Upper_Bound_P'] = self.gauge_upper + (test_elev - self.points['Elevation']) * app.head_factor
        self.points['SMYS_Limit'] = app.test_percent * (2 * app.smys * self.points['WT'] / app.od)
        
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
        
        if total_vol == 0:
            self.prepack_psi = 0
            prepack_profile = [0.0] * len(df_sorted)
            self.cum_gal_at_vent = 0.0
            df_sorted['Prepack_Profile'] = prepack_profile
        else:
            atm = 14.7
            req_abs = req_back_p + atm
            v_rem = total_vol - np.array(cum_vol)
            v_rem[v_rem == 0] = 1e-6
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
        self.table_data = self.points

