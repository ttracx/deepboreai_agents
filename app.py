import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import time
import threading
import json
from datetime import datetime, timedelta
import random
import os

import witsml_connector
import data_processor
from ml_agents import mechanical_sticking, differential_sticking, hole_cleaning, washout_mud_losses, rop_optimization
import orchestrator
import config_manager
import utils
from database.service import initialize_database, save_drilling_cycle
from database.repository import get_time_series_data, get_alert_summary, get_database_statistics

# Set page configuration
st.set_page_config(
    page_title="Drilling NPT/ILT Prediction System",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize database
if 'db_initialized' not in st.session_state:
    try:
        st.session_state.db_initialized = initialize_database()
        if st.session_state.db_initialized:
            st.success("Database initialized successfully")
        else:
            st.error("Failed to initialize database")
    except Exception as e:
        st.error(f"Database initialization error: {str(e)}")
        st.session_state.db_initialized = False

# Add database connection stats to session state
if 'db_stats' not in st.session_state:
    st.session_state.db_stats = {
        'total_data_points': 0,
        'total_predictions': 0,
        'total_alerts': 0,
        'last_db_write': None
    }

# Initialize session state for persistence across reruns
if 'connection_status' not in st.session_state:
    st.session_state.connection_status = False
if 'data' not in st.session_state:
    st.session_state.data = None
if 'alerts' not in st.session_state:
    st.session_state.alerts = []
if 'config' not in st.session_state:
    st.session_state.config = config_manager.get_default_config()
if 'data_thread' not in st.session_state:
    st.session_state.data_thread = None
if 'last_update' not in st.session_state:
    st.session_state.last_update = None
if 'predictions' not in st.session_state:
    st.session_state.predictions = {
        'mechanical_sticking': None,
        'differential_sticking': None,
        'hole_cleaning': None,
        'washout_mud_losses': None,
        'rop_optimization': None
    }
if 'db_initialized' not in st.session_state:
    st.session_state.db_initialized = initialize_database()
if 'db_stats' not in st.session_state:
    # Initialize database statistics
    st.session_state.db_stats = {
        'last_db_write': None,
        'total_data_points': 0,
        'total_predictions': 0,
        'total_alerts': 0
    }
    
    # If database is connected, try to load initial stats
    if st.session_state.db_initialized:
        try:
            # Get database statistics
            db_stats = get_database_statistics()
            if db_stats:
                st.session_state.db_stats.update(db_stats)
        except Exception as e:
            st.error(f"Error loading database statistics: {str(e)}")

# Function for real-time data simulation and processing
def update_data():
    while st.session_state.connection_status:
        try:
            # Fetch data from WITSML source
            new_data = witsml_connector.fetch_data(st.session_state.witsml_config)
            
            if new_data is not None:
                # Process the data
                processed_data = data_processor.process_data(new_data)
                
                # Update session state
                st.session_state.data = processed_data
                st.session_state.last_update = datetime.now()
                
                # Run ML agents for predictions
                st.session_state.predictions['mechanical_sticking'] = mechanical_sticking.predict(processed_data)
                st.session_state.predictions['differential_sticking'] = differential_sticking.predict(processed_data)
                st.session_state.predictions['hole_cleaning'] = hole_cleaning.predict(processed_data)
                st.session_state.predictions['washout_mud_losses'] = washout_mud_losses.predict(processed_data)
                st.session_state.predictions['rop_optimization'] = rop_optimization.predict(processed_data)
                
                # Orchestrate predictions and generate alerts
                new_alerts = orchestrator.evaluate_predictions(
                    st.session_state.predictions, 
                    st.session_state.config['thresholds']
                )
                
                if new_alerts:
                    st.session_state.alerts.extend(new_alerts)
                    # Keep only the last 50 alerts to prevent excessive memory usage
                    st.session_state.alerts = st.session_state.alerts[-50:]
                
                # Save data to database
                if st.session_state.db_initialized:
                    try:
                        # Save data to database
                        success = save_drilling_cycle(
                            processed_data,
                            st.session_state.predictions,
                            new_alerts
                        )
                        
                        if success:
                            # Update database stats in session state
                            st.session_state.db_stats['last_db_write'] = datetime.now()
                            st.session_state.db_stats['total_data_points'] += 1
                            if new_alerts:
                                st.session_state.db_stats['total_alerts'] += len(new_alerts)
                            st.session_state.db_stats['total_predictions'] += len(st.session_state.predictions)
                    except Exception as e:
                        st.error(f"Error saving data to database: {str(e)}")
            
            # Sleep to control update frequency
            time.sleep(st.session_state.config['update_frequency'])
        
        except Exception as e:
            st.error(f"Error updating data: {str(e)}")
            time.sleep(5)  # Wait a bit before retrying

# Header
st.title("Real-Time Drilling NPT/ILT Prediction System")

# Sidebar for configuration
with st.sidebar:
    st.subheader("Configuration")
    
    # Database Status Indicator
    st.write("### Database Status")
    if st.session_state.db_initialized:
        st.success("✅ Database Connected")
        
        # Show basic db stats if available
        if st.session_state.db_stats['last_db_write']:
            st.info(f"Last Write: {st.session_state.db_stats['last_db_write'].strftime('%Y-%m-%d %H:%M:%S')}")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Data Points", st.session_state.db_stats['total_data_points'])
            with col2:
                st.metric("Alerts", st.session_state.db_stats['total_alerts'])
    else:
        st.error("❌ Database Not Connected")
        if st.button("Retry Database Connection"):
            try:
                st.session_state.db_initialized = initialize_database()
                if st.session_state.db_initialized:
                    st.success("Database reconnected successfully")
                    st.rerun()
                else:
                    st.error("Failed to reconnect to database")
            except Exception as e:
                st.error(f"Database reconnection error: {str(e)}")
    
    # WITSML Connection Settings
    st.write("### WITSML Connection")
    witsml_url = st.text_input("WITSML Server URL", "https://witsml.example.com/store")
    witsml_username = st.text_input("Username", "user")
    witsml_password = st.text_input("Password", type="password")
    well_uid = st.text_input("Well UID", "w-12345")
    wellbore_uid = st.text_input("Wellbore UID", "wb-6789")
    
    st.session_state.witsml_config = {
        "url": witsml_url,
        "username": witsml_username,
        "password": witsml_password,
        "well_uid": well_uid,
        "wellbore_uid": wellbore_uid
    }
    
    # Connect/Disconnect button
    if not st.session_state.connection_status:
        if st.button("Connect"):
            try:
                connection_test = witsml_connector.test_connection(st.session_state.witsml_config)
                if connection_test:
                    st.session_state.connection_status = True
                    st.session_state.data_thread = threading.Thread(target=update_data)
                    st.session_state.data_thread.daemon = True
                    st.session_state.data_thread.start()
                    st.success("Connected to WITSML server")
                    st.rerun()
                else:
                    st.error("Failed to connect to WITSML server")
            except Exception as e:
                st.error(f"Connection error: {str(e)}")
    else:
        if st.button("Disconnect"):
            st.session_state.connection_status = False
            st.info("Disconnected from WITSML server")
            st.rerun()
    
    # ML Model Configuration (when connected)
    if st.session_state.connection_status:
        st.write("### ML Agent Thresholds")
        
        # Mechanical Sticking
        st.write("#### Mechanical Sticking")
        st.session_state.config['thresholds']['mechanical_sticking'] = st.slider(
            "Mechanical Sticking Threshold", 
            min_value=0.0, 
            max_value=1.0, 
            value=st.session_state.config['thresholds']['mechanical_sticking'],
            step=0.05
        )
        
        # Differential Sticking
        st.write("#### Differential Sticking")
        st.session_state.config['thresholds']['differential_sticking'] = st.slider(
            "Differential Sticking Threshold", 
            min_value=0.0, 
            max_value=1.0, 
            value=st.session_state.config['thresholds']['differential_sticking'],
            step=0.05
        )
        
        # Hole Cleaning
        st.write("#### Hole Cleaning")
        st.session_state.config['thresholds']['hole_cleaning'] = st.slider(
            "Hole Cleaning Threshold", 
            min_value=0.0, 
            max_value=1.0, 
            value=st.session_state.config['thresholds']['hole_cleaning'],
            step=0.05
        )
        
        # Washout & Mud Losses
        st.write("#### Washout & Mud Losses")
        st.session_state.config['thresholds']['washout_mud_losses'] = st.slider(
            "Washout & Mud Losses Threshold", 
            min_value=0.0, 
            max_value=1.0, 
            value=st.session_state.config['thresholds']['washout_mud_losses'],
            step=0.05
        )
        
        # Update Frequency
        st.write("### System Settings")
        st.session_state.config['update_frequency'] = st.slider(
            "Update Frequency (seconds)", 
            min_value=1, 
            max_value=60, 
            value=st.session_state.config['update_frequency']
        )
        
        if st.button("Save Configuration"):
            config_manager.save_config(st.session_state.config)
            st.success("Configuration saved")

# Main Content Area
if st.session_state.connection_status:
    # Display connection status and last update time
    col1, col2 = st.columns(2)
    with col1:
        st.success("✅ Connected to WITSML server")
    with col2:
        if st.session_state.last_update:
            st.info(f"Last data update: {st.session_state.last_update.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            st.info("Waiting for data...")
    
    # Create tabs for different sections
    tab1, tab2, tab3, tab4 = st.tabs(["Real-Time Parameters", "Predictions & Alerts", "Historical Trends", "Recommendations"])
    
    with tab1:
        st.subheader("Real-Time Drilling Parameters")
        
        if st.session_state.data is not None:
            # Display key drilling parameters in columns
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Weight on Bit (WOB)
                st.metric(
                    label="Weight on Bit (klbs)", 
                    value=f"{st.session_state.data['WOB']:.1f}",
                    delta=f"{st.session_state.data['WOB_change']:.1f}"
                )
                
                # Rate of Penetration (ROP)
                st.metric(
                    label="ROP (ft/hr)", 
                    value=f"{st.session_state.data['ROP']:.1f}",
                    delta=f"{st.session_state.data['ROP_change']:.1f}"
                )
            
            with col2:
                # Rotary Speed (RPM)
                st.metric(
                    label="RPM", 
                    value=f"{st.session_state.data['RPM']:.0f}",
                    delta=f"{st.session_state.data['RPM_change']:.0f}"
                )
                
                # Torque
                st.metric(
                    label="Torque (kft-lbs)", 
                    value=f"{st.session_state.data['Torque']:.1f}",
                    delta=f"{st.session_state.data['Torque_change']:.1f}"
                )
            
            with col3:
                # Standpipe Pressure (SPP)
                st.metric(
                    label="SPP (psi)", 
                    value=f"{st.session_state.data['SPP']:.0f}",
                    delta=f"{st.session_state.data['SPP_change']:.0f}"
                )
                
                # Flow Rate
                st.metric(
                    label="Flow Rate (gpm)", 
                    value=f"{st.session_state.data['Flow_Rate']:.0f}",
                    delta=f"{st.session_state.data['Flow_Rate_change']:.0f}"
                )
            
            # Create a figure with multiple traces for time series data
            st.subheader("Parameter Trends (Last Hour)")
            
            if 'time_series' in st.session_state.data:
                fig = go.Figure()
                
                # Add traces for each parameter
                parameters = ['WOB', 'ROP', 'RPM', 'Torque', 'SPP', 'Flow_Rate']
                colors = px.colors.qualitative.Plotly
                
                for i, param in enumerate(parameters):
                    if param in st.session_state.data['time_series']:
                        fig.add_trace(go.Scatter(
                            x=st.session_state.data['time_series']['timestamp'],
                            y=st.session_state.data['time_series'][param],
                            mode='lines',
                            name=param,
                            line=dict(color=colors[i % len(colors)])
                        ))
                
                fig.update_layout(
                    height=500,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    margin=dict(l=20, r=20, t=30, b=20),
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Collecting time series data...")
                
    with tab2:
        st.subheader("Real-Time Predictions")
        
        if st.session_state.predictions['mechanical_sticking'] is not None:
            # Display prediction gauges in columns
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Mechanical Sticking Gauge
                fig_mech = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=st.session_state.predictions['mechanical_sticking']['probability'],
                    title={'text': "Mechanical Sticking Risk"},
                    gauge={
                        'axis': {'range': [0, 1]},
                        'bar': {'color': "darkblue"},
                        'steps': [
                            {'range': [0, 0.3], 'color': "green"},
                            {'range': [0.3, 0.7], 'color': "yellow"},
                            {'range': [0.7, 1], 'color': "red"}
                        ],
                        'threshold': {
                            'line': {'color': "red", 'width': 4},
                            'thickness': 0.75,
                            'value': st.session_state.config['thresholds']['mechanical_sticking']
                        }
                    }
                ))
                fig_mech.update_layout(height=250)
                st.plotly_chart(fig_mech, use_container_width=True)
                
                # Differential Sticking Gauge
                fig_diff = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=st.session_state.predictions['differential_sticking']['probability'],
                    title={'text': "Differential Sticking Risk"},
                    gauge={
                        'axis': {'range': [0, 1]},
                        'bar': {'color': "darkblue"},
                        'steps': [
                            {'range': [0, 0.3], 'color': "green"},
                            {'range': [0.3, 0.7], 'color': "yellow"},
                            {'range': [0.7, 1], 'color': "red"}
                        ],
                        'threshold': {
                            'line': {'color': "red", 'width': 4},
                            'thickness': 0.75,
                            'value': st.session_state.config['thresholds']['differential_sticking']
                        }
                    }
                ))
                fig_diff.update_layout(height=250)
                st.plotly_chart(fig_diff, use_container_width=True)
            
            with col2:
                # Hole Cleaning Gauge
                fig_hole = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=st.session_state.predictions['hole_cleaning']['probability'],
                    title={'text': "Hole Cleaning Risk"},
                    gauge={
                        'axis': {'range': [0, 1]},
                        'bar': {'color': "darkblue"},
                        'steps': [
                            {'range': [0, 0.3], 'color': "green"},
                            {'range': [0.3, 0.7], 'color': "yellow"},
                            {'range': [0.7, 1], 'color': "red"}
                        ],
                        'threshold': {
                            'line': {'color': "red", 'width': 4},
                            'thickness': 0.75,
                            'value': st.session_state.config['thresholds']['hole_cleaning']
                        }
                    }
                ))
                fig_hole.update_layout(height=250)
                st.plotly_chart(fig_hole, use_container_width=True)
                
                # Washout & Mud Losses Gauge
                fig_wash = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=st.session_state.predictions['washout_mud_losses']['probability'],
                    title={'text': "Washout & Mud Losses Risk"},
                    gauge={
                        'axis': {'range': [0, 1]},
                        'bar': {'color': "darkblue"},
                        'steps': [
                            {'range': [0, 0.3], 'color': "green"},
                            {'range': [0.3, 0.7], 'color': "yellow"},
                            {'range': [0.7, 1], 'color': "red"}
                        ],
                        'threshold': {
                            'line': {'color': "red", 'width': 4},
                            'thickness': 0.75,
                            'value': st.session_state.config['thresholds']['washout_mud_losses']
                        }
                    }
                ))
                fig_wash.update_layout(height=250)
                st.plotly_chart(fig_wash, use_container_width=True)
            
            with col3:
                # ROP Optimization
                st.subheader("ROP Optimization")
                
                if 'recommended_parameters' in st.session_state.predictions['rop_optimization']:
                    rec_params = st.session_state.predictions['rop_optimization']['recommended_parameters']
                    
                    # Display recommended vs current parameters
                    for param, value in rec_params.items():
                        current_val = st.session_state.data.get(param, 0)
                        st.metric(
                            label=f"Recommended {param}", 
                            value=f"{value:.1f}",
                            delta=f"{value - current_val:.1f} from current"
                        )
                    
                    # Expected ROP improvement
                    if 'expected_rop_improvement' in st.session_state.predictions['rop_optimization']:
                        imp = st.session_state.predictions['rop_optimization']['expected_rop_improvement']
                        st.metric(
                            label="Expected ROP Improvement", 
                            value=f"{imp:.1f}%",
                            delta="increase"
                        )
                else:
                    st.info("ROP optimization data not available")
        
        # Alerts section
        st.subheader("Recent Alerts")
        
        if st.session_state.alerts:
            for alert in reversed(st.session_state.alerts[-10:]):
                severity_color = {
                    "HIGH": "red",
                    "MEDIUM": "orange",
                    "LOW": "blue"
                }.get(alert['severity'], "grey")
                
                st.markdown(
                    f"""
                    <div style='padding: 10px; border-left: 5px solid {severity_color}; background-color: rgba(0,0,0,0.05); margin-bottom: 10px;'>
                        <strong>{alert['timestamp']} - {alert['type']} ({alert['severity']})</strong><br/>
                        {alert['message']}<br/>
                        <em>Recommendation: {alert['recommendation']}</em>
                    </div>
                    """, 
                    unsafe_allow_html=True
                )
        else:
            st.info("No alerts generated yet")
    
    with tab3:
        st.subheader("Historical Performance")
        
        # Create columns for quick time selectors
        st.write("### Data Time Range")
        col1, col2, col3, col4 = st.columns(4)
        
        # Quick time range buttons
        with col1:
            hour_btn = st.button("Last Hour")
        with col2:
            hours4_btn = st.button("Last 4 Hours")
        with col3:
            hours12_btn = st.button("Last 12 Hours")
        with col4:
            day_btn = st.button("Last Day")
            
        # Store the selection in session state if not already present
        if 'historical_time_range' not in st.session_state:
            st.session_state.historical_time_range = "Last Hour"
            
        # Update time range based on button clicks
        if hour_btn:
            st.session_state.historical_time_range = "Last Hour"
        elif hours4_btn:
            st.session_state.historical_time_range = "Last 4 Hours"
        elif hours12_btn:
            st.session_state.historical_time_range = "Last 12 Hours"
        elif day_btn:
            st.session_state.historical_time_range = "Last 24 Hours"
            
        # Display the current selection
        st.info(f"Current selection: {st.session_state.historical_time_range}")
        
        # Use the stored time range
        time_range = st.session_state.historical_time_range
        
        # Parameter selector
        selected_params = st.multiselect(
            "Select Parameters to Display",
            ['WOB', 'ROP', 'RPM', 'Torque', 'SPP', 'Flow_Rate'],
            default=['WOB', 'ROP']
        )
        
        if selected_params and st.session_state.db_initialized:
            # Convert time range to hours
            hours_mapping = {
                "Last Hour": 1,
                "Last 4 Hours": 4,
                "Last 12 Hours": 12,
                "Last 24 Hours": 24
            }
            hours = hours_mapping.get(time_range, 24)
            
            # Fetch time series data from database
            try:
                historical_data = get_time_series_data(selected_params, hours=hours)
                
                if historical_data and 'timestamps' in historical_data and len(historical_data['timestamps']) > 0:
                    # Create historical trend chart
                    fig_hist = go.Figure()
                    
                    colors = px.colors.qualitative.Plotly
                    
                    for i, param in enumerate(selected_params):
                        if param in historical_data:
                            fig_hist.add_trace(go.Scatter(
                                x=historical_data['timestamps'],
                                y=historical_data[param],
                                mode='lines',
                                name=param,
                                line=dict(color=colors[i % len(colors)])
                            ))
                    
                    fig_hist.update_layout(
                        title=f"Historical Parameter Trends ({time_range})",
                        height=500,
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        margin=dict(l=20, r=20, t=30, b=20),
                    )
                    
                    st.plotly_chart(fig_hist, use_container_width=True)
                    
                    # Add some statistics for the selected parameters
                    st.subheader("Parameter Statistics")
                    stat_cols = st.columns(len(selected_params))
                    
                    for i, param in enumerate(selected_params):
                        if param in historical_data:
                            data = historical_data[param]
                            with stat_cols[i]:
                                st.metric(f"{param} Avg", f"{sum(data)/len(data):.2f}")
                                st.metric(f"{param} Max", f"{max(data):.2f}")
                                st.metric(f"{param} Min", f"{min(data):.2f}")
                                # Calculate trend (up or down)
                                if len(data) > 1:
                                    trend = data[-1] - data[0]
                                    st.metric(f"{param} Trend", 
                                              f"{trend:.2f}", 
                                              delta=f"{trend:.2f}")
                else:
                    st.info("No historical data available for the selected time range")
            except Exception as e:
                st.error(f"Error fetching historical data: {str(e)}")
            
            # Display alert history on timeline
            st.subheader("Alert History")
            
            # Create radio button to choose alert source
            alert_source = st.radio("Select Alert Source", ["Current Session", "Database (All History)"], horizontal=True)
            
            if alert_source == "Current Session" and st.session_state.alerts:
                # Use alerts from current session
                alert_times = [datetime.strptime(alert['timestamp'], "%Y-%m-%d %H:%M:%S") for alert in st.session_state.alerts]
                alert_types = [alert['type'] for alert in st.session_state.alerts]
                
                fig_timeline = go.Figure()
                
                for i, alert_type in enumerate(set(alert_types)):
                    # Filter alerts by type
                    type_indices = [i for i, t in enumerate(alert_types) if t == alert_type]
                    type_times = [alert_times[i] for i in type_indices]
                    
                    # Add scatter points for each alert type
                    fig_timeline.add_trace(go.Scatter(
                        x=type_times,
                        y=[alert_type] * len(type_times),
                        mode='markers',
                        name=alert_type,
                        marker=dict(size=12)
                    ))
                
                fig_timeline.update_layout(
                    title="Session Alert Timeline",
                    xaxis_title="Time",
                    yaxis_title="Alert Type",
                    height=300,
                    margin=dict(l=20, r=20, t=30, b=20),
                )
                
                st.plotly_chart(fig_timeline, use_container_width=True)
                
                # Show alert count by type
                st.subheader("Alert Counts")
                alert_counts = {}
                for alert in st.session_state.alerts:
                    alert_type = alert['type']
                    if alert_type not in alert_counts:
                        alert_counts[alert_type] = 0
                    alert_counts[alert_type] += 1
                
                # Display counts in columns
                cols = st.columns(len(alert_counts))
                for i, (alert_type, count) in enumerate(alert_counts.items()):
                    with cols[i]:
                        st.metric(f"{alert_type}", count)
                
            elif alert_source == "Database (All History)" and st.session_state.db_initialized:
                try:
                    # Try to get alerts from database for the same time period
                    db_alerts = get_alert_summary(hours=hours, include_acknowledged=True)
                    
                    if db_alerts and db_alerts.get('alerts', []):
                        alerts_data = db_alerts['alerts']
                        
                        # Create timeline of database alerts
                        db_alert_times = [alert['timestamp'] for alert in alerts_data]
                        db_alert_types = [alert['type'] for alert in alerts_data]
                        
                        fig_db_timeline = go.Figure()
                        
                        for i, alert_type in enumerate(set(db_alert_types)):
                            # Filter alerts by type
                            type_indices = [i for i, t in enumerate(db_alert_types) if t == alert_type]
                            type_times = [db_alert_times[i] for i in type_indices]
                            
                            # Add scatter points for each alert type
                            fig_db_timeline.add_trace(go.Scatter(
                                x=type_times,
                                y=[alert_type] * len(type_times),
                                mode='markers',
                                name=alert_type,
                                marker=dict(size=12)
                            ))
                        
                        fig_db_timeline.update_layout(
                            title=f"Database Alert Timeline ({time_range})",
                            xaxis_title="Time",
                            yaxis_title="Alert Type",
                            height=300,
                            margin=dict(l=20, r=20, t=30, b=20),
                        )
                        
                        st.plotly_chart(fig_db_timeline, use_container_width=True)
                        
                        # Show alert counts by severity
                        if 'counts_by_severity' in db_alerts:
                            st.subheader("Alerts by Severity")
                            severity_cols = st.columns(len(db_alerts['counts_by_severity']))
                            
                            for i, (severity, count) in enumerate(db_alerts['counts_by_severity'].items()):
                                with severity_cols[i]:
                                    st.metric(f"{severity}", count)
                    else:
                        st.info("No alerts found in database for the selected time period")
                        
                except Exception as e:
                    st.error(f"Error retrieving alerts from database: {str(e)}")
            else:
                st.info("No alerts available")
        else:
            st.info("Historical data not available yet")
    
    with tab4:
        st.subheader("Drilling Recommendations")
        
        # Get combined recommendations from all agents
        recommendations = orchestrator.get_recommendations(st.session_state.predictions)
        
        if recommendations:
            for i, rec in enumerate(recommendations):
                with st.expander(f"{rec['category']} - {rec['title']}", expanded=i==0):
                    st.markdown(f"**Impact Level**: {rec['impact_level']}")
                    st.markdown(f"**Description**: {rec['description']}")
                    
                    if 'action_items' in rec:
                        st.markdown("**Recommended Actions:**")
                        for item in rec['action_items']:
                            st.markdown(f"- {item}")
                    
                    if 'expected_benefits' in rec:
                        st.markdown("**Expected Benefits:**")
                        for benefit in rec['expected_benefits']:
                            st.markdown(f"- {benefit}")
        else:
            st.info("No recommendations available")

else:
    # Display welcome screen when not connected
    st.header("Welcome to the Real-Time Drilling NPT/ILT Prediction System")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ### System Overview
        
        This application provides real-time predictions and alerts for common drilling challenges:
        
        - **Mechanical Sticking**
        - **Differential Sticking**
        - **Hole Cleaning Issues**
        - **Washouts & Mud Losses**
        - **ROP Optimization**
        
        ### Getting Started
        
        1. Enter your WITSML connection details in the sidebar
        2. Click "Connect" to start receiving real-time data
        3. Monitor predictions and alerts on the dashboard
        4. Adjust thresholds in the configuration panel as needed
        
        ### Key Features
        
        - Vendor-agnostic WITSML integration
        - Real-time data visualization
        - Physics-informed ML prediction models
        - Actionable recommendations
        - Customizable alert thresholds
        """)
    
    with col2:
        st.markdown("""
        ### Benefits
        
        - **Reduced Downtime:** Proactive detection of drilling issues
        - **Enhanced Performance:** Optimize drilling parameters
        - **Ease-of-Use:** Intuitive interface for drilling engineers
        - **Vendor Independence:** Works with any WITSML source
        
        ### System Requirements
        
        - WITSML 1.4.1 or higher compliant data source
        - Internet connection
        - Modern web browser
        """)
    
    st.info("Please configure your WITSML connection in the sidebar to get started.")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center;'>
        <p style='color: #888;'>Real-Time Drilling NPT/ILT Prediction System | Version 1.0 | Data refresh rate: 
        {} seconds</p>
    </div>
    """.format(st.session_state.config['update_frequency']), 
    unsafe_allow_html=True
)
