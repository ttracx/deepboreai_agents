"""
Utility functions for the drilling prediction application.

This module provides various helper functions used throughout the application:
- Date and time formatting
- Data validation
- Alert management
- Configuration utilities
- UI helper functions
"""

import logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import streamlit as st
import json
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def format_timestamp(timestamp, include_seconds=True):
    """
    Format timestamp in a consistent way.
    
    Args:
        timestamp (datetime or str): Timestamp to format
        include_seconds (bool): Whether to include seconds in the formatted string
        
    Returns:
        str: Formatted timestamp
    """
    try:
        if isinstance(timestamp, str):
            timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        
        if include_seconds:
            return timestamp.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return timestamp.strftime("%Y-%m-%d %H:%M")
    except Exception as e:
        logger.error(f"Error formatting timestamp: {str(e)}")
        return str(timestamp)

def get_time_window(window_name):
    """
    Convert a time window name to a timedelta.
    
    Args:
        window_name (str): Name of the time window (e.g., "Last Hour")
        
    Returns:
        timedelta: Time window as timedelta
    """
    windows = {
        "Last Hour": timedelta(hours=1),
        "Last 4 Hours": timedelta(hours=4),
        "Last 12 Hours": timedelta(hours=12),
        "Last 24 Hours": timedelta(hours=24),
        "Last 7 Days": timedelta(days=7)
    }
    
    return windows.get(window_name, timedelta(hours=1))

def filter_data_by_time(data, time_column, window):
    """
    Filter a dataframe to include only data within a specific time window.
    
    Args:
        data (DataFrame): Data to filter
        time_column (str): Name of the time column
        window (timedelta): Time window to filter by
        
    Returns:
        DataFrame: Filtered data
    """
    if data is None or len(data) == 0:
        return data
    
    try:
        end_time = datetime.now()
        start_time = end_time - window
        
        if isinstance(data[time_column].iloc[0], str):
            data[time_column] = pd.to_datetime(data[time_column])
        
        return data[(data[time_column] >= start_time) & (data[time_column] <= end_time)]
    except Exception as e:
        logger.error(f"Error filtering data by time: {str(e)}")
        return data

def validate_drilling_data(data):
    """
    Validate that drilling data contains the required fields.
    
    Args:
        data (dict): Drilling data to validate
        
    Returns:
        bool: True if data is valid, False otherwise
    """
    if data is None:
        return False
    
    required_fields = ['WOB', 'ROP', 'RPM', 'Torque', 'SPP', 'Flow_Rate']
    return all(field in data for field in required_fields)

def clean_alert_history(alerts, max_alerts=50):
    """
    Clean alert history to prevent excessive memory use.
    
    Args:
        alerts (list): List of alerts
        max_alerts (int): Maximum number of alerts to keep
        
    Returns:
        list: Cleaned alert history
    """
    if len(alerts) <= max_alerts:
        return alerts
    
    # Keep only the most recent alerts
    return alerts[-max_alerts:]

def get_severity_color(severity):
    """
    Get color for alert severity.
    
    Args:
        severity (str): Alert severity (HIGH, MEDIUM, LOW)
        
    Returns:
        str: Color code
    """
    colors = {
        "HIGH": "red",
        "MEDIUM": "orange",
        "LOW": "blue"
    }
    return colors.get(severity, "gray")

def format_recommendation(recommendation):
    """
    Format a recommendation for display.
    
    Args:
        recommendation (dict): Recommendation data
        
    Returns:
        str: Formatted HTML for the recommendation
    """
    if not recommendation:
        return ""
    
    impact_colors = {
        "High": "red",
        "Medium": "orange",
        "Low": "blue"
    }
    
    color = impact_colors.get(recommendation.get('impact_level', 'Medium'), "gray")
    
    html = f"""
    <div style='padding: 10px; border-left: 5px solid {color}; background-color: rgba(0,0,0,0.05); margin-bottom: 10px;'>
        <strong>{recommendation.get('title', 'Recommendation')}</strong><br/>
        <p>{recommendation.get('description', '')}</p>
    """
    
    if 'action_items' in recommendation and recommendation['action_items']:
        html += "<strong>Actions:</strong><ul>"
        for item in recommendation['action_items']:
            html += f"<li>{item}</li>"
        html += "</ul>"
    
    if 'expected_benefits' in recommendation and recommendation['expected_benefits']:
        html += "<strong>Expected Benefits:</strong><ul>"
        for benefit in recommendation['expected_benefits']:
            html += f"<li>{benefit}</li>"
        html += "</ul>"
    
    html += "</div>"
    return html

def save_session_data(session_state, filename="session_data.json"):
    """
    Save session data for persistence between restarts.
    
    Args:
        session_state (dict): Session state to save
        filename (str): Filename to save to
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Extract only serializable data
        serializable_data = {
            'alerts': session_state.alerts,
            'config': session_state.config,
            'connection_status': session_state.connection_status,
            'witsml_config': session_state.witsml_config
        }
        
        with open(filename, 'w') as f:
            json.dump(serializable_data, f)
        
        logger.info(f"Session data saved to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving session data: {str(e)}")
        return False

def load_session_data(filename="session_data.json"):
    """
    Load saved session data.
    
    Args:
        filename (str): Filename to load from
        
    Returns:
        dict: Loaded session data or None if error
    """
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                data = json.load(f)
            
            logger.info(f"Session data loaded from {filename}")
            return data
        else:
            logger.info(f"No session data file found at {filename}")
            return None
    except Exception as e:
        logger.error(f"Error loading session data: {str(e)}")
        return None

def calculate_statistics(data_series):
    """
    Calculate basic statistics for a data series.
    
    Args:
        data_series (list or numpy array): Data series
        
    Returns:
        dict: Calculated statistics
    """
    if data_series is None or len(data_series) == 0:
        return {
            'mean': 0,
            'median': 0,
            'min': 0,
            'max': 0,
            'std': 0
        }
    
    try:
        data_array = np.array(data_series)
        return {
            'mean': np.mean(data_array),
            'median': np.median(data_array),
            'min': np.min(data_array),
            'max': np.max(data_array),
            'std': np.std(data_array)
        }
    except Exception as e:
        logger.error(f"Error calculating statistics: {str(e)}")
        return {
            'mean': 0,
            'median': 0,
            'min': 0,
            'max': 0,
            'std': 0
        }

def display_connection_status(status):
    """
    Display connection status with appropriate styling.
    
    Args:
        status (bool): Connection status
    """
    if status:
        st.success("✅ Connected to WITSML server")
    else:
        st.warning("❌ Not connected to WITSML server")

def is_valid_witsml_config(config):
    """
    Check if WITSML configuration is valid.
    
    Args:
        config (dict): WITSML configuration
        
    Returns:
        bool: True if configuration is valid, False otherwise
    """
    required_fields = ['url', 'username', 'password', 'well_uid', 'wellbore_uid']
    if not all(field in config for field in required_fields):
        return False
    
    # Check that none of the fields are empty
    return all(config[field] for field in required_fields)

def format_parameter_value(value, precision=1):
    """
    Format a parameter value with appropriate precision.
    
    Args:
        value (float or int): Value to format
        precision (int): Number of decimal places
        
    Returns:
        str: Formatted value
    """
    try:
        if isinstance(value, int):
            return f"{value:,d}"
        else:
            return f"{value:,.{precision}f}"
    except:
        return str(value)

def get_trend_indicator(current, previous):
    """
    Get a trend indicator arrow based on the change between values.
    
    Args:
        current (float): Current value
        previous (float): Previous value
        
    Returns:
        str: Trend indicator (↑, ↓, or →)
    """
    if current is None or previous is None:
        return "→"
    
    try:
        diff = current - previous
        if abs(diff) < 0.001:
            return "→"
        elif diff > 0:
            return "↑"
        else:
            return "↓"
    except:
        return "→"
