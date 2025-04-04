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
        # Convert string to datetime if needed
        if isinstance(timestamp, str):
            timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        
        # Format with or without seconds
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
    window_mapping = {
        "Last Hour": timedelta(hours=1),
        "Last 4 Hours": timedelta(hours=4),
        "Last 12 Hours": timedelta(hours=12),
        "Last 24 Hours": timedelta(hours=24),
        "Last Day": timedelta(days=1),
        "Last Week": timedelta(days=7)
    }
    
    return window_mapping.get(window_name, timedelta(hours=24))

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
    try:
        cutoff_time = datetime.now() - window
        return data[data[time_column] >= cutoff_time]
    
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
    required_fields = ['WOB', 'ROP', 'RPM', 'Torque', 'SPP', 'Flow_Rate']
    
    for field in required_fields:
        if field not in data:
            logger.warning(f"Missing required field {field} in drilling data")
            return False
    
    return True

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
    severity_colors = {
        "HIGH": "red",
        "MEDIUM": "orange",
        "LOW": "blue"
    }
    
    return severity_colors.get(severity, "grey")

def format_recommendation(recommendation):
    """
    Format a recommendation for display.
    
    Args:
        recommendation (dict): Recommendation data
        
    Returns:
        str: Formatted HTML for the recommendation
    """
    try:
        # Extract recommendation data
        rec_text = recommendation['recommendation']
        source = recommendation['source']
        priority = recommendation['priority']
        probability = recommendation.get('probability', 0)
        
        # Define color based on priority
        color = "#ff4444" if priority == 'high' else "#ff9900" if priority == 'medium' else "#3399ff"
        
        # Create formatted HTML
        html = f"""
        <div style="padding: 10px; border-left: 5px solid {color}; background-color: rgba(0,0,0,0.05); margin-bottom: 10px;">
            <strong>{source}</strong> ({probability:.1%} probability)<br/>
            {rec_text}
        </div>
        """
        
        return html
    
    except Exception as e:
        logger.error(f"Error formatting recommendation: {str(e)}")
        return str(recommendation)

def save_session_data(session_state, filename="session_data.json"):
    """
    Save session data for persistence between restarts.
    
    Args:
        session_state (dict): Session state to save
        filename (str): Filename to save to
        
    Returns:
        bool: True if successful, False otherwise
    """
    import json
    
    try:
        # Convert session state to serializable format
        data = {}
        
        # Extract key values that need to be saved
        keys_to_save = ['config', 'alerts']
        
        for key in keys_to_save:
            if key in session_state:
                data[key] = session_state[key]
        
        # Save to file
        with open(filename, 'w') as f:
            json.dump(data, f)
        
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
    import json
    import os
    
    try:
        # Check if file exists
        if not os.path.exists(filename):
            logger.warning(f"Session data file {filename} does not exist")
            return None
        
        # Load from file
        with open(filename, 'r') as f:
            data = json.load(f)
        
        logger.info(f"Session data loaded from {filename}")
        return data
    
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
    try:
        if not data_series or len(data_series) == 0:
            return {
                'min': 0,
                'max': 0,
                'avg': 0,
                'std': 0,
                'count': 0
            }
        
        # Calculate statistics
        min_val = min(data_series)
        max_val = max(data_series)
        avg = sum(data_series) / len(data_series)
        
        # Standard deviation
        variance = sum((x - avg) ** 2 for x in data_series) / len(data_series)
        std = variance ** 0.5
        
        return {
            'min': min_val,
            'max': max_val,
            'avg': avg,
            'std': std,
            'count': len(data_series)
        }
    
    except Exception as e:
        logger.error(f"Error calculating statistics: {str(e)}")
        return {
            'min': 0,
            'max': 0,
            'avg': 0,
            'std': 0,
            'count': 0
        }

def display_connection_status(status):
    """
    Display connection status with appropriate styling.
    
    Args:
        status (bool): Connection status
    """
    try:
        import streamlit as st
        
        if status:
            st.success("✅ Connected to WITSML server")
        else:
            st.error("❌ Not connected to WITSML server")
    
    except Exception as e:
        logger.error(f"Error displaying connection status: {str(e)}")

def is_valid_witsml_config(config):
    """
    Check if WITSML configuration is valid.
    
    Args:
        config (dict): WITSML configuration
        
    Returns:
        bool: True if configuration is valid, False otherwise
    """
    required_fields = ['url', 'username', 'password', 'well_uid', 'wellbore_uid']
    
    for field in required_fields:
        if field not in config or not config[field]:
            logger.warning(f"Missing or empty WITSML configuration field: {field}")
            return False
    
    return True

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
        # Format as float with specified precision
        return f"{value:.{precision}f}"
    
    except Exception as e:
        logger.error(f"Error formatting parameter value: {str(e)}")
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
    try:
        # Calculate percent change
        if previous == 0:
            return "→"
        
        percent_change = (current - previous) / abs(previous) * 100
        
        # Determine indicator based on change
        if percent_change > 5:
            return "↑"  # Up arrow
        elif percent_change < -5:
            return "↓"  # Down arrow
        else:
            return "→"  # Right arrow (no significant change)
    
    except Exception as e:
        logger.error(f"Error calculating trend indicator: {str(e)}")
        return "→"