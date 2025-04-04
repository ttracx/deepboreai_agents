"""
Database service module for the drilling prediction application.

This module provides high-level functions for interacting with the database,
including storing data points, retrieving data for UI, and managing alerts.
"""

import logging
from datetime import datetime, timedelta
from sqlalchemy import and_
from database.connection import init_db, get_session
from database.models import Prediction
from database.repository import (
    store_drilling_data, store_prediction, store_alert,
    get_recent_drilling_data, get_drilling_data_in_timeframe,
    get_recent_predictions, get_recent_alerts,
    acknowledge_alert, get_parameter_statistics, get_risk_trend
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def initialize_database():
    """
    Initialize the database.
    
    Returns:
        bool: True if successful, False otherwise
    """
    return init_db()


def save_drilling_cycle(drilling_data, predictions, alerts):
    """
    Save a complete drilling cycle (data, predictions, alerts) to the database.
    
    Args:
        drilling_data (dict): Raw and processed drilling data
        predictions (dict): Dictionary of predictions from different ML agents
        alerts (list): List of generated alerts
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Store drilling data
        drilling_data_obj = store_drilling_data(drilling_data)
        if not drilling_data_obj:
            logger.error("Failed to store drilling data")
            return False
        
        drilling_data_id = drilling_data_obj.id
        
        # Store predictions
        for agent_type, prediction in predictions.items():
            if prediction:
                prediction_obj = store_prediction(prediction, agent_type, drilling_data_id)
                
                if prediction_obj:
                    prediction_id = prediction_obj.id
                    
                    # If this prediction generated alerts, store them
                    for alert in alerts:
                        # Only store alerts related to this prediction
                        if alert.get('type') == f"{agent_type.replace('_', ' ').title()} Risk" or \
                           (agent_type == 'washout_mud_losses' and 
                            (alert.get('type') == 'Washout Risk' or alert.get('type') == 'Mud Losses Risk')):
                            store_alert(alert, prediction_id)
        
        logger.info("Successfully saved complete drilling cycle to database")
        return True
    
    except Exception as e:
        logger.error(f"Error saving drilling cycle: {str(e)}")
        return False


def get_time_series_data(parameter_names, hours=24):
    """
    Get time series data for specified parameters.
    
    Args:
        parameter_names (list): List of parameter names to retrieve
        hours (int): Number of hours to look back
        
    Returns:
        dict: Dictionary with timestamps and parameter values
    """
    try:
        # Get drilling data within timeframe
        drilling_data = get_drilling_data_in_timeframe(hours)
        
        # Prepare result dictionary
        result = {
            'timestamp': [],
        }
        
        # Add entry for each parameter
        for param in parameter_names:
            result[param] = []
        
        # Fill with data
        for data_point in drilling_data:
            data_dict = data_point.to_dict()
            result['timestamp'].append(data_point.timestamp)
            
            for param in parameter_names:
                result[param].append(data_dict.get(param, 0))
        
        return result
    
    except Exception as e:
        logger.error(f"Error getting time series data: {str(e)}")
        return {'timestamp': []}


def get_alert_summary(include_acknowledged=False):
    """
    Get a summary of recent alerts.
    
    Args:
        include_acknowledged (bool): Whether to include acknowledged alerts
        
    Returns:
        dict: Dictionary with alert counts by severity and type
    """
    try:
        # Get recent alerts
        alerts = get_recent_alerts(limit=1000, include_acknowledged=include_acknowledged)
        
        # Count by severity
        severity_counts = {
            'HIGH': 0,
            'MEDIUM': 0,
            'LOW': 0
        }
        
        # Count by type
        type_counts = {}
        
        for alert in alerts:
            # Count by severity
            severity = alert.severity
            if severity in severity_counts:
                severity_counts[severity] += 1
            
            # Count by type
            alert_type = alert.alert_type
            if alert_type in type_counts:
                type_counts[alert_type] += 1
            else:
                type_counts[alert_type] = 1
        
        return {
            'severity_counts': severity_counts,
            'type_counts': type_counts,
            'total_count': len(alerts)
        }
    
    except Exception as e:
        logger.error(f"Error getting alert summary: {str(e)}")
        return {
            'severity_counts': {'HIGH': 0, 'MEDIUM': 0, 'LOW': 0},
            'type_counts': {},
            'total_count': 0
        }


def get_agent_predictions_summary(agent_type, hours=24):
    """
    Get a summary of predictions for a specific agent.
    
    Args:
        agent_type (str): Type of ML agent
        hours (int): Number of hours to look back
        
    Returns:
        dict: Dictionary with prediction statistics
    """
    try:
        # Get all predictions for this agent in the timeframe
        session = get_session()
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        
        predictions = session.query(Prediction)\
            .filter(and_(
                Prediction.timestamp >= time_threshold,
                Prediction.agent_type == agent_type
            ))\
            .all()
        
        session.close()
        
        if not predictions:
            return {
                'count': 0,
                'avg_probability': 0,
                'max_probability': 0,
                'high_risk_count': 0,
                'medium_risk_count': 0,
                'low_risk_count': 0
            }
        
        # Calculate statistics
        count = len(predictions)
        probabilities = [p.probability for p in predictions]
        avg_probability = sum(probabilities) / count if count > 0 else 0
        max_probability = max(probabilities) if probabilities else 0
        
        # Count by risk level
        high_risk_count = sum(1 for p in predictions if p.probability >= 0.7)
        medium_risk_count = sum(1 for p in predictions if 0.4 <= p.probability < 0.7)
        low_risk_count = sum(1 for p in predictions if p.probability < 0.4)
        
        return {
            'count': count,
            'avg_probability': avg_probability,
            'max_probability': max_probability,
            'high_risk_count': high_risk_count,
            'medium_risk_count': medium_risk_count,
            'low_risk_count': low_risk_count
        }
    
    except Exception as e:
        logger.error(f"Error getting agent predictions summary: {str(e)}")
        return {
            'count': 0,
            'avg_probability': 0,
            'max_probability': 0,
            'high_risk_count': 0,
            'medium_risk_count': 0,
            'low_risk_count': 0
        }