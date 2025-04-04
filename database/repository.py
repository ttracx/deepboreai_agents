"""
Database repository module for the drilling prediction application.

This module provides functions for storing and retrieving data from the database.
"""

import logging
from datetime import datetime, timedelta
from sqlalchemy import desc, asc, and_, func
from database.connection import get_session
from database.models import DrillingData, Prediction, Alert

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def store_drilling_data(data_dict):
    """
    Store drilling data in the database.
    
    Args:
        data_dict (dict): Dictionary containing drilling data
        
    Returns:
        DrillingData: The stored drilling data object, or None if failed
    """
    try:
        session = get_session()
        if session is None:
            return None
        
        # Create drilling data object from dictionary
        drilling_data = DrillingData.from_dict(data_dict)
        
        # Add and commit to database
        session.add(drilling_data)
        session.commit()
        
        # Get the ID of the new record
        drilling_data_id = drilling_data.id
        
        # Close session
        session.close()
        
        logger.info(f"Stored drilling data with ID: {drilling_data_id}")
        return drilling_data
        
    except Exception as e:
        logger.error(f"Error storing drilling data: {str(e)}")
        if session:
            session.rollback()
            session.close()
        return None


def store_prediction(prediction_dict, agent_type, drilling_data_id):
    """
    Store a prediction in the database.
    
    Args:
        prediction_dict (dict): Dictionary containing prediction data
        agent_type (str): Type of ML agent that generated the prediction
        drilling_data_id (int): ID of the associated drilling data
        
    Returns:
        Prediction: The stored prediction object, or None if failed
    """
    try:
        session = get_session()
        if session is None:
            return None
        
        # Create prediction object from dictionary
        prediction = Prediction.from_dict(prediction_dict, agent_type, drilling_data_id)
        
        # Add and commit to database
        session.add(prediction)
        session.commit()
        
        # Get the ID of the new record
        prediction_id = prediction.id
        
        # Close session
        session.close()
        
        logger.info(f"Stored {agent_type} prediction with ID: {prediction_id}")
        return prediction
        
    except Exception as e:
        logger.error(f"Error storing prediction: {str(e)}")
        if session:
            session.rollback()
            session.close()
        return None


def store_alert(alert_dict, prediction_id):
    """
    Store an alert in the database.
    
    Args:
        alert_dict (dict): Dictionary containing alert data
        prediction_id (int): ID of the associated prediction
        
    Returns:
        Alert: The stored alert object, or None if failed
    """
    try:
        session = get_session()
        if session is None:
            return None
        
        # Create alert object from dictionary
        alert = Alert.from_dict(alert_dict, prediction_id)
        
        # Add and commit to database
        session.add(alert)
        session.commit()
        
        # Get the ID of the new record
        alert_id = alert.id
        
        # Close session
        session.close()
        
        logger.info(f"Stored alert with ID: {alert_id}")
        return alert
        
    except Exception as e:
        logger.error(f"Error storing alert: {str(e)}")
        if session:
            session.rollback()
            session.close()
        return None


def get_recent_drilling_data(limit=100):
    """
    Get recent drilling data from the database.
    
    Args:
        limit (int): Maximum number of records to retrieve
        
    Returns:
        list: List of drilling data objects
    """
    try:
        session = get_session()
        if session is None:
            return []
        
        # Query recent drilling data ordered by timestamp
        drilling_data = session.query(DrillingData)\
            .order_by(desc(DrillingData.timestamp))\
            .limit(limit)\
            .all()
        
        # Close session
        session.close()
        
        return drilling_data
        
    except Exception as e:
        logger.error(f"Error retrieving drilling data: {str(e)}")
        if session:
            session.close()
        return []


def get_drilling_data_in_timeframe(hours=24):
    """
    Get drilling data from the last specified hours.
    
    Args:
        hours (int): Number of hours to look back
        
    Returns:
        list: List of drilling data objects
    """
    try:
        session = get_session()
        if session is None:
            return []
        
        # Calculate time threshold
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        
        # Query drilling data within timeframe
        drilling_data = session.query(DrillingData)\
            .filter(DrillingData.timestamp >= time_threshold)\
            .order_by(asc(DrillingData.timestamp))\
            .all()
        
        # Close session
        session.close()
        
        return drilling_data
        
    except Exception as e:
        logger.error(f"Error retrieving drilling data in timeframe: {str(e)}")
        if session:
            session.close()
        return []


def get_recent_predictions(agent_type=None, limit=100):
    """
    Get recent predictions from the database.
    
    Args:
        agent_type (str, optional): Filter by agent type
        limit (int): Maximum number of records to retrieve
        
    Returns:
        list: List of prediction objects
    """
    try:
        session = get_session()
        if session is None:
            return []
        
        # Build query
        query = session.query(Prediction).order_by(desc(Prediction.timestamp))
        
        # Apply filter if agent_type is specified
        if agent_type:
            query = query.filter(Prediction.agent_type == agent_type)
        
        # Apply limit and execute query
        predictions = query.limit(limit).all()
        
        # Close session
        session.close()
        
        return predictions
        
    except Exception as e:
        logger.error(f"Error retrieving predictions: {str(e)}")
        if session:
            session.close()
        return []


def get_recent_alerts(limit=100, include_acknowledged=False):
    """
    Get recent alerts from the database.
    
    Args:
        limit (int): Maximum number of records to retrieve
        include_acknowledged (bool): Whether to include acknowledged alerts
        
    Returns:
        list: List of alert objects
    """
    try:
        session = get_session()
        if session is None:
            return []
        
        # Build query
        query = session.query(Alert).order_by(desc(Alert.timestamp))
        
        # Filter out acknowledged alerts if specified
        if not include_acknowledged:
            query = query.filter(Alert.acknowledged == False)
        
        # Apply limit and execute query
        alerts = query.limit(limit).all()
        
        # Close session
        session.close()
        
        return alerts
        
    except Exception as e:
        logger.error(f"Error retrieving alerts: {str(e)}")
        if session:
            session.close()
        return []


def acknowledge_alert(alert_id):
    """
    Mark an alert as acknowledged.
    
    Args:
        alert_id (int): ID of the alert to acknowledge
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        session = get_session()
        if session is None:
            return False
        
        # Find the alert
        alert = session.query(Alert).filter(Alert.id == alert_id).first()
        
        if alert:
            # Update the acknowledged flag
            alert.acknowledged = True
            session.commit()
            session.close()
            logger.info(f"Alert {alert_id} acknowledged")
            return True
        else:
            session.close()
            logger.warning(f"Alert {alert_id} not found")
            return False
        
    except Exception as e:
        logger.error(f"Error acknowledging alert: {str(e)}")
        if session:
            session.rollback()
            session.close()
        return False


def get_parameter_statistics(parameter_name, hours=24):
    """
    Get statistics for a drilling parameter over time.
    
    Args:
        parameter_name (str): Name of the parameter to analyze
        hours (int): Number of hours to look back
        
    Returns:
        dict: Dictionary containing statistics
    """
    try:
        session = get_session()
        if session is None:
            return {}
        
        # Calculate time threshold
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        
        # Map parameter name to column
        param_map = {
            'WOB': DrillingData.wob,
            'ROP': DrillingData.rop,
            'RPM': DrillingData.rpm,
            'Torque': DrillingData.torque,
            'SPP': DrillingData.spp,
            'Flow_Rate': DrillingData.flow_rate,
            'ECD': DrillingData.ecd,
            'MSE': DrillingData.mse
        }
        
        column = param_map.get(parameter_name)
        if not column:
            session.close()
            logger.warning(f"Unknown parameter: {parameter_name}")
            return {}
        
        # Query statistics
        stats = session.query(
            func.avg(column).label('avg'),
            func.min(column).label('min'),
            func.max(column).label('max'),
            func.stddev(column).label('std')
        ).filter(DrillingData.timestamp >= time_threshold).first()
        
        # Close session
        session.close()
        
        return {
            'avg': stats.avg if stats.avg is not None else 0,
            'min': stats.min if stats.min is not None else 0,
            'max': stats.max if stats.max is not None else 0,
            'std': stats.std if stats.std is not None else 0
        }
        
    except Exception as e:
        logger.error(f"Error getting parameter statistics: {str(e)}")
        if session:
            session.close()
        return {}


def get_database_statistics():
    """
    Get general database statistics for the dashboard.
    
    Returns:
        dict: Dictionary containing database statistics
    """
    try:
        session = get_session()
        if session is None:
            return None
        
        # Get count of drilling data points
        drilling_data_count = session.query(func.count(DrillingData.id)).scalar() or 0
        
        # Get count of predictions
        predictions_count = session.query(func.count(Prediction.id)).scalar() or 0
        
        # Get count of alerts
        alerts_count = session.query(func.count(Alert.id)).scalar() or 0
        
        # Get last write timestamp
        last_drilling_data = session.query(DrillingData).order_by(desc(DrillingData.timestamp)).first()
        last_write = last_drilling_data.timestamp if last_drilling_data else None
        
        # Close session
        session.close()
        
        return {
            'total_data_points': drilling_data_count,
            'total_predictions': predictions_count,
            'total_alerts': alerts_count,
            'last_db_write': last_write
        }
        
    except Exception as e:
        logger.error(f"Error getting database statistics: {str(e)}")
        if session:
            session.close()
        return None


def get_alert_summary(hours=24, include_acknowledged=True):
    """
    Get a summary of alerts in the specified time period.
    
    Args:
        hours (int): Number of hours to look back
        include_acknowledged (bool): Whether to include acknowledged alerts
        
    Returns:
        dict: Dictionary containing alert summary information
    """
    try:
        session = get_session()
        if session is None:
            return None
        
        # Calculate time threshold
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        
        # Base query
        query = session.query(Alert).filter(Alert.timestamp >= time_threshold)
        
        # Filter out acknowledged alerts if specified
        if not include_acknowledged:
            query = query.filter(Alert.acknowledged == False)
        
        # Get all matching alerts
        alerts = query.all()
        
        # Close session
        session.close()
        
        if not alerts:
            return {'alerts': [], 'counts_by_type': {}, 'counts_by_severity': {}}
        
        # Convert to dictionaries
        alert_dicts = [alert.to_dict() for alert in alerts]
        
        # Count by type
        counts_by_type = {}
        for alert in alerts:
            alert_type = alert.alert_type
            if alert_type not in counts_by_type:
                counts_by_type[alert_type] = 0
            counts_by_type[alert_type] += 1
        
        # Count by severity
        counts_by_severity = {}
        for alert in alerts:
            severity = alert.severity
            if severity not in counts_by_severity:
                counts_by_severity[severity] = 0
            counts_by_severity[severity] += 1
        
        return {
            'alerts': alert_dicts,
            'counts_by_type': counts_by_type,
            'counts_by_severity': counts_by_severity
        }
        
    except Exception as e:
        logger.error(f"Error getting alert summary: {str(e)}")
        if session:
            session.close()
        return None


def get_time_series_data(parameters, hours=24):
    """
    Get time series data for selected parameters in the specified time period.
    
    Args:
        parameters (list): List of parameter names to retrieve
        hours (int): Number of hours to look back
        
    Returns:
        dict: Dictionary with timestamps and parameter values
    """
    try:
        # Get drilling data within the time frame
        drilling_data = get_drilling_data_in_timeframe(hours=hours)
        
        if not drilling_data:
            return None
        
        # Map parameter names to model attributes
        param_map = {
            'WOB': 'wob',
            'ROP': 'rop',
            'RPM': 'rpm',
            'Torque': 'torque',
            'SPP': 'spp',
            'Flow_Rate': 'flow_rate',
            'ECD': 'ecd',
            'MSE': 'mse'
        }
        
        # Initialize result dictionary
        result = {'timestamps': []}
        for param in parameters:
            result[param] = []
        
        # Populate result
        for data_point in drilling_data:
            result['timestamps'].append(data_point.timestamp)
            
            for param in parameters:
                model_attr = param_map.get(param)
                if model_attr and hasattr(data_point, model_attr):
                    result[param].append(getattr(data_point, model_attr))
                else:
                    result[param].append(0)  # Default value if attribute not found
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting time series data: {str(e)}")
        return None


def get_risk_trend(agent_type, hours=24, interval_hours=1):
    """
    Get the risk trend for a specific agent over time.
    
    Args:
        agent_type (str): Type of ML agent
        hours (int): Total time window to analyze (hours)
        interval_hours (int): Size of each interval (hours)
        
    Returns:
        dict: Dictionary with timestamps and average risk probabilities
    """
    try:
        session = get_session()
        if session is None:
            return {'timestamps': [], 'probabilities': []}
        
        # Calculate time threshold
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        
        # Get all predictions in the timeframe
        predictions = session.query(Prediction)\
            .filter(and_(
                Prediction.timestamp >= time_threshold,
                Prediction.agent_type == agent_type
            ))\
            .order_by(asc(Prediction.timestamp))\
            .all()
        
        session.close()
        
        if not predictions:
            return {'timestamps': [], 'probabilities': []}
        
        # Group by intervals
        intervals = int(hours / interval_hours)
        interval_duration = timedelta(hours=interval_hours)
        
        timestamps = []
        probabilities = []
        
        interval_start = time_threshold
        for i in range(intervals):
            interval_end = interval_start + interval_duration
            
            # Filter predictions in this interval
            interval_predictions = [p for p in predictions if interval_start <= p.timestamp < interval_end]
            
            # Calculate average probability for this interval
            if interval_predictions:
                avg_probability = sum(p.probability for p in interval_predictions) / len(interval_predictions)
                timestamps.append(interval_start.strftime("%Y-%m-%d %H:%M:%S"))
                probabilities.append(avg_probability)
            else:
                timestamps.append(interval_start.strftime("%Y-%m-%d %H:%M:%S"))
                probabilities.append(0)
            
            interval_start = interval_end
        
        return {'timestamps': timestamps, 'probabilities': probabilities}
        
    except Exception as e:
        logger.error(f"Error getting risk trend: {str(e)}")
        if session:
            session.close()
        return {'timestamps': [], 'probabilities': []}