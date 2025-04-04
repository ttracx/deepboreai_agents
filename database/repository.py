"""
Repository module for database operations.

This module provides functions for CRUD operations on the database models.
"""

import logging
from datetime import datetime, timedelta
from sqlalchemy import desc, func, and_
from database.connection import get_session
from database.models import DrillingData, Prediction, Alert

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ----- DrillingData Repository Methods -----

def save_drilling_data(data_dict):
    """
    Save drilling data to the database.
    
    Args:
        data_dict (dict): Drilling data as dictionary
    
    Returns:
        int: ID of saved drilling data
    """
    try:
        # Create model from dict
        drilling_data = DrillingData.from_dict(data_dict)
        
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return None
        
        try:
            # Add model to session
            session.add(drilling_data)
            session.commit()
            
            logger.info(f"Saved drilling data with ID: {drilling_data.id}")
            return drilling_data.id
        
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving drilling data: {str(e)}")
            return None
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in save_drilling_data: {str(e)}")
        return None

def get_latest_drilling_data():
    """
    Get the latest drilling data from the database.
    
    Returns:
        dict: Latest drilling data as dictionary
    """
    try:
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return None
        
        try:
            # Get latest record
            drilling_data = session.query(DrillingData).order_by(desc(DrillingData.timestamp)).first()
            
            if drilling_data:
                logger.debug(f"Retrieved latest drilling data with ID: {drilling_data.id}")
                return drilling_data.to_dict()
            else:
                logger.warning("No drilling data found in database")
                return None
        
        except Exception as e:
            logger.error(f"Error getting latest drilling data: {str(e)}")
            return None
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in get_latest_drilling_data: {str(e)}")
        return None

def get_drilling_data_by_id(data_id):
    """
    Get drilling data by ID.
    
    Args:
        data_id (int): ID of drilling data
    
    Returns:
        dict: Drilling data as dictionary
    """
    try:
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return None
        
        try:
            # Get record by ID
            drilling_data = session.query(DrillingData).filter(DrillingData.id == data_id).first()
            
            if drilling_data:
                logger.debug(f"Retrieved drilling data with ID: {drilling_data.id}")
                return drilling_data.to_dict()
            else:
                logger.warning(f"No drilling data found with ID: {data_id}")
                return None
        
        except Exception as e:
            logger.error(f"Error getting drilling data by ID: {str(e)}")
            return None
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in get_drilling_data_by_id: {str(e)}")
        return None

def get_drilling_data_by_time_range(start_time, end_time=None):
    """
    Get drilling data within a time range.
    
    Args:
        start_time (datetime): Start time
        end_time (datetime, optional): End time. Defaults to current time.
    
    Returns:
        list: List of drilling data dictionaries
    """
    try:
        # Set end time to current time if not provided
        if end_time is None:
            end_time = datetime.utcnow()
        
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return []
        
        try:
            # Get records within time range
            query = session.query(DrillingData).filter(
                and_(
                    DrillingData.timestamp >= start_time,
                    DrillingData.timestamp <= end_time
                )
            ).order_by(DrillingData.timestamp)
            
            # Convert to list of dictionaries
            result = [data.to_dict() for data in query.all()]
            
            logger.debug(f"Retrieved {len(result)} drilling data records within time range")
            return result
        
        except Exception as e:
            logger.error(f"Error getting drilling data by time range: {str(e)}")
            return []
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in get_drilling_data_by_time_range: {str(e)}")
        return []

def delete_old_drilling_data(days=30):
    """
    Delete drilling data older than specified days.
    
    Args:
        days (int): Number of days to keep
    
    Returns:
        int: Number of deleted records
    """
    try:
        # Calculate cutoff date
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return 0
        
        try:
            # Delete old records
            result = session.query(DrillingData).filter(DrillingData.timestamp < cutoff_date).delete()
            session.commit()
            
            logger.info(f"Deleted {result} old drilling data records")
            return result
        
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting old drilling data: {str(e)}")
            return 0
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in delete_old_drilling_data: {str(e)}")
        return 0

# ----- Prediction Repository Methods -----

def save_prediction(data_dict, agent_type, drilling_data_id=None):
    """
    Save prediction to the database.
    
    Args:
        data_dict (dict): Prediction data as dictionary
        agent_type (str): Type of ML agent
        drilling_data_id (int, optional): ID of associated drilling data
    
    Returns:
        int: ID of saved prediction
    """
    try:
        # Create model from dict
        prediction = Prediction.from_dict(data_dict, agent_type, drilling_data_id)
        
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return None
        
        try:
            # Add model to session
            session.add(prediction)
            session.commit()
            
            logger.info(f"Saved {agent_type} prediction with ID: {prediction.id}")
            return prediction.id
        
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving prediction: {str(e)}")
            return None
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in save_prediction: {str(e)}")
        return None

def get_latest_prediction(agent_type):
    """
    Get the latest prediction for a specific agent type.
    
    Args:
        agent_type (str): Type of ML agent
    
    Returns:
        dict: Latest prediction as dictionary
    """
    try:
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return None
        
        try:
            # Get latest record for agent type
            prediction = session.query(Prediction).filter(
                Prediction.agent_type == agent_type
            ).order_by(desc(Prediction.timestamp)).first()
            
            if prediction:
                logger.debug(f"Retrieved latest {agent_type} prediction with ID: {prediction.id}")
                return prediction.to_dict()
            else:
                logger.warning(f"No {agent_type} prediction found in database")
                return None
        
        except Exception as e:
            logger.error(f"Error getting latest prediction: {str(e)}")
            return None
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in get_latest_prediction: {str(e)}")
        return None

def get_predictions_by_drilling_data_id(drilling_data_id):
    """
    Get predictions for a specific drilling data record.
    
    Args:
        drilling_data_id (int): ID of drilling data
    
    Returns:
        dict: Dictionary of predictions by agent type
    """
    try:
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return {}
        
        try:
            # Get predictions for drilling data ID
            predictions_query = session.query(Prediction).filter(
                Prediction.drilling_data_id == drilling_data_id
            )
            
            # Convert to dictionary by agent type
            result = {}
            for prediction in predictions_query.all():
                result[prediction.agent_type] = prediction.to_dict()
            
            logger.debug(f"Retrieved {len(result)} predictions for drilling data ID: {drilling_data_id}")
            return result
        
        except Exception as e:
            logger.error(f"Error getting predictions by drilling data ID: {str(e)}")
            return {}
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in get_predictions_by_drilling_data_id: {str(e)}")
        return {}

def get_predictions_by_time_range(agent_type, start_time, end_time=None):
    """
    Get predictions for a specific agent type within a time range.
    
    Args:
        agent_type (str): Type of ML agent
        start_time (datetime): Start time
        end_time (datetime, optional): End time. Defaults to current time.
    
    Returns:
        list: List of prediction dictionaries
    """
    try:
        # Set end time to current time if not provided
        if end_time is None:
            end_time = datetime.utcnow()
        
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return []
        
        try:
            # Get records within time range
            query = session.query(Prediction).filter(
                and_(
                    Prediction.agent_type == agent_type,
                    Prediction.timestamp >= start_time,
                    Prediction.timestamp <= end_time
                )
            ).order_by(Prediction.timestamp)
            
            # Convert to list of dictionaries
            result = [prediction.to_dict() for prediction in query.all()]
            
            logger.debug(f"Retrieved {len(result)} {agent_type} predictions within time range")
            return result
        
        except Exception as e:
            logger.error(f"Error getting predictions by time range: {str(e)}")
            return []
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in get_predictions_by_time_range: {str(e)}")
        return []

def delete_old_predictions(days=30):
    """
    Delete predictions older than specified days.
    
    Args:
        days (int): Number of days to keep
    
    Returns:
        int: Number of deleted records
    """
    try:
        # Calculate cutoff date
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return 0
        
        try:
            # Delete old records
            result = session.query(Prediction).filter(Prediction.timestamp < cutoff_date).delete()
            session.commit()
            
            logger.info(f"Deleted {result} old prediction records")
            return result
        
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting old predictions: {str(e)}")
            return 0
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in delete_old_predictions: {str(e)}")
        return 0

# ----- Alert Repository Methods -----

def save_alert(data_dict, prediction_id=None):
    """
    Save alert to the database.
    
    Args:
        data_dict (dict): Alert data as dictionary
        prediction_id (int, optional): ID of associated prediction
    
    Returns:
        int: ID of saved alert
    """
    try:
        # Create model from dict
        alert = Alert.from_dict(data_dict, prediction_id)
        
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return None
        
        try:
            # Add model to session
            session.add(alert)
            session.commit()
            
            logger.info(f"Saved alert with ID: {alert.id}")
            return alert.id
        
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving alert: {str(e)}")
            return None
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in save_alert: {str(e)}")
        return None

def get_alerts_by_time_range(start_time, end_time=None, acknowledged=None):
    """
    Get alerts within a time range.
    
    Args:
        start_time (datetime): Start time
        end_time (datetime, optional): End time. Defaults to current time.
        acknowledged (bool, optional): Filter by acknowledged status. None for all.
    
    Returns:
        list: List of alert dictionaries
    """
    try:
        # Set end time to current time if not provided
        if end_time is None:
            end_time = datetime.utcnow()
        
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return []
        
        try:
            # Build query
            query = session.query(Alert).filter(
                and_(
                    Alert.timestamp >= start_time,
                    Alert.timestamp <= end_time
                )
            )
            
            # Add acknowledged filter if provided
            if acknowledged is not None:
                query = query.filter(Alert.acknowledged == acknowledged)
            
            # Order by timestamp descending (newest first)
            query = query.order_by(desc(Alert.timestamp))
            
            # Convert to list of dictionaries
            result = [alert.to_dict() for alert in query.all()]
            
            logger.debug(f"Retrieved {len(result)} alerts within time range")
            return result
        
        except Exception as e:
            logger.error(f"Error getting alerts by time range: {str(e)}")
            return []
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in get_alerts_by_time_range: {str(e)}")
        return []

def get_alert_by_id(alert_id):
    """
    Get alert by ID.
    
    Args:
        alert_id (int): ID of the alert
    
    Returns:
        dict: Alert as dictionary or None if not found
    """
    try:
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return None
        
        try:
            # Get alert by ID
            alert = session.query(Alert).filter(Alert.id == alert_id).first()
            
            if alert:
                logger.debug(f"Retrieved alert with ID: {alert.id}")
                return alert.to_dict()
            else:
                logger.warning(f"No alert found with ID: {alert_id}")
                return None
        
        except Exception as e:
            logger.error(f"Error getting alert by ID: {str(e)}")
            return None
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in get_alert_by_id: {str(e)}")
        return None

def update_alert_acknowledgement(alert_id, acknowledged=True):
    """
    Update the acknowledgement status of an alert.
    
    Args:
        alert_id (int): ID of the alert
        acknowledged (bool, optional): New acknowledgement status. Defaults to True.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return False
        
        try:
            # Get alert by ID
            alert = session.query(Alert).filter(Alert.id == alert_id).first()
            
            if alert:
                # Update status
                alert.acknowledged = acknowledged
                session.commit()
                
                logger.info(f"Updated acknowledgement status for alert ID {alert_id} to {acknowledged}")
                return True
            else:
                logger.warning(f"No alert found with ID: {alert_id}")
                return False
        
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating alert acknowledgement: {str(e)}")
            return False
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in update_alert_acknowledgement: {str(e)}")
        return False

def get_alert_summary(days=7):
    """
    Get a summary of alerts by type and severity for a specified period.
    
    Args:
        days (int, optional): Number of days to include in summary. Defaults to 7.
    
    Returns:
        dict: Summary of alerts by type and severity
    """
    try:
        # Calculate start time
        start_time = datetime.utcnow() - timedelta(days=days)
        
        # Get session
        session = get_session()
        if not session:
            logger.error("Failed to get database session")
            return {}
        
        try:
            # Get count by type
            type_query = session.query(
                Alert.alert_type,
                func.count(Alert.id).label('count')
            ).filter(
                Alert.timestamp >= start_time
            ).group_by(Alert.alert_type)
            
            # Get count by severity
            severity_query = session.query(
                Alert.severity,
                func.count(Alert.id).label('count')
            ).filter(
                Alert.timestamp >= start_time
            ).group_by(Alert.severity)
            
            # Get count by day
            day_query = session.query(
                func.date(Alert.timestamp).label('date'),
                func.count(Alert.id).label('count')
            ).filter(
                Alert.timestamp >= start_time
            ).group_by(func.date(Alert.timestamp))
            
            # Build result dictionary
            result = {
                'by_type': {item.alert_type: item.count for item in type_query.all()},
                'by_severity': {item.severity: item.count for item in severity_query.all()},
                'by_day': {str(item.date): item.count for item in day_query.all()},
                'total': session.query(func.count(Alert.id)).filter(Alert.timestamp >= start_time).scalar()
            }
            
            logger.debug(f"Generated alert summary for last {days} days")
            return result
        
        except Exception as e:
            logger.error(f"Error getting alert summary: {str(e)}")
            return {}
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error in get_alert_summary: {str(e)}")
        return {}