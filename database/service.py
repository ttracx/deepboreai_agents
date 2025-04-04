"""
Service module for database operations.

This module provides high-level service functions that use the repository layer
to interact with the database in a business-logic-focused way.
"""

import logging
from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError
from database.repository import (
    save_drilling_data, get_latest_drilling_data, get_drilling_data_by_id,
    get_drilling_data_by_time_range, delete_old_drilling_data,
    save_prediction, get_latest_prediction, get_predictions_by_drilling_data_id,
    get_predictions_by_time_range, delete_old_predictions,
    save_alert, get_alerts_by_time_range, get_alert_by_id,
    update_alert_acknowledgement, get_alert_summary
)
from database.connection import init_db

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseService:
    """Service class for database operations."""
    
    def __init__(self):
        """Initialize the database service."""
        # Initialize database schema
        init_result = init_db()
        if init_result:
            logger.info("Database initialized successfully")
        else:
            logger.error("Database initialization failed")
    
    def store_drilling_data(self, data):
        """
        Store drilling data in the database.
        
        Args:
            data (dict): Drilling data to store
            
        Returns:
            int: ID of stored data or None if error
        """
        try:
            # Validate data
            if not data:
                logger.warning("Empty data provided to store_drilling_data")
                return None
            
            # Save data to database
            data_id = save_drilling_data(data)
            
            if data_id:
                logger.info(f"Stored drilling data with ID: {data_id}")
            else:
                logger.warning("Failed to store drilling data")
                
            return data_id
            
        except Exception as e:
            logger.error(f"Error in store_drilling_data: {str(e)}")
            return None
    
    def get_current_drilling_data(self):
        """
        Get the current (latest) drilling data.
        
        Returns:
            dict: Latest drilling data or None if not available
        """
        try:
            # Get latest data from database
            data = get_latest_drilling_data()
            
            if data:
                logger.debug("Retrieved current drilling data")
            else:
                logger.warning("No current drilling data available")
                
            return data
            
        except Exception as e:
            logger.error(f"Error in get_current_drilling_data: {str(e)}")
            return None
    
    def get_drilling_history(self, hours=24):
        """
        Get drilling data history for a specified number of hours.
        
        Args:
            hours (int): Number of hours of history to retrieve
            
        Returns:
            list: List of drilling data dictionaries
        """
        try:
            # Calculate start time
            start_time = datetime.utcnow() - timedelta(hours=hours)
            
            # Get data from database
            data_list = get_drilling_data_by_time_range(start_time)
            
            logger.info(f"Retrieved {len(data_list)} drilling history records for last {hours} hours")
            return data_list
            
        except Exception as e:
            logger.error(f"Error in get_drilling_history: {str(e)}")
            return []
    
    def store_prediction(self, prediction_data, agent_type, drilling_data_id=None):
        """
        Store a prediction in the database.
        
        Args:
            prediction_data (dict): Prediction data
            agent_type (str): Type of ML agent
            drilling_data_id (int, optional): ID of associated drilling data
            
        Returns:
            int: ID of stored prediction or None if error
        """
        try:
            # Validate data
            if not prediction_data:
                logger.warning(f"Empty prediction data provided for {agent_type}")
                return None
            
            # Save prediction to database
            prediction_id = save_prediction(prediction_data, agent_type, drilling_data_id)
            
            if prediction_id:
                logger.info(f"Stored {agent_type} prediction with ID: {prediction_id}")
            else:
                logger.warning(f"Failed to store {agent_type} prediction")
                
            return prediction_id
            
        except Exception as e:
            logger.error(f"Error in store_prediction: {str(e)}")
            return None
    
    def get_current_prediction(self, agent_type):
        """
        Get the current (latest) prediction for a specific agent type.
        
        Args:
            agent_type (str): Type of ML agent
            
        Returns:
            dict: Latest prediction data or None if not available
        """
        try:
            # Get latest prediction from database
            prediction = get_latest_prediction(agent_type)
            
            if prediction:
                logger.debug(f"Retrieved current {agent_type} prediction")
            else:
                logger.warning(f"No current {agent_type} prediction available")
                
            return prediction
            
        except Exception as e:
            logger.error(f"Error in get_current_prediction: {str(e)}")
            return None
    
    def get_all_current_predictions(self):
        """
        Get the current (latest) predictions for all agent types.
        
        Returns:
            dict: Dictionary of latest predictions by agent type
        """
        try:
            # Get latest predictions for each agent type
            agent_types = [
                'mechanical_sticking',
                'differential_sticking',
                'hole_cleaning',
                'washout_mud_losses',
                'rop_optimization'
            ]
            
            result = {}
            for agent_type in agent_types:
                prediction = self.get_current_prediction(agent_type)
                result[agent_type] = prediction
            
            logger.debug("Retrieved all current predictions")
            return result
            
        except Exception as e:
            logger.error(f"Error in get_all_current_predictions: {str(e)}")
            return {}
    
    def get_prediction_history(self, agent_type, hours=24):
        """
        Get prediction history for a specific agent type.
        
        Args:
            agent_type (str): Type of ML agent
            hours (int): Number of hours of history to retrieve
            
        Returns:
            list: List of prediction dictionaries
        """
        try:
            # Calculate start time
            start_time = datetime.utcnow() - timedelta(hours=hours)
            
            # Get predictions from database
            predictions = get_predictions_by_time_range(agent_type, start_time)
            
            logger.info(f"Retrieved {len(predictions)} {agent_type} prediction records for last {hours} hours")
            return predictions
            
        except Exception as e:
            logger.error(f"Error in get_prediction_history: {str(e)}")
            return []
    
    def store_alert(self, alert_data, prediction_id=None):
        """
        Store an alert in the database.
        
        Args:
            alert_data (dict): Alert data
            prediction_id (int, optional): ID of associated prediction
            
        Returns:
            int: ID of stored alert or None if error
        """
        try:
            # Validate data
            if not alert_data:
                logger.warning("Empty alert data provided")
                return None
            
            # Save alert to database
            alert_id = save_alert(alert_data, prediction_id)
            
            if alert_id:
                logger.info(f"Stored alert with ID: {alert_id}")
            else:
                logger.warning("Failed to store alert")
                
            return alert_id
            
        except Exception as e:
            logger.error(f"Error in store_alert: {str(e)}")
            return None
    
    def get_alerts(self, hours=24, acknowledged=None):
        """
        Get alerts for a specified number of hours.
        
        Args:
            hours (int): Number of hours of history to retrieve
            acknowledged (bool, optional): Filter by acknowledged status
            
        Returns:
            list: List of alert dictionaries
        """
        try:
            # Calculate start time
            start_time = datetime.utcnow() - timedelta(hours=hours)
            
            # Get alerts from database
            alerts = get_alerts_by_time_range(start_time, None, acknowledged)
            
            status_str = "" if acknowledged is None else f" (acknowledged: {acknowledged})"
            logger.info(f"Retrieved {len(alerts)} alerts for last {hours} hours{status_str}")
            return alerts
            
        except Exception as e:
            logger.error(f"Error in get_alerts: {str(e)}")
            return []
    
    def acknowledge_alert(self, alert_id, acknowledged=True):
        """
        Update the acknowledgement status of an alert.
        
        Args:
            alert_id (int): ID of the alert
            acknowledged (bool): New acknowledgement status
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Update alert status
            result = update_alert_acknowledgement(alert_id, acknowledged)
            
            if result:
                status_str = "acknowledged" if acknowledged else "unacknowledged"
                logger.info(f"Alert {alert_id} {status_str} successfully")
            else:
                logger.warning(f"Failed to update acknowledgement for alert {alert_id}")
                
            return result
            
        except Exception as e:
            logger.error(f"Error in acknowledge_alert: {str(e)}")
            return False
    
    def get_alert_statistics(self, days=7):
        """
        Get statistical summary of alerts.
        
        Args:
            days (int): Number of days to include in summary
            
        Returns:
            dict: Summary of alerts
        """
        try:
            # Get alert summary
            summary = get_alert_summary(days)
            
            logger.info(f"Retrieved alert statistics for last {days} days")
            return summary
            
        except Exception as e:
            logger.error(f"Error in get_alert_statistics: {str(e)}")
            return {}
    
    def perform_maintenance(self, keep_days=30):
        """
        Perform database maintenance by removing old records.
        
        Args:
            keep_days (int): Number of days of data to keep
            
        Returns:
            dict: Summary of maintenance actions
        """
        try:
            # Delete old data
            drilling_deleted = delete_old_drilling_data(keep_days)
            predictions_deleted = delete_old_predictions(keep_days)
            
            result = {
                'drilling_data_deleted': drilling_deleted,
                'predictions_deleted': predictions_deleted,
                'timestamp': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                'status': 'success'
            }
            
            logger.info(f"Database maintenance completed: {drilling_deleted} drilling records and "
                       f"{predictions_deleted} prediction records deleted")
            return result
            
        except Exception as e:
            logger.error(f"Error in perform_maintenance: {str(e)}")
            return {
                'drilling_data_deleted': 0,
                'predictions_deleted': 0,
                'timestamp': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                'status': 'error',
                'error': str(e)
            }
    
    def check_database_connection(self):
        """
        Check if the database connection is working.
        
        Returns:
            bool: True if connection is working, False otherwise
        """
        try:
            # Try to execute a simple query
            result = get_latest_drilling_data() is not None
            
            if result:
                logger.info("Database connection check: SUCCESS")
            else:
                # If no data found, try a different approach
                # This could indicate either connection issues or just empty database
                try:
                    from database.connection import get_session
                    session = get_session()
                    if session:
                        # If we got a session, run a simple query
                        session.execute("SELECT 1")
                        session.close()
                        logger.info("Database connection check: SUCCESS (no data)")
                        return True
                    else:
                        logger.error("Database connection check: FAILED (no session)")
                        return False
                except Exception as inner_e:
                    logger.error(f"Database connection inner check failed: {str(inner_e)}")
                    return False
            
            return result
            
        except Exception as e:
            logger.error(f"Database connection check failed: {str(e)}")
            return False