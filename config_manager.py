import json
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration file path
CONFIG_FILE = "drilling_config.json"

def get_default_config():
    """
    Create and return default configuration settings.
    
    Returns:
        dict: Default configuration settings
    """
    default_config = {
        "thresholds": {
            "mechanical_sticking": 0.7,
            "differential_sticking": 0.7,
            "hole_cleaning": 0.6,
            "washout_mud_losses": 0.7
        },
        "update_frequency": 5,  # seconds
        "data_retention": {
            "max_alerts": 100,
            "max_history_hours": 24
        },
        "witsml": {
            "default_url": "https://witsml.example.com/store",
            "polling_interval": 5  # seconds
        },
        "visualization": {
            "default_time_window": 60  # minutes
        },
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return default_config

def load_config():
    """
    Load configuration from file or create default if not exists.
    
    Returns:
        dict: Configuration settings
    """
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                logger.info("Configuration loaded from file")
                return config
        else:
            # Create default configuration
            config = get_default_config()
            save_config(config)
            logger.info("Default configuration created")
            return config
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        return get_default_config()

def save_config(config):
    """
    Save configuration to file.
    
    Args:
        config (dict): Configuration to save
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Update timestamp
        config["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Save to file
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)
        
        logger.info("Configuration saved successfully")
        return True
    except Exception as e:
        logger.error(f"Error saving configuration: {str(e)}")
        return False

def update_threshold(agent_type, value):
    """
    Update a specific agent threshold.
    
    Args:
        agent_type (str): Type of ML agent 
        value (float): New threshold value
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        config = load_config()
        
        if agent_type in config["thresholds"]:
            config["thresholds"][agent_type] = value
            save_config(config)
            logger.info(f"Updated {agent_type} threshold to {value}")
            return True
        else:
            logger.error(f"Invalid agent type: {agent_type}")
            return False
    except Exception as e:
        logger.error(f"Error updating threshold: {str(e)}")
        return False

def update_update_frequency(seconds):
    """
    Update the data update frequency.
    
    Args:
        seconds (int): Update frequency in seconds
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        config = load_config()
        config["update_frequency"] = seconds
        save_config(config)
        logger.info(f"Updated update frequency to {seconds} seconds")
        return True
    except Exception as e:
        logger.error(f"Error updating update frequency: {str(e)}")
        return False
