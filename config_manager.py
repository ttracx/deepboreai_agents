"""
Configuration management for the drilling prediction application.

This module handles loading, saving, and updating configuration settings.
"""

import logging
import json
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Default configuration file path
CONFIG_FILE = "drilling_config.json"

def get_default_config():
    """
    Create and return default configuration settings.
    
    Returns:
        dict: Default configuration settings
    """
    return {
        "witsml": {
            "url": "",
            "username": "",
            "password": "",
            "well_uid": "",
            "wellbore_uid": "",
            "use_simulation": True,  # Use simulation mode by default
            "simulation_volatility": 0.2,
            "simulation_trending": True
        },
        "display": {
            "refresh_rate": 5,  # seconds
            "max_history_hours": 24,
            "default_time_window": "Last Hour",
            "show_prediction_details": True,
            "dark_mode": False
        },
        "ml_agents": {
            "enabled": True,
            "mechanical_sticking": {
                "enabled": True,
                "threshold": 0.6,
                "sensitivity": 0.8
            },
            "differential_sticking": {
                "enabled": True,
                "threshold": 0.65,
                "sensitivity": 0.7
            },
            "hole_cleaning": {
                "enabled": True,
                "threshold": 0.65,
                "sensitivity": 0.75
            },
            "washout_mud_losses": {
                "enabled": True,
                "threshold": 0.7,
                "sensitivity": 0.8
            },
            "rop_optimization": {
                "enabled": True,
                "threshold": 0.0,  # Not used for this agent
                "sensitivity": 0.5,
                "aggressiveness": 0.3
            }
        },
        "database": {
            "retention_days": 30,
            "auto_clean": True
        }
    }

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
                logger.info(f"Configuration loaded from {CONFIG_FILE}")
                
                # Ensure all default keys exist (for backward compatibility)
                default_config = get_default_config()
                
                # Update config with any missing default values
                # This handles the case where new config options are added in updates
                for section, section_values in default_config.items():
                    if section not in config:
                        config[section] = section_values
                        continue
                    
                    if isinstance(section_values, dict):
                        for key, value in section_values.items():
                            if key not in config[section]:
                                config[section][key] = value
                            
                            # Handle nested dictionaries (agent settings)
                            if isinstance(value, dict) and isinstance(config[section][key], dict):
                                for subkey, subvalue in value.items():
                                    if subkey not in config[section][key]:
                                        config[section][key][subkey] = subvalue
                
                return config
        else:
            # Create and save default config
            config = get_default_config()
            save_config(config)
            logger.info(f"Default configuration created and saved to {CONFIG_FILE}")
            return config
    
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        logger.info("Using default configuration")
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
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)
        logger.info(f"Configuration saved to {CONFIG_FILE}")
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
        # Validate input
        if not isinstance(value, (int, float)) or value < 0 or value > 1:
            logger.error(f"Invalid threshold value: {value}. Must be between 0 and 1.")
            return False
        
        # Load current config
        config = load_config()
        
        # Update threshold if agent exists
        if agent_type in config['ml_agents']:
            config['ml_agents'][agent_type]['threshold'] = value
            logger.info(f"Updated {agent_type} threshold to {value}")
            
            # Save updated config
            return save_config(config)
        else:
            logger.error(f"Unknown agent type: {agent_type}")
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
        # Validate input
        if not isinstance(seconds, (int, float)) or seconds < 1:
            logger.error(f"Invalid update frequency: {seconds}. Must be at least 1 second.")
            return False
        
        # Load current config
        config = load_config()
        
        # Update refresh rate
        config['display']['refresh_rate'] = int(seconds)
        logger.info(f"Updated refresh rate to {seconds} seconds")
        
        # Save updated config
        return save_config(config)
    
    except Exception as e:
        logger.error(f"Error updating refresh rate: {str(e)}")
        return False

def update_witsml_settings(witsml_config):
    """
    Update WITSML connection settings.
    
    Args:
        witsml_config (dict): WITSML configuration settings
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Load current config
        config = load_config()
        
        # Update WITSML settings
        for key, value in witsml_config.items():
            if key in config['witsml']:
                config['witsml'][key] = value
        
        logger.info("Updated WITSML connection settings")
        
        # Save updated config
        return save_config(config)
    
    except Exception as e:
        logger.error(f"Error updating WITSML settings: {str(e)}")
        return False

def toggle_agent(agent_type, enabled=True):
    """
    Enable or disable a specific ML agent.
    
    Args:
        agent_type (str): Type of ML agent
        enabled (bool): Whether to enable or disable the agent
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Load current config
        config = load_config()
        
        # Update agent enabled status if agent exists
        if agent_type in config['ml_agents']:
            config['ml_agents'][agent_type]['enabled'] = enabled
            status = "enabled" if enabled else "disabled"
            logger.info(f"{agent_type} {status}")
            
            # Save updated config
            return save_config(config)
        else:
            logger.error(f"Unknown agent type: {agent_type}")
            return False
    
    except Exception as e:
        logger.error(f"Error toggling agent: {str(e)}")
        return False

def toggle_simulation_mode(enabled=True):
    """
    Enable or disable simulation mode.
    
    Args:
        enabled (bool): Whether to enable or disable simulation mode
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Load current config
        config = load_config()
        
        # Update simulation mode
        config['witsml']['use_simulation'] = enabled
        status = "enabled" if enabled else "disabled"
        logger.info(f"Simulation mode {status}")
        
        # Save updated config
        return save_config(config)
    
    except Exception as e:
        logger.error(f"Error toggling simulation mode: {str(e)}")
        return False