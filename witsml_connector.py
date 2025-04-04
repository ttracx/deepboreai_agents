"""
WITSML connector for the drilling prediction application.

This module provides functions for connecting to and retrieving data from a WITSML server.
It includes a simulation mode for demo purposes when a real WITSML server is not available.
"""

import logging
import random
import math
import xml.etree.ElementTree as ET
import requests
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WitsmlClient:
    def __init__(self, url, username, password):
        """
        Initialize the WITSML client.
        
        Args:
            url (str): The URL of the WITSML server
            username (str): Username for authentication
            password (str): Password for authentication
        """
        self.url = url
        self.username = username
        self.password = password
        
        # Set up session with authentication
        self.session = requests.Session()
        self.session.auth = (username, password)
        
        # Define namespaces
        self.ns = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'witsml': 'http://www.witsml.org/schemas/1series'
        }
        
        logger.info(f"Initialized WITSML client for server at {url}")
    
    def get_version(self):
        """Get the WITSML server version"""
        soap_request = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:witsml="http://www.witsml.org/schemas/1series">
            <soap:Body>
                <witsml:WMLS_GetVersion />
            </soap:Body>
        </soap:Envelope>"""
        
        try:
            response = self.session.post(self.url, data=soap_request, headers={'Content-Type': 'text/xml'})
            response.raise_for_status()
            
            # Parse XML response
            root = ET.fromstring(response.text)
            version = root.find('.//witsml:WMLS_GetVersionResponse', self.ns)
            
            if version is not None and version.text:
                logger.info(f"WITSML server version: {version.text}")
                return version.text
            else:
                logger.warning("Failed to get WITSML server version")
                return None
                
        except Exception as e:
            logger.error(f"Error getting WITSML server version: {str(e)}")
            return None
    
    def get_cap(self):
        """Get the capabilities of the WITSML server"""
        soap_request = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:witsml="http://www.witsml.org/schemas/1series">
            <soap:Body>
                <witsml:WMLS_GetCap />
            </soap:Body>
        </soap:Envelope>"""
        
        try:
            response = self.session.post(self.url, data=soap_request, headers={'Content-Type': 'text/xml'})
            response.raise_for_status()
            
            # Parse XML response
            root = ET.fromstring(response.text)
            cap = root.find('.//witsml:WMLS_GetCapResponse', self.ns)
            
            if cap is not None and cap.text:
                logger.debug("Retrieved WITSML server capabilities")
                return cap.text
            else:
                logger.warning("Failed to get WITSML server capabilities")
                return None
                
        except Exception as e:
            logger.error(f"Error getting WITSML server capabilities: {str(e)}")
            return None
    
    def get_log_data(self, well_uid, wellbore_uid, log_uid=None, start_index=None, end_index=None):
        """
        Get log data from the WITSML server.
        
        Args:
            well_uid (str): UID of the well
            wellbore_uid (str): UID of the wellbore
            log_uid (str, optional): UID of the log. If not provided, gets the most recent log.
            start_index (str, optional): Starting index for the data
            end_index (str, optional): Ending index for the data
            
        Returns:
            dict: Log data in a dictionary format
        """
        # First, get the log UID if not provided
        if log_uid is None:
            logs = self.get_logs(well_uid, wellbore_uid)
            if logs and len(logs) > 0:
                # Get the most recent log
                log_uid = logs[0]['uid']
                logger.info(f"Using most recent log with UID: {log_uid}")
            else:
                logger.error("No logs found for wellbore")
                return None
        
        # Build query
        query = f"""<?xml version="1.0" encoding="UTF-8"?>
        <logs xmlns="http://www.witsml.org/schemas/1series" version="1.4.1.1">
            <log uidWell="{well_uid}" uidWellbore="{wellbore_uid}" uid="{log_uid}">
                <startIndex>{start_index if start_index else ''}</startIndex>
                <endIndex>{end_index if end_index else ''}</endIndex>
                <logData/>
            </log>
        </logs>"""
        
        soap_request = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:witsml="http://www.witsml.org/schemas/1series">
            <soap:Body>
                <witsml:WMLS_GetFromStore>
                    <witsml:WMLtypeIn>log</witsml:WMLtypeIn>
                    <witsml:QueryIn>{query}</witsml:QueryIn>
                    <witsml:OptionsIn>returnElements=all</witsml:OptionsIn>
                </witsml:WMLS_GetFromStore>
            </soap:Body>
        </soap:Envelope>"""
        
        try:
            response = self.session.post(self.url, data=soap_request, headers={'Content-Type': 'text/xml'})
            response.raise_for_status()
            
            # Parse XML response
            root = ET.fromstring(response.text)
            result = root.find('.//witsml:WMLS_GetFromStoreResponse', self.ns)
            
            if result is not None and result.text:
                # Parse the log data XML
                log_xml = ET.fromstring(result.text)
                
                # Extract log curve info
                mnemonics = []
                units = []
                
                for curve in log_xml.findall('.//logCurveInfo', {}):
                    mnemonic = self._get_element_text(curve, './mnemonic')
                    unit = self._get_element_text(curve, './unit')
                    
                    if mnemonic:
                        mnemonics.append(mnemonic)
                        units.append(unit)
                
                # Extract log data
                data_points = []
                
                for data_point in log_xml.findall('.//logData/data', {}):
                    if data_point.text:
                        values = data_point.text.split(',')
                        if len(values) == len(mnemonics):
                            data_point_dict = {}
                            
                            # Convert values to appropriate types
                            for i, value in enumerate(values):
                                try:
                                    # Try to convert to float or int
                                    if '.' in value:
                                        data_point_dict[mnemonics[i]] = float(value)
                                    else:
                                        data_point_dict[mnemonics[i]] = int(value)
                                except ValueError:
                                    # Keep as string if conversion fails
                                    data_point_dict[mnemonics[i]] = value
                            
                            data_points.append(data_point_dict)
                
                # Build result dictionary
                result_dict = {
                    'mnemonics': mnemonics,
                    'units': dict(zip(mnemonics, units)),
                    'data': data_points
                }
                
                logger.info(f"Retrieved {len(data_points)} log data points")
                return result_dict
            else:
                logger.warning("Failed to get log data")
                return None
                
        except Exception as e:
            logger.error(f"Error getting log data: {str(e)}")
            return None
    
    def get_logs(self, well_uid, wellbore_uid):
        """
        Get a list of logs for a wellbore.
        
        Args:
            well_uid (str): UID of the well
            wellbore_uid (str): UID of the wellbore
            
        Returns:
            list: List of log information dictionaries
        """
        # Build query
        query = f"""<?xml version="1.0" encoding="UTF-8"?>
        <logs xmlns="http://www.witsml.org/schemas/1series" version="1.4.1.1">
            <log uidWell="{well_uid}" uidWellbore="{wellbore_uid}"/>
        </logs>"""
        
        soap_request = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:witsml="http://www.witsml.org/schemas/1series">
            <soap:Body>
                <witsml:WMLS_GetFromStore>
                    <witsml:WMLtypeIn>log</witsml:WMLtypeIn>
                    <witsml:QueryIn>{query}</witsml:QueryIn>
                    <witsml:OptionsIn>returnElements=header</witsml:OptionsIn>
                </witsml:WMLS_GetFromStore>
            </soap:Body>
        </soap:Envelope>"""
        
        try:
            response = self.session.post(self.url, data=soap_request, headers={'Content-Type': 'text/xml'})
            response.raise_for_status()
            
            # Parse XML response
            root = ET.fromstring(response.text)
            result = root.find('.//witsml:WMLS_GetFromStoreResponse', self.ns)
            
            if result is not None and result.text:
                # Parse the logs XML
                logs_xml = ET.fromstring(result.text)
                
                # Extract log info
                logs = []
                
                for log in logs_xml.findall('.//log', {}):
                    log_info = {
                        'uid': log.get('uid'),
                        'name': self._get_element_text(log, './name'),
                        'indexType': self._get_element_text(log, './indexType'),
                        'startIndex': self._get_element_text(log, './startIndex'),
                        'endIndex': self._get_element_text(log, './endIndex'),
                        'direction': self._get_element_text(log, './direction'),
                        'serviceCompany': self._get_element_text(log, './serviceCompany')
                    }
                    logs.append(log_info)
                
                # Sort logs by end index (most recent first)
                logs.sort(key=lambda x: x['endIndex'] if x['endIndex'] else '', reverse=True)
                
                logger.info(f"Retrieved {len(logs)} logs for wellbore")
                return logs
            else:
                logger.warning("Failed to get logs")
                return []
                
        except Exception as e:
            logger.error(f"Error getting logs: {str(e)}")
            return []
    
    def _get_element_text(self, parent, xpath):
        """Helper method to get element text or None if the element doesn't exist"""
        element = parent.find(xpath, {})
        return element.text if element is not None else None


def test_connection(config):
    """
    Test the connection to the WITSML server.
    
    Args:
        config (dict): Connection configuration with url, username, password, well_uid, wellbore_uid
        
    Returns:
        bool: True if connection was successful, False otherwise
    """
    try:
        # Check if we're using simulation mode
        if config.get('use_simulation', True):
            logger.info("Using simulation mode, connection test successful")
            return True
        
        # Validate required fields
        required_fields = ['url', 'username', 'password', 'well_uid', 'wellbore_uid']
        for field in required_fields:
            if not config.get(field):
                logger.error(f"Missing required field: {field}")
                return False
        
        # Create client and test connection
        client = WitsmlClient(config['url'], config['username'], config['password'])
        version = client.get_version()
        
        if version:
            # Test getting logs
            logs = client.get_logs(config['well_uid'], config['wellbore_uid'])
            
            if logs:
                logger.info(f"Connection test successful, found {len(logs)} logs")
                return True
            else:
                logger.warning("Connection test partially successful, but no logs found")
                return False
        else:
            logger.error("Connection test failed, could not get server version")
            return False
    
    except Exception as e:
        logger.error(f"Connection test failed with error: {str(e)}")
        return False


def fetch_data(config):
    """
    Fetch the latest drilling data from the WITSML server.
    
    Args:
        config (dict): Connection configuration
        
    Returns:
        dict: The latest drilling data
    """
    try:
        # Check if we're using simulation mode
        if config.get('use_simulation', True):
            return generate_simulated_data()
        
        # Validate required fields
        required_fields = ['url', 'username', 'password', 'well_uid', 'wellbore_uid']
        for field in required_fields:
            if not config.get(field):
                logger.error(f"Missing required field: {field}")
                return None
        
        # Create client and fetch data
        client = WitsmlClient(config['url'], config['username'], config['password'])
        
        # Get the most recent log data point
        log_data = client.get_log_data(
            config['well_uid'],
            config['wellbore_uid'],
            log_uid=config.get('log_uid'),
            start_index=None,  # Get all available data
            end_index=None
        )
        
        if log_data and log_data['data']:
            # Get the most recent data point
            latest_data = log_data['data'][-1]
            
            # Calculate derived parameters
            data_with_derived = calculate_derived_parameters(latest_data)
            
            logger.info("Successfully fetched data from WITSML server")
            return data_with_derived
        else:
            logger.warning("No data found in WITSML server")
            return None
    
    except Exception as e:
        logger.error(f"Error fetching data from WITSML server: {str(e)}")
        return None


def generate_simulated_data():
    """
    Generate simulated drilling data for demonstration.
    
    Returns:
        dict: Simulated drilling data
    """
    # Base values for parameters
    base_values = {
        'DEPTH': 10000 + random.uniform(-50, 50),  # ft
        'WOB': 25 + random.uniform(-5, 5),  # klbs
        'ROP': 60 + random.uniform(-10, 15),  # ft/hr
        'RPM': 120 + random.uniform(-20, 20),  # rpm
        'TORQUE': 8000 + random.uniform(-1000, 1000),  # ft-lbs
        'SPP': 3500 + random.uniform(-200, 200),  # psi
        'FLOW_RATE': 600 + random.uniform(-50, 50),  # gpm
        'ECD': 12.5 + random.uniform(-0.5, 0.5),  # ppg
        'HOOK_LOAD': 200 + random.uniform(-20, 20),  # klbs
    }
    
    # Create drilling data
    data = {
        'depth': base_values['DEPTH'],
        'WOB': base_values['WOB'],
        'ROP': base_values['ROP'],
        'RPM': base_values['RPM'],
        'Torque': base_values['TORQUE'] / 1000,  # Convert to kft-lbs
        'SPP': base_values['SPP'],
        'Flow_Rate': base_values['FLOW_RATE'],
        'ECD': base_values['ECD'],
        'hook_load': base_values['HOOK_LOAD'],
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Add some random changes to simulate trends
    data['WOB_change'] = random.uniform(-1, 1)
    data['ROP_change'] = random.uniform(-2, 2)
    data['RPM_change'] = random.uniform(-5, 5)
    data['Torque_change'] = random.uniform(-0.2, 0.2)
    data['SPP_change'] = random.uniform(-50, 50)
    data['Flow_Rate_change'] = random.uniform(-10, 10)
    
    # Calculate derived parameters
    data_with_derived = calculate_derived_parameters(data)
    
    logger.debug("Generated simulated drilling data")
    return data_with_derived


def generate_time_series(end_time, minutes):
    """
    Generate time series data for demonstration.
    
    Args:
        end_time (datetime): The end time for the series
        minutes (int): Number of minutes to generate
        
    Returns:
        dict: Time series data with timestamp and parameter values
    """
    # Initialize result
    result = {
        'timestamp': [],
        'depth': [],
        'WOB': [],
        'ROP': [],
        'RPM': [],
        'Torque': [],
        'SPP': [],
        'Flow_Rate': [],
        'ECD': [],
        'hook_load': [],
        'MSE': [],
        'differential_pressure': [],
        'hole_cleaning_index': []
    }
    
    # Base values
    base = {
        'depth': 10000,
        'WOB': 25,
        'ROP': 60,
        'RPM': 120,
        'Torque': 8,  # kft-lbs
        'SPP': 3500,
        'Flow_Rate': 600,
        'ECD': 12.5,
        'hook_load': 200
    }
    
    # Generate time points at 10-second intervals
    time_points = []
    for i in range(minutes * 6):  # 6 points per minute (10-second intervals)
        time_point = end_time - timedelta(minutes=minutes) + timedelta(seconds=i*10)
        time_points.append(time_point)
    
    # Generate parameter values with realistic trends
    for t in time_points:
        # Time-based factor for trending (0 to 2π over the time range)
        time_factor = 2 * math.pi * time_points.index(t) / len(time_points)
        
        # Add values to result with some randomness and sinusoidal trends
        result['timestamp'].append(t.strftime("%Y-%m-%d %H:%M:%S"))
        
        # Depth increases steadily with some random variation
        result['depth'].append(base['depth'] + time_points.index(t) * 0.2 + random.uniform(-0.1, 0.1))
        
        # Other parameters follow sinusoidal patterns with noise
        result['WOB'].append(base['WOB'] + 3 * math.sin(time_factor) + random.uniform(-1, 1))
        result['ROP'].append(base['ROP'] + 10 * math.sin(time_factor * 0.7) + random.uniform(-5, 5))
        result['RPM'].append(base['RPM'] + 15 * math.sin(time_factor * 0.5) + random.uniform(-5, 5))
        result['Torque'].append(base['Torque'] + 1 * math.sin(time_factor * 1.3) + random.uniform(-0.3, 0.3))
        result['SPP'].append(base['SPP'] + 200 * math.sin(time_factor * 0.9) + random.uniform(-50, 50))
        result['Flow_Rate'].append(base['Flow_Rate'] + 40 * math.sin(time_factor * 1.1) + random.uniform(-10, 10))
        result['ECD'].append(base['ECD'] + 0.3 * math.sin(time_factor * 0.8) + random.uniform(-0.1, 0.1))
        result['hook_load'].append(base['hook_load'] + 15 * math.sin(time_factor * 0.6) + random.uniform(-5, 5))
        
        # Calculate some derived parameters
        current_index = len(result['timestamp']) - 1
        
        # MSE calculation
        wob = result['WOB'][current_index]
        rpm = result['RPM'][current_index]
        torque = result['Torque'][current_index]
        rop = result['ROP'][current_index]
        bit_diameter = 8.5  # inches
        
        if rop > 0:
            mse = 4 * wob * 1000 / (3.14159 * (bit_diameter ** 2)) + \
                  (480 * rpm * torque) / (3.14159 * (bit_diameter ** 2) * rop)
        else:
            mse = 0
            
        result['MSE'].append(mse)
        
        # Differential pressure
        depth = result['depth'][current_index]
        ecd = result['ECD'][current_index]
        hydrostatic = 0.052 * ecd * depth
        pore_pressure = 0.45 * depth
        result['differential_pressure'].append(max(0, hydrostatic - pore_pressure))
        
        # Hole cleaning index
        flow_rate = result['Flow_Rate'][current_index]
        rpm = result['RPM'][current_index]
        rop = result['ROP'][current_index]
        
        if flow_rate > 0 and rpm > 0:
            hole_cleaning = min(1.0, max(0.1, 
                0.5 + 0.3 * (flow_rate / 800) + 0.2 * (rpm / 150) - 0.1 * (rop / 50)
            ))
        else:
            hole_cleaning = 0.1
            
        result['hole_cleaning_index'].append(hole_cleaning)
    
    logger.info(f"Generated time series data with {len(time_points)} points")
    return result


def calculate_derived_parameters(data):
    """
    Calculate derived parameters from raw drilling data.
    
    Args:
        data (dict): Raw drilling data
        
    Returns:
        dict: Data with additional derived parameters
    """
    # Make a copy to avoid modifying the original
    result = data.copy()
    
    try:
        # Calculate Mechanical Specific Energy (MSE)
        if 'WOB' in data and 'RPM' in data and 'Torque' in data and 'ROP' in data:
            wob = data['WOB']  # klbs
            rpm = data['RPM']
            torque = data['Torque']  # kft-lbs
            rop = data['ROP']  # ft/hr
            bit_diameter = 8.5  # inches
            
            if rop > 0:
                mse = 4 * wob * 1000 / (3.14159 * (bit_diameter ** 2)) + \
                      (480 * rpm * torque) / (3.14159 * (bit_diameter ** 2) * rop)
                result['MSE'] = mse
            else:
                result['MSE'] = 0
        else:
            result['MSE'] = 0
        
        # Calculate differential pressure
        if 'depth' in data and 'ECD' in data:
            depth = data['depth']
            ecd = data['ECD']
            
            # Simple hydrostatic pressure calculation
            hydrostatic = 0.052 * ecd * depth  # psi
            
            # Assume a pore pressure gradient of 0.45 psi/ft (typical)
            pore_pressure = 0.45 * depth
            
            # Differential pressure is the difference
            result['differential_pressure'] = max(0, hydrostatic - pore_pressure)
        else:
            result['differential_pressure'] = 0
        
        # Calculate hole cleaning index
        if 'Flow_Rate' in data and 'RPM' in data and 'ROP' in data:
            flow_rate = data['Flow_Rate']
            rpm = data['RPM']
            rop = data['ROP']
            
            # Higher flow rate and RPM improve hole cleaning, higher ROP reduces it
            if flow_rate > 0 and rpm > 0:
                result['hole_cleaning_index'] = min(1.0, max(0.1, 
                    0.5 + 0.3 * (flow_rate / 800) + 0.2 * (rpm / 150) - 0.1 * (rop / 50)
                ))
            else:
                result['hole_cleaning_index'] = 0.1
        else:
            result['hole_cleaning_index'] = 0.1
        
        # Calculate drag factor
        if 'hook_load' in data and 'depth' in data:
            hook_load = data['hook_load']
            depth = data['depth']
            
            # Very simplified model - in reality this would be more complex
            drill_string_weight = depth * 0.02  # Assume 20 lbs per foot of drill string
            theoretical_hook_load = drill_string_weight
            
            if theoretical_hook_load > 0:
                result['drag_factor'] = min(1.0, max(0.1, hook_load / theoretical_hook_load))
            else:
                result['drag_factor'] = 0.1
        else:
            result['drag_factor'] = 0.1
        
        return result
        
    except Exception as e:
        logger.error(f"Error calculating derived parameters: {str(e)}")
        return data  # Return original data if calculation fails