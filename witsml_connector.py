import requests
import xml.etree.ElementTree as ET
import logging
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from io import StringIO
import random  # For demo purposes only

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
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({
            'Content-Type': 'text/xml;charset=UTF-8'
        })
        
    def get_version(self):
        """Get the WITSML server version"""
        try:
            template = """<?xml version="1.0" encoding="UTF-8"?>
            <WMLS:GetVersion xmlns:WMLS="http://www.witsml.org/schemas/1series"/>"""
            
            response = self.session.post(self.url, data=template)
            response.raise_for_status()
            
            return response.text.strip()
        except Exception as e:
            logger.error(f"Error getting WITSML version: {str(e)}")
            return None
    
    def get_cap(self):
        """Get the capabilities of the WITSML server"""
        try:
            template = """<?xml version="1.0" encoding="UTF-8"?>
            <WMLS:GetCap xmlns:WMLS="http://www.witsml.org/schemas/1series"/>"""
            
            response = self.session.post(self.url, data=template)
            response.raise_for_status()
            
            return response.text
        except Exception as e:
            logger.error(f"Error getting WITSML capabilities: {str(e)}")
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
        try:
            # If no log_uid is provided, get the most recent log
            if not log_uid:
                logs = self.get_logs(well_uid, wellbore_uid)
                if not logs:
                    logger.error("No logs found")
                    return None
                log_uid = logs[0]['uid']  # Assume first log is most recent
            
            # Build the query
            query_template = """<?xml version="1.0" encoding="UTF-8"?>
            <WMLS:GetFromStore xmlns:WMLS="http://www.witsml.org/schemas/1series">
                <WMLS:WMLtypeIn>log</WMLS:WMLtypeIn>
                <WMLS:QueryIn>
                    <logs xmlns="http://www.witsml.org/schemas/1series" version="1.4.1.1">
                        <log uidWell="{well_uid}" uidWellbore="{wellbore_uid}" uid="{log_uid}">
                            <dataRowCount></dataRowCount>
                            <startIndex>{start_index}</startIndex>
                            <endIndex>{end_index}</endIndex>
                            <logData>true</logData>
                        </log>
                    </logs>
                </WMLS:QueryIn>
                <WMLS:OptionsIn>returnElements=all</WMLS:OptionsIn>
            </WMLS:GetFromStore>"""
            
            query = query_template.format(
                well_uid=well_uid,
                wellbore_uid=wellbore_uid,
                log_uid=log_uid,
                start_index=start_index if start_index else "",
                end_index=end_index if end_index else ""
            )
            
            response = self.session.post(self.url, data=query)
            response.raise_for_status()
            
            # Parse the XML response
            root = ET.fromstring(response.text)
            
            # Check for success
            result = root.find('.//WMLS:Result', {'WMLS': 'http://www.witsml.org/schemas/1series'})
            if result is None or result.text != "1":
                error = root.find('.//WMLS:SuppMsgOut', {'WMLS': 'http://www.witsml.org/schemas/1series'})
                logger.error(f"Error getting log data: {error.text if error is not None else 'Unknown error'}")
                return None
            
            # Extract data from XML response
            logs_data = {}
            
            # Get log data and parse it
            log_data = root.find('.//{http://www.witsml.org/schemas/1series}logData')
            if log_data is not None:
                # Get mnemonics
                mnemonics_element = log_data.find('.//{http://www.witsml.org/schemas/1series}mnemonicList')
                if mnemonics_element is not None:
                    mnemonics = mnemonics_element.text.split(',')
                    logs_data['mnemonics'] = mnemonics
                
                # Get units
                units_element = log_data.find('.//{http://www.witsml.org/schemas/1series}unitList')
                if units_element is not None:
                    units = units_element.text.split(',')
                    logs_data['units'] = units
                
                # Get data rows
                data_rows = []
                for data_row in log_data.findall('.//{http://www.witsml.org/schemas/1series}data'):
                    if data_row.text:
                        values = data_row.text.split(',')
                        data_rows.append(values)
                
                logs_data['data'] = data_rows
            
            return logs_data
        
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
        try:
            query_template = """<?xml version="1.0" encoding="UTF-8"?>
            <WMLS:GetFromStore xmlns:WMLS="http://www.witsml.org/schemas/1series">
                <WMLS:WMLtypeIn>log</WMLS:WMLtypeIn>
                <WMLS:QueryIn>
                    <logs xmlns="http://www.witsml.org/schemas/1series" version="1.4.1.1">
                        <log uidWell="{well_uid}" uidWellbore="{wellbore_uid}">
                        </log>
                    </logs>
                </WMLS:QueryIn>
                <WMLS:OptionsIn>returnElements=all</WMLS:OptionsIn>
            </WMLS:GetFromStore>"""
            
            query = query_template.format(
                well_uid=well_uid,
                wellbore_uid=wellbore_uid
            )
            
            response = self.session.post(self.url, data=query)
            response.raise_for_status()
            
            # Parse the XML response
            root = ET.fromstring(response.text)
            
            # Check for success
            result = root.find('.//WMLS:Result', {'WMLS': 'http://www.witsml.org/schemas/1series'})
            if result is None or result.text != "1":
                error = root.find('.//WMLS:SuppMsgOut', {'WMLS': 'http://www.witsml.org/schemas/1series'})
                logger.error(f"Error getting logs: {error.text if error is not None else 'Unknown error'}")
                return None
            
            # Extract logs information
            logs_info = []
            
            log_elements = root.findall('.//{http://www.witsml.org/schemas/1series}log')
            for log_element in log_elements:
                log_info = {
                    'uid': log_element.get('uid'),
                    'name': self._get_element_text(log_element, './/{http://www.witsml.org/schemas/1series}name'),
                    'indexType': self._get_element_text(log_element, './/{http://www.witsml.org/schemas/1series}indexType'),
                    'startIndex': self._get_element_text(log_element, './/{http://www.witsml.org/schemas/1series}startIndex'),
                    'endIndex': self._get_element_text(log_element, './/{http://www.witsml.org/schemas/1series}endIndex'),
                    'direction': self._get_element_text(log_element, './/{http://www.witsml.org/schemas/1series}direction'),
                    'objectGrowing': self._get_element_text(log_element, './/{http://www.witsml.org/schemas/1series}objectGrowing')
                }
                logs_info.append(log_info)
            
            return logs_info
        
        except Exception as e:
            logger.error(f"Error getting logs: {str(e)}")
            return None
    
    def _get_element_text(self, parent, xpath):
        """Helper method to get element text or None if the element doesn't exist"""
        element = parent.find(xpath)
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
        # For demo purposes, we'll just check if the configuration looks valid
        if not all([config['url'], config['username'], config['password'], config['well_uid'], config['wellbore_uid']]):
            logger.warning("One or more required connection parameters are missing")
            return False
        
        # Try to create a WITSML client and get the version
        client = WitsmlClient(config['url'], config['username'], config['password'])
        version = client.get_version()
        
        if version:
            logger.info(f"Connected to WITSML server, version: {version}")
            return True
        else:
            return False
            
    except Exception as e:
        logger.error(f"Error testing connection: {str(e)}")
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
        # For demonstration purposes, we'll simulate drilling data
        # In a real implementation, this would connect to the actual WITSML server
        
        # Create simulated data
        now = datetime.now()
        
        # Generate realistic drilling parameters with some variation
        # Use random.gauss to create normally distributed values around a mean
        data = {
            'timestamp': now.strftime("%Y-%m-%d %H:%M:%S"),
            'depth': 10000 + random.uniform(0, 5),  # Current depth in feet
            'WOB': random.gauss(25, 2),  # Weight on bit in klbs
            'ROP': random.gauss(50, 5),  # Rate of penetration in ft/hr
            'RPM': random.gauss(120, 10),  # Rotary speed
            'Torque': random.gauss(10, 1),  # Torque in kft-lbs
            'SPP': random.gauss(3500, 100),  # Standpipe pressure in psi
            'Flow_Rate': random.gauss(600, 20),  # Flow rate in gpm
            'ECD': random.gauss(12.5, 0.2),  # Equivalent circulating density in ppg
            'Hook_Load': random.gauss(200, 10),  # Hook load in klbs
            'MSE': random.gauss(35000, 2000),  # Mechanical specific energy in psi
        }
        
        # Calculate change from previous reading (simulate)
        data['WOB_change'] = random.uniform(-0.5, 0.5)
        data['ROP_change'] = random.uniform(-2, 2)
        data['RPM_change'] = random.uniform(-5, 5)
        data['Torque_change'] = random.uniform(-0.3, 0.3)
        data['SPP_change'] = random.uniform(-50, 50)
        data['Flow_Rate_change'] = random.uniform(-10, 10)
        
        # Add time series data (last hour with 1-minute intervals)
        # In a real implementation, this would be fetched from the WITSML server
        data['time_series'] = generate_time_series(now, 60)  # 60 minutes of data
        
        logger.info(f"Fetched data at {data['timestamp']}")
        return data
    
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        return None


def generate_time_series(end_time, minutes):
    """
    Generate time series data for demonstration.
    
    Args:
        end_time (datetime): The end time for the series
        minutes (int): Number of minutes to generate
        
    Returns:
        dict: Time series data with timestamp and parameter values
    """
    # Create time points at 1-minute intervals
    timestamps = [end_time - timedelta(minutes=i) for i in range(minutes)]
    timestamps.reverse()  # Oldest first
    
    # Create a dictionary to hold the time series data
    time_series = {
        'timestamp': timestamps,
        'WOB': [],
        'ROP': [],
        'RPM': [],
        'Torque': [],
        'SPP': [],
        'Flow_Rate': [],
        'ECD': [],
        'MSE': []
    }
    
    # Generate realistic drilling parameter trends
    # Use a combination of trend, cyclical patterns, and random noise
    
    # Base values
    wob_base = 25
    rop_base = 50
    rpm_base = 120
    torque_base = 10
    spp_base = 3500
    flow_base = 600
    ecd_base = 12.5
    mse_base = 35000
    
    # Trends (slight increase or decrease over time)
    wob_trend = 0.01
    rop_trend = 0.05
    rpm_trend = 0
    torque_trend = 0.005
    spp_trend = 0.5
    flow_trend = 0
    ecd_trend = 0.001
    mse_trend = 10
    
    for i, _ in enumerate(timestamps):
        # Add trend component
        wob = wob_base + wob_trend * i
        rop = rop_base + rop_trend * i
        rpm = rpm_base + rpm_trend * i
        torque = torque_base + torque_trend * i
        spp = spp_base + spp_trend * i
        flow = flow_base + flow_trend * i
        ecd = ecd_base + ecd_trend * i
        mse = mse_base + mse_trend * i
        
        # Add cyclical component (sine wave)
        wob += 1.5 * np.sin(i / 10)
        rop += 3.0 * np.sin(i / 15)
        rpm += 8.0 * np.sin(i / 20)
        torque += 0.8 * np.sin(i / 12)
        spp += 80 * np.sin(i / 18)
        flow += 15 * np.sin(i / 25)
        ecd += 0.1 * np.sin(i / 30)
        mse += 1500 * np.sin(i / 22)
        
        # Add random noise
        wob += random.gauss(0, 0.3)
        rop += random.gauss(0, 1.0)
        rpm += random.gauss(0, 2.0)
        torque += random.gauss(0, 0.2)
        spp += random.gauss(0, 20)
        flow += random.gauss(0, 5)
        ecd += random.gauss(0, 0.05)
        mse += random.gauss(0, 500)
        
        # Ensure values are physically plausible
        wob = max(5, min(40, wob))
        rop = max(10, min(100, rop))
        rpm = max(60, min(180, rpm))
        torque = max(5, min(15, torque))
        spp = max(2500, min(4500, spp))
        flow = max(400, min(800, flow))
        ecd = max(11.5, min(13.5, ecd))
        mse = max(25000, min(45000, mse))
        
        # Add to time series
        time_series['WOB'].append(wob)
        time_series['ROP'].append(rop)
        time_series['RPM'].append(rpm)
        time_series['Torque'].append(torque)
        time_series['SPP'].append(spp)
        time_series['Flow_Rate'].append(flow)
        time_series['ECD'].append(ecd)
        time_series['MSE'].append(mse)
    
    return time_series
