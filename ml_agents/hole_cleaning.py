"""
Hole cleaning prediction agent.

This module implements a machine learning agent for predicting hole cleaning issues.
"""

import logging
import numpy as np
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HoleCleaningAgent:
    """
    Agent for predicting hole cleaning issues during drilling operations.
    
    This agent analyzes drilling parameters to predict the likelihood of
    inadequate hole cleaning, which can lead to packoffs, stuck pipe,
    and other drilling problems.
    """
    
    def __init__(self, sensitivity=0.75):
        """
        Initialize the hole cleaning agent.
        
        Args:
            sensitivity (float): Sensitivity parameter (0-1) that affects
                the agent's prediction threshold. Higher values make the
                agent more sensitive to potential issues.
        """
        self.sensitivity = sensitivity
        logger.info(f"Initialized hole cleaning agent with sensitivity {sensitivity}")
    
    def predict(self, drilling_data):
        """
        Predict the likelihood of hole cleaning issues based on drilling data.
        
        Args:
            drilling_data (dict): Dictionary containing drilling parameters
            
        Returns:
            dict: Prediction results including probability and contributing factors
        """
        try:
            if not drilling_data:
                logger.warning("Empty drilling data provided to hole cleaning agent")
                return {
                    'probability': 0.0,
                    'contributing_factors': [],
                    'recommendations': []
                }
            
            # Extract relevant parameters
            hole_cleaning_index = drilling_data.get('hole_cleaning_index', 0)
            rop = drilling_data.get('ROP', 0)
            rpm = drilling_data.get('RPM', 0)
            flow_rate = drilling_data.get('Flow_Rate', 0)
            ecd = drilling_data.get('ECD', 0)
            torque = drilling_data.get('Torque', 0)
            depth = drilling_data.get('depth', 0)
            
            # Get trend data if available
            rop_change = drilling_data.get('ROP_change', 0)
            flow_rate_change = drilling_data.get('Flow_Rate_change', 0)
            
            # Apply physics-informed prediction model for hole cleaning issues
            
            # 1. Use hole cleaning index if available (direct measure of cleaning efficiency)
            if hole_cleaning_index > 0:
                # Invert since higher index means better cleaning (lower risk)
                base_probability = min(1.0, max(0.0, 1.0 - hole_cleaning_index))
            else:
                # Default moderate value if index not available
                base_probability = 0.5
            
            # 2. Analyze additional factors that contribute to hole cleaning issues
            
            # ROP factor - higher ROP produces more cuttings
            rop_factor = 0
            if rop > 0:
                # Normalize ROP (assuming 100 ft/hr as high risk threshold)
                rop_factor = min(1.0, max(0.0, rop / 100))
            
            # RPM factor - lower RPM reduces hole cleaning efficiency
            rpm_factor = 0
            if rpm > 0:
                # Inverse relationship - lower RPM increases risk
                rpm_factor = min(1.0, max(0.0, 1.0 - (rpm / 150)))
            
            # Flow rate factor - lower flow rate reduces hole cleaning
            flow_rate_factor = 0
            if flow_rate > 0:
                # Inverse relationship - lower flow rates increase risk
                flow_rate_factor = min(1.0, max(0.0, 1.0 - (flow_rate / 800)))
            
            # ECD factor - extreme ECDs (too high or too low) can affect cleaning
            ecd_factor = 0
            if ecd > 0:
                # Optimum ECD around 11-12 ppg (simplified model)
                ecd_deviation = abs(ecd - 11.5)
                ecd_factor = min(1.0, max(0.0, ecd_deviation / 3))
            
            # Hole angle factor - assumed horizontal if depth is high (simplified model)
            # In reality, this would use inclination data
            hole_angle_factor = 0
            if depth > 8000:  # Assume horizontal section after 8000 ft
                hole_angle_factor = 0.8  # Higher risk in horizontal sections
            else:
                hole_angle_factor = min(0.6, max(0.1, depth / 10000))  # Gradual increase with depth
            
            # 3. Combine factors with appropriate weights
            cleaning_probability = (
                0.3 * base_probability +
                0.2 * rop_factor +
                0.1 * rpm_factor +
                0.2 * flow_rate_factor +
                0.1 * ecd_factor +
                0.1 * hole_angle_factor
            )
            
            # Apply sensitivity adjustment
            cleaning_probability = min(1.0, cleaning_probability * (1.0 + (self.sensitivity - 0.5)))
            
            # 4. Adjust based on trends (increasing ROP or decreasing flow rate increases risk)
            if rop_change > 5:  # ROP increasing rapidly
                cleaning_probability = min(1.0, cleaning_probability + 0.1)
            
            if flow_rate_change < -20:  # Flow rate decreasing rapidly
                cleaning_probability = min(1.0, cleaning_probability + 0.1)
            
            # 5. Determine contributing factors
            contributing_factors = []
            recommendations = []
            
            # Add contributing factors in order of significance
            if hole_cleaning_index < 0.6 and hole_cleaning_index > 0:
                contributing_factors.append({
                    'factor': 'Low Hole Cleaning Index',
                    'value': f"{hole_cleaning_index:.2f}"
                })
                recommendations.append("Increase flow rate and pipe rotation to improve hole cleaning")
            
            if rop_factor > 0.7:
                contributing_factors.append({
                    'factor': 'High ROP',
                    'value': f"{rop:.1f} ft/hr"
                })
                recommendations.append("Reduce ROP to prevent excess cuttings generation")
            
            if flow_rate_factor > 0.6:
                contributing_factors.append({
                    'factor': 'Low Flow Rate',
                    'value': f"{flow_rate:.0f} gpm"
                })
                recommendations.append("Increase flow rate to improve cuttings removal")
            
            if rpm_factor > 0.6:
                contributing_factors.append({
                    'factor': 'Low RPM',
                    'value': f"{rpm:.0f} rpm"
                })
                recommendations.append("Increase rotary speed to improve hole cleaning")
            
            if ecd_factor > 0.6:
                contributing_factors.append({
                    'factor': 'Non-optimal ECD',
                    'value': f"{ecd:.2f} ppg"
                })
                recommendations.append("Adjust mud properties to optimize ECD")
            
            if hole_angle_factor > 0.7:
                contributing_factors.append({
                    'factor': 'High Hole Angle/Depth',
                    'value': f"{depth:.0f} ft"
                })
                recommendations.append("Increase flowrate and RPM in high-angle sections")
            
            # Add general recommendations if high probability
            if cleaning_probability > 0.7 and not recommendations:
                recommendations.append("Perform wiper trips to clean the hole")
                recommendations.append("Consider optimizing mud properties for better cuttings transport")
            
            # Prepare prediction result
            prediction = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'probability': cleaning_probability,
                'contributing_factors': contributing_factors,
                'recommendations': recommendations
            }
            
            logger.debug(f"Hole cleaning issue prediction: {cleaning_probability:.2f}")
            return prediction
            
        except Exception as e:
            logger.error(f"Error in hole cleaning prediction: {str(e)}")
            return {
                'probability': 0.0,
                'contributing_factors': [],
                'recommendations': ["Error in prediction model"]
            }