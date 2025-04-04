"""
Differential sticking prediction agent.

This module implements a machine learning agent for predicting differential sticking.
"""

import logging
import numpy as np
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DifferentialStickingAgent:
    """
    Agent for predicting differential sticking during drilling operations.
    
    This agent analyzes drilling parameters to predict the likelihood of
    differential sticking, which occurs when the drill string becomes 
    embedded in a filter cake due to differential pressure between the 
    wellbore and the formation.
    """
    
    def __init__(self, sensitivity=0.7):
        """
        Initialize the differential sticking agent.
        
        Args:
            sensitivity (float): Sensitivity parameter (0-1) that affects
                the agent's prediction threshold. Higher values make the
                agent more sensitive to potential sticking.
        """
        self.sensitivity = sensitivity
        logger.info(f"Initialized differential sticking agent with sensitivity {sensitivity}")
    
    def predict(self, drilling_data):
        """
        Predict the likelihood of differential sticking based on drilling data.
        
        Args:
            drilling_data (dict): Dictionary containing drilling parameters
            
        Returns:
            dict: Prediction results including probability and contributing factors
        """
        try:
            if not drilling_data:
                logger.warning("Empty drilling data provided to differential sticking agent")
                return {
                    'probability': 0.0,
                    'contributing_factors': [],
                    'recommendations': []
                }
            
            # Extract relevant parameters
            differential_pressure = drilling_data.get('differential_pressure', 0)
            ecd = drilling_data.get('ECD', 0)
            wob = drilling_data.get('WOB', 0)
            depth = drilling_data.get('depth', 0)
            hook_load = drilling_data.get('hook_load', 0)
            flow_rate = drilling_data.get('Flow_Rate', 0)
            
            # Apply physics-informed prediction model for differential sticking
            
            # 1. Calculate base probability from differential pressure
            # Higher differential pressure increases risk of differential sticking
            # Normalize differential pressure (assuming 1000 psi as high risk threshold)
            diff_pressure_normalized = min(1.0, differential_pressure / 1000)
            base_probability = min(1.0, max(0.0, diff_pressure_normalized * 0.8))
            
            # 2. Analyze additional factors that contribute to differential sticking
            
            # Equivalent Circulating Density (ECD) - higher values increase risk
            ecd_factor = 0
            if ecd > 0:
                # Normalize ECD (assuming 13 ppg as high risk threshold)
                ecd_factor = min(1.0, max(0.0, (ecd - 10) / 3))
            
            # Low flow rate increases risk (poor filter cake management)
            flow_rate_factor = 0
            if flow_rate > 0:
                # Inverse relationship - lower flow rates increase risk
                flow_rate_factor = min(1.0, max(0.0, 1.0 - (flow_rate / 800)))
            
            # Stationary time factor - assumed from hook load and WOB relationship
            # When hook load is low compared to depth-based theoretical weight, it might indicate stationary pipe
            stationary_factor = 0
            if depth > 0 and hook_load > 0:
                # Theoretical string weight based on depth (simplified model)
                theoretical_weight = depth * 0.02  # Assume 20 lbs per foot
                
                if theoretical_weight > 0:
                    weight_ratio = hook_load / theoretical_weight
                    # If ratio is low, more weight is transferred to formation (increased sticking risk)
                    stationary_factor = min(1.0, max(0.0, 1.0 - weight_ratio))
            
            # 3. Combine factors with appropriate weights
            sticking_probability = (
                0.4 * base_probability +
                0.3 * ecd_factor +
                0.2 * flow_rate_factor +
                0.1 * stationary_factor
            )
            
            # Apply sensitivity adjustment
            sticking_probability = min(1.0, sticking_probability * (1.0 + (self.sensitivity - 0.5)))
            
            # 4. Determine contributing factors
            contributing_factors = []
            recommendations = []
            
            # Add contributing factors in order of significance
            if differential_pressure > 500:
                contributing_factors.append({
                    'factor': 'High Differential Pressure',
                    'value': f"{differential_pressure:.0f} psi"
                })
                recommendations.append("Reduce mud weight to decrease differential pressure")
            
            if ecd_factor > 0.5:
                contributing_factors.append({
                    'factor': 'High ECD',
                    'value': f"{ecd:.2f} ppg"
                })
                recommendations.append("Reduce ECD by adjusting mud properties or reducing pump rate")
            
            if flow_rate_factor > 0.6:
                contributing_factors.append({
                    'factor': 'Low Flow Rate',
                    'value': f"{flow_rate:.0f} gpm"
                })
                recommendations.append("Increase flow rate to improve filter cake management")
            
            if stationary_factor > 0.5:
                contributing_factors.append({
                    'factor': 'Extended Stationary Time',
                    'value': "Detected"
                })
                recommendations.append("Keep pipe moving to prevent embedment in filter cake")
            
            # Add general recommendations if high probability
            if sticking_probability > 0.7 and not recommendations:
                recommendations.append("Monitor for signs of differential sticking: overpull, high torque, no reciprocation")
                recommendations.append("Consider reducing mud weight and keeping pipe moving")
            
            # Prepare prediction result
            prediction = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'probability': sticking_probability,
                'contributing_factors': contributing_factors,
                'recommendations': recommendations
            }
            
            logger.debug(f"Differential sticking prediction: {sticking_probability:.2f}")
            return prediction
            
        except Exception as e:
            logger.error(f"Error in differential sticking prediction: {str(e)}")
            return {
                'probability': 0.0,
                'contributing_factors': [],
                'recommendations': ["Error in prediction model"]
            }