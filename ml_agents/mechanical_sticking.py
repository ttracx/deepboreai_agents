"""
Mechanical sticking prediction agent.

This module implements a machine learning agent for predicting mechanical sticking.
"""

import logging
import numpy as np
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MechanicalStickingAgent:
    """
    Agent for predicting mechanical sticking during drilling operations.
    
    This agent analyzes drilling parameters to predict the likelihood of
    mechanical sticking, which can occur due to various factors such as
    undergauge hole, keyseating, ledges, or wellbore instability.
    """
    
    def __init__(self, sensitivity=0.8):
        """
        Initialize the mechanical sticking agent.
        
        Args:
            sensitivity (float): Sensitivity parameter (0-1) that affects
                the agent's prediction threshold. Higher values make the
                agent more sensitive to potential sticking.
        """
        self.sensitivity = sensitivity
        logger.info(f"Initialized mechanical sticking agent with sensitivity {sensitivity}")
    
    def predict(self, drilling_data):
        """
        Predict the likelihood of mechanical sticking based on drilling data.
        
        Args:
            drilling_data (dict): Dictionary containing drilling parameters
            
        Returns:
            dict: Prediction results including probability and contributing factors
        """
        try:
            if not drilling_data:
                logger.warning("Empty drilling data provided to mechanical sticking agent")
                return {
                    'probability': 0.0,
                    'contributing_factors': [],
                    'recommendations': []
                }
            
            # Extract relevant parameters
            wob = drilling_data.get('WOB', 0)
            rpm = drilling_data.get('RPM', 0)
            torque = drilling_data.get('Torque', 0)
            flow_rate = drilling_data.get('Flow_Rate', 0)
            drag_factor = drilling_data.get('drag_factor', 0)
            
            # Check for rate of change parameters
            torque_change = drilling_data.get('Torque_change', 0)
            rpm_change = drilling_data.get('RPM_change', 0)
            
            # Get statistical metrics if available
            torque_avg = drilling_data.get('torque_avg', torque)
            torque_std = drilling_data.get('torque_std', 0)
            rpm_avg = drilling_data.get('rpm_avg', rpm)
            rpm_std = drilling_data.get('rpm_std', 0)
            
            # Apply physics-informed prediction model for mechanical sticking
            
            # 1. Calculate base probability from drag factor
            # Higher drag factor indicates greater friction and potential for sticking
            base_probability = min(1.0, max(0.0, drag_factor * 0.8))
            
            # 2. Analyze torque and RPM patterns for mechanical sticking signatures
            
            # Normalized torque (how much current torque deviates from average)
            if torque_avg > 0 and torque_std > 0:
                torque_factor = (torque - torque_avg) / (torque_std + 1)  # +1 to avoid division by zero
            else:
                torque_factor = 0
                
            # High torque is a risk factor
            torque_risk = min(1.0, max(0.0, 0.3 + 0.7 * (torque_factor if torque_factor > 0 else 0)))
            
            # Torque instability (rapid changes) indicates potential sticking
            torque_instability = min(1.0, abs(torque_change) / (torque_avg * 0.2 + 0.1))
            
            # RPM instability
            if rpm_avg > 0 and rpm_std > 0:
                rpm_instability = min(1.0, abs(rpm_change) / (rpm_avg * 0.2 + 0.1))
            else:
                rpm_instability = 0
            
            # 3. Combine factors with appropriate weights
            sticking_probability = (
                0.35 * base_probability +
                0.25 * torque_risk +
                0.25 * torque_instability +
                0.15 * rpm_instability
            )
            
            # Apply sensitivity adjustment
            sticking_probability = min(1.0, sticking_probability * (1.0 + (self.sensitivity - 0.5)))
            
            # 4. Determine contributing factors
            contributing_factors = []
            recommendations = []
            
            # Add contributing factors in order of significance
            if drag_factor > 0.6:
                contributing_factors.append({
                    'factor': 'High Drag Factor',
                    'value': f"{drag_factor:.2f}"
                })
                recommendations.append("Work pipe to reduce drag and consider lubricant additives to mud")
            
            if torque_risk > 0.5:
                contributing_factors.append({
                    'factor': 'Elevated Torque',
                    'value': f"{torque:.1f} kft-lbs"
                })
                recommendations.append("Reduce weight on bit (WOB) to decrease torque")
            
            if torque_instability > 0.6:
                contributing_factors.append({
                    'factor': 'Torque Instability',
                    'value': f"{torque_change:.2f} kft-lbs/min"
                })
                recommendations.append("Stabilize drilling parameters and check for formation changes")
            
            if rpm_instability > 0.6:
                contributing_factors.append({
                    'factor': 'RPM Instability',
                    'value': f"{rpm_change:.1f} RPM/min"
                })
                recommendations.append("Stabilize rotary speed and check for possible vibrations")
            
            # If flow rate is low, it could contribute to poor hole cleaning
            if flow_rate < 400 and sticking_probability > 0.4:
                contributing_factors.append({
                    'factor': 'Low Flow Rate',
                    'value': f"{flow_rate:.0f} gpm"
                })
                recommendations.append("Increase flow rate to improve hole cleaning")
            
            # If WOB is high, it could contribute to sticking risk
            if wob > 30 and sticking_probability > 0.4:
                contributing_factors.append({
                    'factor': 'High WOB',
                    'value': f"{wob:.1f} klbs"
                })
                recommendations.append("Reduce weight on bit (WOB) to decrease mechanical sticking risk")
            
            # Add general recommendations if high probability
            if sticking_probability > 0.7 and not recommendations:
                recommendations.append("Perform slack-off and pick-up tests to check for potential sticking points")
                recommendations.append("Consider working the pipe and reaming to clean the hole")
            
            # Prepare prediction result
            prediction = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'probability': sticking_probability,
                'contributing_factors': contributing_factors,
                'recommendations': recommendations
            }
            
            logger.debug(f"Mechanical sticking prediction: {sticking_probability:.2f}")
            return prediction
            
        except Exception as e:
            logger.error(f"Error in mechanical sticking prediction: {str(e)}")
            return {
                'probability': 0.0,
                'contributing_factors': [],
                'recommendations': ["Error in prediction model"]
            }