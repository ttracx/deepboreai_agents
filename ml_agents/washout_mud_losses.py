"""
Washout and mud losses prediction agent.

This module implements a machine learning agent for predicting washouts and mud losses.
"""

import logging
import numpy as np
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WashoutMudLossesAgent:
    """
    Agent for predicting washouts and mud losses during drilling operations.
    
    This agent analyzes drilling parameters to predict the likelihood of
    washouts (damage to drilling equipment) and mud losses (loss of drilling
    fluid to the formation).
    """
    
    def __init__(self, sensitivity=0.8):
        """
        Initialize the washout and mud losses agent.
        
        Args:
            sensitivity (float): Sensitivity parameter (0-1) that affects
                the agent's prediction threshold. Higher values make the
                agent more sensitive to potential issues.
        """
        self.sensitivity = sensitivity
        logger.info(f"Initialized washout and mud losses agent with sensitivity {sensitivity}")
    
    def predict(self, drilling_data):
        """
        Predict the likelihood of washouts and mud losses based on drilling data.
        
        Args:
            drilling_data (dict): Dictionary containing drilling parameters
            
        Returns:
            dict: Prediction results including probability, issue type, and contributing factors
        """
        try:
            if not drilling_data:
                logger.warning("Empty drilling data provided to washout/mud losses agent")
                return {
                    'probability': 0.0,
                    'issue_type': 'Unknown',
                    'contributing_factors': [],
                    'recommendations': []
                }
            
            # Extract relevant parameters
            spp = drilling_data.get('SPP', 0)
            flow_rate = drilling_data.get('Flow_Rate', 0)
            torque = drilling_data.get('Torque', 0)
            rpm = drilling_data.get('RPM', 0)
            ecd = drilling_data.get('ECD', 0)
            depth = drilling_data.get('depth', 0)
            
            # Get trend data if available
            spp_change = drilling_data.get('SPP_change', 0)
            flow_rate_change = drilling_data.get('Flow_Rate_change', 0)
            torque_change = drilling_data.get('Torque_change', 0)
            
            # Get statistical data if available
            spp_avg = drilling_data.get('spp_avg', spp)
            spp_std = drilling_data.get('spp_std', 0)
            flow_rate_avg = drilling_data.get('flow_rate_avg', flow_rate)
            flow_rate_std = drilling_data.get('flow_rate_std', 0)
            torque_avg = drilling_data.get('torque_avg', torque)
            torque_std = drilling_data.get('torque_std', 0)
            
            # Apply physics-informed prediction models for washouts and mud losses
            
            # ---- Washout Detection ----
            
            # 1. Analyze pressure and flow rate for washout signatures
            
            # Sudden drop in standpipe pressure (main washout indicator)
            spp_drop_factor = 0
            if spp_change < 0 and spp_avg > 0:
                # Normalize pressure drop
                spp_drop_factor = min(1.0, max(0.0, abs(spp_change) / (spp_avg * 0.1)))
            
            # Unexpected relationship between flow rate and pressure
            # In a washout, pressure drops more than expected for a flow rate change
            flow_pressure_anomaly = 0
            if spp_change < 0 and flow_rate_change > 0:
                # Flow increasing but pressure decreasing is an anomaly
                flow_pressure_anomaly = min(1.0, max(0.0, (flow_rate_change / 20) * abs(spp_change / 100)))
            
            # Torque instability can indicate washout
            torque_instability = 0
            torque_contribution = 0
            if torque_avg > 0:
                torque_contribution = abs(torque_change) / (torque_avg * 0.2 + 0.1)
                torque_instability = min(1.0, max(0.0, torque_contribution))
            
            # Combine washout indicators
            washout_probability = (
                0.5 * spp_drop_factor +
                0.3 * flow_pressure_anomaly +
                0.2 * torque_instability
            )
            
            # Apply sensitivity adjustment
            washout_probability = min(1.0, washout_probability * (1.0 + (self.sensitivity - 0.5)))
            
            # ---- Mud Loss Detection ----
            
            # 1. Analyze flow rate and pressure for mud loss signatures
            
            # Flow rate decrease (main mud loss indicator)
            flow_loss_factor = 0
            flow_contribution = 0
            if flow_rate_change < 0 and flow_rate_avg > 0:
                flow_contribution = abs(flow_rate_change) / (flow_rate_avg * 0.1)
                flow_loss_factor = min(1.0, max(0.0, flow_contribution))
            
            # Pressure decrease with flow rate decrease
            pressure_flow_correlation = 0
            flow_rate_contribution = 0
            if spp_change < 0 and flow_rate_change < 0:
                flow_rate_contribution = abs(flow_rate_change) / (flow_rate_avg * 0.1 + 0.1)
                pressure_flow_correlation = min(1.0, max(0.0, flow_rate_contribution * abs(spp_change / 100)))
            
            # High ECD can contribute to mud losses
            ecd_factor = 0
            if ecd > 12:  # Threshold for higher risk
                ecd_factor = min(1.0, max(0.0, (ecd - 12) / 3))
            
            # Combine mud loss indicators
            mud_loss_probability = (
                0.4 * flow_loss_factor +
                0.3 * pressure_flow_correlation +
                0.3 * ecd_factor
            )
            
            # Apply sensitivity adjustment
            mud_loss_probability = min(1.0, mud_loss_probability * (1.0 + (self.sensitivity - 0.5)))
            
            # Determine which issue has higher probability
            issue_type = "Washout" if washout_probability > mud_loss_probability else "Mud Losses"
            issue_probability = max(washout_probability, mud_loss_probability)
            
            # Determine contributing factors and recommendations
            contributing_factors = []
            recommendations = []
            
            if issue_type == "Washout":
                # Add washout-specific contributing factors
                if spp_drop_factor > 0.5:
                    contributing_factors.append({
                        'factor': 'Standpipe Pressure Drop',
                        'value': f"{spp_change:.0f} psi"
                    })
                    recommendations.append("Monitor for surface pressure fluctuations")
                
                if flow_pressure_anomaly > 0.5:
                    contributing_factors.append({
                        'factor': 'Flow-Pressure Anomaly',
                        'value': "Detected"
                    })
                    recommendations.append("Check for inconsistent flow and pressure relationships")
                
                if torque_instability > 0.5 and torque_contribution > 0:
                    contributing_factors.append({
                        'factor': 'Torque Instability',
                        'value': f"{torque_change:.2f} kft-lbs/min"
                    })
                    recommendations.append("Watch for erratic torque behavior")
                
                # Add general washout recommendations
                if not recommendations:
                    recommendations.append("Perform flow check to confirm washout")
                    recommendations.append("Prepare to pull out of hole if washout confirmed")
            else:
                # Add mud loss-specific contributing factors
                if flow_loss_factor > 0.5:
                    contributing_factors.append({
                        'factor': 'Flow Return Decrease',
                        'value': f"{flow_rate_change:.0f} gpm"
                    })
                    recommendations.append("Monitor pit volume and flow returns closely")
                
                if pressure_flow_correlation > 0.5:
                    contributing_factors.append({
                        'factor': 'Pressure-Flow Correlation',
                        'value': "Detected"
                    })
                    recommendations.append("Check for simultaneous pressure and flow decreases")
                
                if ecd_factor > 0.5:
                    contributing_factors.append({
                        'factor': 'High ECD',
                        'value': f"{ecd:.2f} ppg"
                    })
                    recommendations.append("Consider reducing mud weight or ECD")
                
                # Add general mud loss recommendations
                if not recommendations:
                    recommendations.append("Perform flow check to confirm losses")
                    recommendations.append("Prepare loss circulation material (LCM) if losses confirmed")
            
            # Prepare prediction result
            prediction = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'probability': issue_probability,
                'issue_type': issue_type,
                'contributing_factors': contributing_factors,
                'recommendations': recommendations
            }
            
            logger.debug(f"{issue_type} prediction: {issue_probability:.2f}")
            return prediction
            
        except Exception as e:
            logger.error(f"Error in washout/mud losses prediction: {str(e)}")
            return {
                'probability': 0.0,
                'issue_type': 'Unknown',
                'contributing_factors': [],
                'recommendations': ["Error in prediction model"]
            }