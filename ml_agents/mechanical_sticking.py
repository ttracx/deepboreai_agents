import numpy as np
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MechanicalStickingAgent:
    """
    Physics-informed ML agent for detecting and predicting mechanical sticking.
    
    Mechanical sticking occurs when the drillstring gets physically stuck due to
    wellbore geometry issues, collapsed formation, or mechanical restrictions.
    """
    
    def __init__(self):
        # Initialize model parameters
        # In a full implementation, these would be trained parameters
        # For this demo, we'll use physics-based heuristics and thresholds
        self.model_parameters = {
            'torque_weight_ratio_threshold': 0.45,  # Torque to WOB ratio threshold
            'drag_factor_threshold': 1.3,  # Excessive drag threshold
            'rpm_fluctuation_threshold': 2.5,  # RPM standard deviation threshold
            'wob_fluctuation_threshold': 1.2,  # WOB standard deviation threshold
            'torque_spike_threshold': 1.5,  # Torque spike ratio threshold
            'torque_baseline_weight': 0.25,
            'drag_weight': 0.25,
            'fluctuation_weight': 0.2,
            'historical_weight': 0.3
        }
    
    def predict(self, drilling_data):
        """
        Predict the likelihood of mechanical sticking.
        
        Args:
            drilling_data (dict): Processed drilling data
            
        Returns:
            dict: Prediction results with probability and contributing factors
        """
        try:
            # Extract relevant drilling parameters
            wob = drilling_data.get('WOB', 0)
            torque = drilling_data.get('Torque', 0)
            rpm = drilling_data.get('RPM', 0)
            drag_factor = drilling_data.get('Drag_Factor', 1.0)
            
            # Feature 1: Torque to Weight on Bit ratio
            # Higher values indicate higher friction and potential sticking points
            torque_wob_ratio = torque / max(1, wob)  # Avoid division by zero
            torque_feature = min(1.0, torque_wob_ratio / self.model_parameters['torque_weight_ratio_threshold'])
            
            # Feature 2: Drag factor
            # Higher values indicate increased friction in the wellbore
            drag_feature = min(1.0, drag_factor / self.model_parameters['drag_factor_threshold'])
            
            # Feature 3: RPM and WOB fluctuations
            # High fluctuations can indicate impending sticking
            rpm_std = drilling_data.get('RPM_std', 0)
            wob_std = drilling_data.get('WOB_std', 0)
            
            rpm_fluctuation = min(1.0, rpm_std / self.model_parameters['rpm_fluctuation_threshold'])
            wob_fluctuation = min(1.0, wob_std / self.model_parameters['wob_fluctuation_threshold'])
            fluctuation_feature = (rpm_fluctuation + wob_fluctuation) / 2
            
            # Feature 4: Historical trends
            # Look for worsening conditions over time
            historical_feature = 0.0
            
            if 'time_series' in drilling_data:
                time_series = drilling_data['time_series']
                if 'Torque' in time_series and 'WOB' in time_series and len(time_series['Torque']) > 10:
                    # Calculate trend in torque/WOB ratio
                    recent_torque = np.array(time_series['Torque'][-10:])
                    recent_wob = np.array(time_series['WOB'][-10:])
                    recent_wob = np.where(recent_wob < 0.1, 0.1, recent_wob)  # Avoid division by zero
                    
                    recent_ratio = recent_torque / recent_wob
                    
                    # Simple trend detection
                    if len(recent_ratio) > 5:
                        early_avg = np.mean(recent_ratio[:5])
                        late_avg = np.mean(recent_ratio[-5:])
                        
                        # If increasing trend, higher likelihood of sticking
                        trend_factor = (late_avg - early_avg) / early_avg if early_avg > 0 else 0
                        historical_feature = min(1.0, max(0, trend_factor * 5))
            
            # Combine features with weighted average
            sticking_probability = (
                self.model_parameters['torque_baseline_weight'] * torque_feature +
                self.model_parameters['drag_weight'] * drag_feature +
                self.model_parameters['fluctuation_weight'] * fluctuation_feature +
                self.model_parameters['historical_weight'] * historical_feature
            )
            
            # Clip probability to [0, 1] range
            sticking_probability = min(1.0, max(0.0, sticking_probability))
            
            # Determine contributing factors for explanation
            contributing_factors = []
            
            if torque_feature > 0.6:
                contributing_factors.append("High torque to WOB ratio")
            
            if drag_feature > 0.6:
                contributing_factors.append("Elevated drag factor")
            
            if fluctuation_feature > 0.6:
                contributing_factors.append("Significant RPM and WOB fluctuations")
            
            if historical_feature > 0.6:
                contributing_factors.append("Worsening trend in torque/WOB ratio")
            
            # Generate recommendations based on sticking probability
            recommendations = self._generate_recommendations(sticking_probability, contributing_factors, drilling_data)
            
            # Prepare result dictionary
            result = {
                'probability': sticking_probability,
                'contributing_factors': contributing_factors,
                'recommendations': recommendations,
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            logger.info(f"Mechanical sticking prediction: {sticking_probability:.2f}")
            return result
        
        except Exception as e:
            logger.error(f"Error predicting mechanical sticking: {str(e)}")
            return {
                'probability': 0.0,
                'contributing_factors': ["Error in prediction model"],
                'recommendations': ["Check system logs for errors"],
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
    
    def _generate_recommendations(self, probability, factors, data):
        """Generate drilling recommendations based on prediction results"""
        recommendations = []
        
        if probability < 0.3:
            # Low risk - normal operations
            recommendations.append("Continue normal operations")
            recommendations.append("Monitor torque and drag trends")
        
        elif probability < 0.7:
            # Medium risk - preventive measures
            recommendations.append("Reduce WOB and RPM temporarily")
            recommendations.append("Perform controlled back-reaming to clean the wellbore")
            
            if "High torque to WOB ratio" in factors:
                recommendations.append("Consider adding lubricant to drilling fluid")
            
            if "Elevated drag factor" in factors:
                recommendations.append("Perform wiper trips to ensure wellbore is clean")
        
        else:
            # High risk - significant action required
            recommendations.append("Stop drilling and work pipe up and down")
            recommendations.append("Reduce drilling parameters to minimum")
            recommendations.append("Circulate to clean wellbore")
            
            if "Worsening trend in torque/WOB ratio" in factors:
                recommendations.append("Prepare contingency plan for stuck pipe")
            
            if "Significant RPM and WOB fluctuations" in factors:
                recommendations.append("Check for tight spots through controlled movement")
        
        return recommendations

# Create a singleton instance
agent = MechanicalStickingAgent()

def predict(drilling_data):
    """Wrapper function to call the agent's predict method"""
    return agent.predict(drilling_data)
