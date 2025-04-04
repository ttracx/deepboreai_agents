import numpy as np
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DifferentialStickingAgent:
    """
    Physics-informed ML agent for detecting and predicting differential sticking.
    
    Differential sticking occurs when the drillstring becomes embedded in the filter 
    cake on the low side of the wellbore due to the difference between hydrostatic 
    and formation pressure.
    """
    
    def __init__(self):
        # Initialize model parameters based on physics principles
        self.model_parameters = {
            'diff_pressure_threshold': 600,  # psi threshold for high differential pressure
            'stationary_time_threshold': 15,  # minutes of stationary time risk
            'ecd_to_pp_ratio_threshold': 1.25,  # ECD to pore pressure ratio threshold
            'filter_cake_indicator_threshold': 0.6,  # filter cake buildup indicator
            'diff_pressure_weight': 0.35,
            'stationary_weight': 0.25,
            'wellbore_geometry_weight': 0.15,
            'filter_cake_weight': 0.25
        }
    
    def predict(self, drilling_data):
        """
        Predict the likelihood of differential sticking.
        
        Args:
            drilling_data (dict): Processed drilling data
            
        Returns:
            dict: Prediction results with probability and contributing factors
        """
        try:
            # Extract relevant drilling parameters
            differential_pressure = drilling_data.get('differential_pressure', 0)
            ecd = drilling_data.get('ECD', 12.5)
            rpm = drilling_data.get('RPM', 0)
            flow_rate = drilling_data.get('Flow_Rate', 0)
            
            # Feature 1: Differential pressure risk
            # Higher differential pressure increases sticking risk
            diff_pressure_feature = min(1.0, differential_pressure / self.model_parameters['diff_pressure_threshold'])
            
            # Feature 2: Stationary time risk
            # Longer stationary time increases sticking risk due to filter cake buildup
            stationary_feature = 0.0
            
            if 'time_series' in drilling_data and 'RPM' in drilling_data['time_series']:
                # Check for periods of low/no rotation
                rpm_series = drilling_data['time_series']['RPM']
                if len(rpm_series) > 5:
                    # Count consecutive low RPM readings (stationary)
                    stationary_count = 0
                    for rpm_val in rpm_series[-10:]:  # Check last 10 readings
                        if rpm_val < 5:  # Effectively stationary
                            stationary_count += 1
                        else:
                            # Reset count if rotation detected
                            stationary_count = 0
                    
                    # Convert count to minutes (assuming 1-minute data intervals)
                    stationary_minutes = stationary_count
                    stationary_feature = min(1.0, stationary_minutes / self.model_parameters['stationary_time_threshold'])
            
            # Feature 3: Wellbore geometry risk
            # Horizontal and high-angle wells have higher risk
            # Assume a simple model where depth approximately represents inclination
            wellbore_feature = 0.5  # Default medium risk
            
            # Feature 4: Filter cake buildup risk
            # Low flow rates and high solid content increase filter cake buildup
            filter_cake_feature = 0.0
            
            # Simple filter cake model based on flow rate
            # Lower flow rates lead to more filter cake buildup
            if flow_rate > 0:
                flow_factor = min(1.0, 500 / flow_rate)  # Normalize with 500 gpm as reference
                filter_cake_feature = flow_factor * 0.7  # Scale to reasonable range
            
            # Add ECD influence on filter cake
            if ecd > 10:
                ecd_factor = min(1.0, (ecd - 10) / 4)  # ECD above 10 increases risk
                filter_cake_feature = max(filter_cake_feature, ecd_factor)
            
            # Combine features with weighted average
            sticking_probability = (
                self.model_parameters['diff_pressure_weight'] * diff_pressure_feature +
                self.model_parameters['stationary_weight'] * stationary_feature +
                self.model_parameters['wellbore_geometry_weight'] * wellbore_feature +
                self.model_parameters['filter_cake_weight'] * filter_cake_feature
            )
            
            # Clip probability to [0, 1] range
            sticking_probability = min(1.0, max(0.0, sticking_probability))
            
            # Determine contributing factors for explanation
            contributing_factors = []
            
            if diff_pressure_feature > 0.6:
                contributing_factors.append("High differential pressure")
            
            if stationary_feature > 0.5:
                contributing_factors.append("Extended stationary time")
            
            if wellbore_feature > 0.7:
                contributing_factors.append("High wellbore inclination")
            
            if filter_cake_feature > 0.6:
                contributing_factors.append("Significant filter cake buildup potential")
            
            # Generate recommendations based on sticking probability
            recommendations = self._generate_recommendations(sticking_probability, contributing_factors, drilling_data)
            
            # Prepare result dictionary
            result = {
                'probability': sticking_probability,
                'contributing_factors': contributing_factors,
                'recommendations': recommendations,
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            logger.info(f"Differential sticking prediction: {sticking_probability:.2f}")
            return result
        
        except Exception as e:
            logger.error(f"Error predicting differential sticking: {str(e)}")
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
            recommendations.append("Monitor ECD and differential pressure")
        
        elif probability < 0.7:
            # Medium risk - preventive measures
            recommendations.append("Reduce stationary time")
            recommendations.append("Maintain pipe movement when circulation is stopped")
            
            if "High differential pressure" in factors:
                recommendations.append("Consider reducing mud weight if safe to do so")
            
            if "Significant filter cake buildup potential" in factors:
                recommendations.append("Increase flow rate to reduce filter cake buildup")
                recommendations.append("Consider adjusting drilling fluid properties")
        
        else:
            # High risk - significant action required
            recommendations.append("Implement continuous pipe rotation")
            recommendations.append("Minimize connections and stationary time")
            recommendations.append("Monitor torque closely for early signs of sticking")
            
            if "High differential pressure" in factors:
                recommendations.append("Optimize mud weight to minimize differential pressure")
                recommendations.append("Consider managed pressure drilling techniques")
            
            if "Extended stationary time" in factors:
                recommendations.append("Implement work pipe protocol during connections")
        
        return recommendations

# Create a singleton instance
agent = DifferentialStickingAgent()

def predict(drilling_data):
    """Wrapper function to call the agent's predict method"""
    return agent.predict(drilling_data)
