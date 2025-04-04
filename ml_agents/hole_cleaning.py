import numpy as np
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HoleCleaningAgent:
    """
    Physics-informed ML agent for detecting and predicting hole cleaning issues.
    
    Hole cleaning issues occur when cuttings are not efficiently removed from the wellbore,
    leading to increased ECD, pack-offs, and potential stuck pipe.
    """
    
    def __init__(self):
        # Initialize model parameters based on physics and fluid dynamics
        self.model_parameters = {
            'rop_flow_ratio_threshold': 0.15,  # ROP to flow rate ratio threshold
            'ecd_increase_threshold': 0.3,  # ppg increase indication
            'pressure_fluctuation_threshold': 150,  # psi
            'torque_increase_threshold': 20,  # percent increase
            'annular_velocity_min': 120,  # ft/min minimum for effective cleaning
            'rop_flow_weight': 0.25,
            'ecd_weight': 0.25,
            'pressure_weight': 0.2,
            'torque_weight': 0.15,
            'historical_weight': 0.15
        }
    
    def predict(self, drilling_data):
        """
        Predict the likelihood of hole cleaning issues.
        
        Args:
            drilling_data (dict): Processed drilling data
            
        Returns:
            dict: Prediction results with probability and contributing factors
        """
        try:
            # Extract relevant drilling parameters
            rop = drilling_data.get('ROP', 0)
            flow_rate = drilling_data.get('Flow_Rate', 0)
            ecd = drilling_data.get('ECD', 12.5)
            spp = drilling_data.get('SPP', 0)
            torque = drilling_data.get('Torque', 0)
            hole_cleaning_index = drilling_data.get('hole_cleaning_index', 1.0)
            
            # Feature 1: ROP to flow rate ratio
            # Higher ROP relative to flow rate increases risk of poor hole cleaning
            rop_flow_ratio = (rop / 100) / (flow_rate / 600) if flow_rate > 0 else 1.0
            rop_flow_feature = min(1.0, rop_flow_ratio / self.model_parameters['rop_flow_ratio_threshold'])
            
            # Feature 2: ECD trends
            # Increasing ECD can indicate cuttings accumulation
            ecd_feature = 0.0
            
            if 'time_series' in drilling_data and 'ECD' in drilling_data['time_series']:
                ecd_series = drilling_data['time_series']['ECD']
                if len(ecd_series) > 10:
                    early_avg = np.mean(ecd_series[:5])
                    late_avg = np.mean(ecd_series[-5:])
                    
                    ecd_increase = late_avg - early_avg
                    ecd_feature = min(1.0, ecd_increase / self.model_parameters['ecd_increase_threshold'])
                    ecd_feature = max(0.0, ecd_feature)  # Ensure non-negative
            
            # Feature 3: Pressure fluctuations
            # High standpipe pressure fluctuations can indicate cuttings accumulation
            pressure_feature = 0.0
            
            if 'time_series' in drilling_data and 'SPP' in drilling_data['time_series']:
                spp_series = drilling_data['time_series']['SPP']
                if len(spp_series) > 10:
                    spp_std = np.std(spp_series[-10:])
                    pressure_feature = min(1.0, spp_std / self.model_parameters['pressure_fluctuation_threshold'])
            
            # Feature 4: Torque trends
            # Increasing torque can indicate cuttings beds
            torque_feature = 0.0
            
            if 'time_series' in drilling_data and 'Torque' in drilling_data['time_series']:
                torque_series = drilling_data['time_series']['Torque']
                if len(torque_series) > 10:
                    early_avg = np.mean(torque_series[:5])
                    late_avg = np.mean(torque_series[-5:])
                    
                    if early_avg > 0:
                        torque_increase_pct = 100 * (late_avg - early_avg) / early_avg
                        torque_feature = min(1.0, torque_increase_pct / self.model_parameters['torque_increase_threshold'])
                        torque_feature = max(0.0, torque_feature)  # Ensure non-negative
            
            # Feature 5: Historical hole cleaning performance
            # Use the calculated hole cleaning index
            historical_feature = min(1.0, max(0.0, (hole_cleaning_index - 1.0) * 2))
            
            # Combine features with weighted average
            cleaning_issue_probability = (
                self.model_parameters['rop_flow_weight'] * rop_flow_feature +
                self.model_parameters['ecd_weight'] * ecd_feature +
                self.model_parameters['pressure_weight'] * pressure_feature +
                self.model_parameters['torque_weight'] * torque_feature +
                self.model_parameters['historical_weight'] * historical_feature
            )
            
            # Clip probability to [0, 1] range
            cleaning_issue_probability = min(1.0, max(0.0, cleaning_issue_probability))
            
            # Determine contributing factors for explanation
            contributing_factors = []
            
            if rop_flow_feature > 0.6:
                contributing_factors.append("High ROP relative to flow rate")
            
            if ecd_feature > 0.6:
                contributing_factors.append("Increasing ECD trend")
            
            if pressure_feature > 0.6:
                contributing_factors.append("Significant pressure fluctuations")
            
            if torque_feature > 0.6:
                contributing_factors.append("Increasing torque trend")
            
            if historical_feature > 0.6:
                contributing_factors.append("Poor hole cleaning efficiency")
            
            # Generate recommendations based on probability
            recommendations = self._generate_recommendations(cleaning_issue_probability, contributing_factors, drilling_data)
            
            # Prepare result dictionary
            result = {
                'probability': cleaning_issue_probability,
                'contributing_factors': contributing_factors,
                'recommendations': recommendations,
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            logger.info(f"Hole cleaning issue prediction: {cleaning_issue_probability:.2f}")
            return result
        
        except Exception as e:
            logger.error(f"Error predicting hole cleaning issues: {str(e)}")
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
            recommendations.append("Monitor ECD and cuttings returns")
        
        elif probability < 0.7:
            # Medium risk - preventive measures
            recommendations.append("Perform pumps-off flow check")
            recommendations.append("Consider wiper trips to clean the wellbore")
            
            if "High ROP relative to flow rate" in factors:
                recommendations.append("Reduce ROP or increase flow rate")
            
            if "Increasing ECD trend" in factors:
                recommendations.append("Monitor for pressure spikes during connections")
            
            if "Poor hole cleaning efficiency" in factors:
                recommendations.append("Optimize drilling fluid properties")
                recommendations.append("Increase annular velocity if possible")
        
        else:
            # High risk - significant action required
            recommendations.append("Stop drilling and circulate bottoms up")
            recommendations.append("Perform wiper trips with high flow rates")
            recommendations.append("Monitor ECD closely")
            
            if "Significant pressure fluctuations" in factors:
                recommendations.append("Check for packoff indications")
            
            if "Increasing torque trend" in factors:
                recommendations.append("Work pipe with high flow rates to clean wellbore")
                recommendations.append("Consider modifying mud rheology to improve cutting transport")
            
            if "High ROP relative to flow rate" in factors:
                recommendations.append("Significantly reduce ROP until hole cleaning improves")
        
        return recommendations

# Create a singleton instance
agent = HoleCleaningAgent()

def predict(drilling_data):
    """Wrapper function to call the agent's predict method"""
    return agent.predict(drilling_data)
