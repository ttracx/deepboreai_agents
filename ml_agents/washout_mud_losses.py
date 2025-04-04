import numpy as np
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WashoutMudLossesAgent:
    """
    Physics-informed ML agent for detecting and predicting washouts and mud losses.
    
    Washouts occur when the drill string or components erode, while mud losses 
    happen when drilling fluid is lost to the formation.
    """
    
    def __init__(self):
        # Initialize model parameters based on fluid mechanics and formation physics
        self.model_parameters = {
            'spp_drop_threshold': 200,  # psi drop indicating potential washout
            'flow_return_ratio_threshold': 0.9,  # Return flow / pumped flow ratio below this indicates losses
            'pressure_spikes_threshold': 3,  # Number of pressure spikes indicating potential washout
            'torque_fluctuation_threshold': 2.0,  # Nm standard deviation indicating potential washout
            'pressure_drop_weight': 0.3,
            'flow_return_weight': 0.3,
            'pressure_anomaly_weight': 0.2,
            'torque_fluctuation_weight': 0.2
        }
    
    def predict(self, drilling_data):
        """
        Predict the likelihood of washouts and mud losses.
        
        Args:
            drilling_data (dict): Processed drilling data
            
        Returns:
            dict: Prediction results with probability and contributing factors
        """
        try:
            # Extract relevant drilling parameters
            spp = drilling_data.get('SPP', 0)
            flow_rate = drilling_data.get('Flow_Rate', 0)
            torque = drilling_data.get('Torque', 0)
            
            # Feature 1: Standpipe pressure drops
            # Sudden drops in SPP can indicate washout
            pressure_drop_feature = 0.0
            
            if 'time_series' in drilling_data and 'SPP' in drilling_data['time_series']:
                spp_series = drilling_data['time_series']['SPP']
                if len(spp_series) > 10:
                    # Calculate differences between consecutive readings
                    spp_diff = np.diff(spp_series[-10:])
                    # Look for significant drops
                    significant_drops = [d for d in spp_diff if d < -50]
                    if significant_drops:
                        max_drop = abs(min(significant_drops))
                        pressure_drop_feature = min(1.0, max_drop / self.model_parameters['spp_drop_threshold'])
            
            # Feature 2: Flow rate return anomalies
            # Low return flow compared to pump flow indicates losses
            # For this demo, we'll simulate flow return with an estimated value
            flow_return_feature = 0.0
            
            # Simulate a return flow value (in a real system, this would come from sensors)
            # Return flow ratio < 1 indicates losses
            return_flow_ratio = np.random.uniform(0.85, 1.05)  # Simulate return flow
            if return_flow_ratio < self.model_parameters['flow_return_ratio_threshold']:
                flow_return_feature = (self.model_parameters['flow_return_ratio_threshold'] - return_flow_ratio) * 10
                flow_return_feature = min(1.0, flow_return_feature)
            
            # Feature 3: Pressure anomalies
            # Erratic pressure behavior can indicate washout development
            pressure_anomaly_feature = 0.0
            
            if 'time_series' in drilling_data and 'SPP' in drilling_data['time_series']:
                spp_series = drilling_data['time_series']['SPP']
                if len(spp_series) > 15:
                    # Calculate rolling standard deviation
                    window_size = 5
                    std_values = []
                    for i in range(len(spp_series) - window_size):
                        std_values.append(np.std(spp_series[i:i+window_size]))
                    
                    # Look for windows with high standard deviation (spikes)
                    spikes = [s for s in std_values if s > 50]  # Threshold for significant fluctuation
                    pressure_anomaly_feature = min(1.0, len(spikes) / self.model_parameters['pressure_spikes_threshold'])
            
            # Feature 4: Torque fluctuations
            # Erratic torque can indicate washout or BHA/drill pipe problems
            torque_fluctuation_feature = 0.0
            
            if 'time_series' in drilling_data and 'Torque' in drilling_data['time_series']:
                torque_series = drilling_data['time_series']['Torque']
                if len(torque_series) > 10:
                    torque_std = np.std(torque_series[-10:])
                    torque_fluctuation_feature = min(1.0, torque_std / self.model_parameters['torque_fluctuation_threshold'])
            
            # Combine features with weighted average
            washout_mud_loss_probability = (
                self.model_parameters['pressure_drop_weight'] * pressure_drop_feature +
                self.model_parameters['flow_return_weight'] * flow_return_feature +
                self.model_parameters['pressure_anomaly_weight'] * pressure_anomaly_feature +
                self.model_parameters['torque_fluctuation_weight'] * torque_fluctuation_feature
            )
            
            # Clip probability to [0, 1] range
            washout_mud_loss_probability = min(1.0, max(0.0, washout_mud_loss_probability))
            
            # Determine contributing factors for explanation
            contributing_factors = []
            
            if pressure_drop_feature > 0.6:
                contributing_factors.append("Significant standpipe pressure drops")
            
            if flow_return_feature > 0.6:
                contributing_factors.append("Reduced fluid returns")
            
            if pressure_anomaly_feature > 0.6:
                contributing_factors.append("Erratic pressure behavior")
            
            if torque_fluctuation_feature > 0.6:
                contributing_factors.append("Unusual torque fluctuations")
            
            # Determine if the issue is primarily washout vs mud losses
            issue_type = "Washout"
            if flow_return_feature > pressure_drop_feature:
                issue_type = "Mud Losses"
            
            # Generate recommendations based on probability
            recommendations = self._generate_recommendations(
                washout_mud_loss_probability, 
                contributing_factors, 
                drilling_data, 
                issue_type
            )
            
            # Prepare result dictionary
            result = {
                'probability': washout_mud_loss_probability,
                'issue_type': issue_type,
                'contributing_factors': contributing_factors,
                'recommendations': recommendations,
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            logger.info(f"{issue_type} prediction: {washout_mud_loss_probability:.2f}")
            return result
        
        except Exception as e:
            logger.error(f"Error predicting washout/mud losses: {str(e)}")
            return {
                'probability': 0.0,
                'issue_type': "Unknown",
                'contributing_factors': ["Error in prediction model"],
                'recommendations': ["Check system logs for errors"],
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
    
    def _generate_recommendations(self, probability, factors, data, issue_type):
        """Generate drilling recommendations based on prediction results"""
        recommendations = []
        
        if probability < 0.3:
            # Low risk - normal operations
            recommendations.append("Continue normal operations")
            if issue_type == "Washout":
                recommendations.append("Monitor standpipe pressure for unexpected changes")
            else:
                recommendations.append("Monitor flow returns and pit volumes")
        
        elif probability < 0.7:
            # Medium risk - preventive measures
            if issue_type == "Washout":
                recommendations.append("Check drill string connection integrity")
                recommendations.append("Monitor pressure trends carefully")
                
                if "Unusual torque fluctuations" in factors:
                    recommendations.append("Check for bottom hole assembly issues")
                
                if "Erratic pressure behavior" in factors:
                    recommendations.append("Consider flow tests to isolate pressure anomalies")
            else:
                recommendations.append("Monitor pit volumes closely")
                recommendations.append("Perform flow checks to verify losses")
                
                if "Reduced fluid returns" in factors:
                    recommendations.append("Prepare lost circulation material")
                    recommendations.append("Consider reducing mud weight if safe to do so")
        
        else:
            # High risk - significant action required
            if issue_type == "Washout":
                recommendations.append("Stop drilling and perform flow checks")
                recommendations.append("Prepare to trip out if washout confirmed")
                
                if "Significant standpipe pressure drops" in factors:
                    recommendations.append("Monitor for additional pressure decreases")
                    recommendations.append("Verify BHA component integrity")
                
                if "Erratic pressure behavior" in factors:
                    recommendations.append("Isolate pressure fluctuations through systematic testing")
            else:
                recommendations.append("Stop drilling and assess loss severity")
                recommendations.append("Activate lost circulation protocol")
                recommendations.append("Consider spotting lost circulation material")
                
                if "Reduced fluid returns" in factors:
                    recommendations.append("Evaluate need for blind/controlled drilling procedures")
                    recommendations.append("Monitor for well control concerns due to losses")
        
        return recommendations

# Create a singleton instance
agent = WashoutMudLossesAgent()

def predict(drilling_data):
    """Wrapper function to call the agent's predict method"""
    return agent.predict(drilling_data)
