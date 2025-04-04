import numpy as np
import logging
from datetime import datetime
from sklearn.linear_model import Ridge
import joblib
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ROPOptimizationAgent:
    """
    Physics-informed ML agent for optimizing rate of penetration (ROP).
    
    This agent analyzes drilling parameters to recommend optimal settings
    for maximizing ROP while maintaining safe drilling practices.
    """
    
    def __init__(self):
        # Initialize the optimization model
        self.model = None
        self.is_trained = False
        
        # Physics constraints and configuration
        self.constraints = {
            'max_wob': 50,  # Maximum weight on bit (klbs)
            'min_wob': 5,   # Minimum weight on bit (klbs)
            'max_rpm': 180, # Maximum RPM
            'min_rpm': 60,  # Minimum RPM
            'max_flow': 800, # Maximum flow rate (gpm)
            'min_flow': 400, # Minimum flow rate (gpm)
            'max_torque': 15, # Maximum torque (kft-lbs)
            'max_spp': 4500  # Maximum standpipe pressure (psi)
        }
        
        # Optimization parameters
        self.optimization_params = {
            'wob_increment': 2,    # Increment for WOB testing (klbs)
            'rpm_increment': 10,   # Increment for RPM testing
            'learning_rate': 0.1,  # Learning rate for model updates
            'history_window': 20,  # Number of data points to use for training
            'training_frequency': 10  # How often to retrain the model (in data points)
        }
        
        # Training data storage
        self.training_data = {
            'X': [],  # Input features (WOB, RPM, Flow, etc.)
            'y': []   # Target (ROP)
        }
        
        # Data point counter
        self.data_counter = 0
        
        # Initialize the model
        self._initialize_model()
    
    def _initialize_model(self):
        """Initialize the ROP prediction model"""
        # For this implementation, we'll use a simple Ridge regression model
        # In a full system, more complex models like physics-informed neural networks would be used
        self.model = Ridge(alpha=1.0)
        
        # If a pre-trained model exists, load it
        if os.path.exists('rop_model.pkl'):
            try:
                self.model = joblib.load('rop_model.pkl')
                self.is_trained = True
                logger.info("Loaded pre-trained ROP model")
            except Exception as e:
                logger.warning(f"Could not load pre-trained model: {str(e)}")
    
    def _update_training_data(self, drilling_data):
        """Update the training data with new drilling information"""
        try:
            # Extract features for ROP prediction
            features = [
                drilling_data.get('WOB', 0),           # Weight on bit
                drilling_data.get('RPM', 0),           # Rotary speed
                drilling_data.get('Flow_Rate', 0),     # Flow rate
                drilling_data.get('Torque', 0),        # Torque
                drilling_data.get('SPP', 0),           # Standpipe pressure
                drilling_data.get('ECD', 12.5),        # Equivalent circulating density
                drilling_data.get('MSE', 30000)        # Mechanical specific energy
            ]
            
            # Target value is ROP
            target = drilling_data.get('ROP', 0)
            
            # Add to training data
            if target > 0:  # Only use valid ROP values
                self.training_data['X'].append(features)
                self.training_data['y'].append(target)
                
                # Keep only the most recent data points
                window = self.optimization_params['history_window']
                if len(self.training_data['X']) > window:
                    self.training_data['X'] = self.training_data['X'][-window:]
                    self.training_data['y'] = self.training_data['y'][-window:]
                
                self.data_counter += 1
                
                # Retrain the model periodically
                if self.data_counter % self.optimization_params['training_frequency'] == 0:
                    self._train_model()
            
        except Exception as e:
            logger.error(f"Error updating training data: {str(e)}")
    
    def _train_model(self):
        """Train the ROP prediction model with current data"""
        try:
            if len(self.training_data['X']) >= 5:  # Need minimum data points
                # Convert to numpy arrays
                X = np.array(self.training_data['X'])
                y = np.array(self.training_data['y'])
                
                # Train the model
                self.model.fit(X, y)
                self.is_trained = True
                
                # Save the model (in a full implementation)
                # joblib.dump(self.model, 'rop_model.pkl')
                
                logger.info(f"ROP model trained with {len(self.training_data['X'])} data points")
            else:
                logger.info("Not enough data points for ROP model training")
                
        except Exception as e:
            logger.error(f"Error training ROP model: {str(e)}")
    
    def _optimize_parameters(self, current_data):
        """Find optimal drilling parameters to maximize ROP"""
        if not self.is_trained:
            logger.info("ROP model not trained yet, using default recommendations")
            return self._get_default_recommendations(current_data)
        
        try:
            # Current parameter values
            current_wob = current_data.get('WOB', 20)
            current_rpm = current_data.get('RPM', 120)
            current_flow = current_data.get('Flow_Rate', 600)
            current_rop = current_data.get('ROP', 0)
            
            # Define parameter ranges to test
            wob_range = np.arange(
                max(self.constraints['min_wob'], current_wob - 2 * self.optimization_params['wob_increment']),
                min(self.constraints['max_wob'], current_wob + 2 * self.optimization_params['wob_increment']),
                self.optimization_params['wob_increment']
            )
            
            rpm_range = np.arange(
                max(self.constraints['min_rpm'], current_rpm - 2 * self.optimization_params['rpm_increment']),
                min(self.constraints['max_rpm'], current_rpm + 2 * self.optimization_params['rpm_increment']),
                self.optimization_params['rpm_increment']
            )
            
            # Keep other parameters constant for simplicity
            flow = current_flow
            
            # Get current values for other parameters
            torque = current_data.get('Torque', 10)
            spp = current_data.get('SPP', 3500)
            ecd = current_data.get('ECD', 12.5)
            mse = current_data.get('MSE', 30000)
            
            # Create test combinations
            best_rop = 0
            best_params = {'WOB': current_wob, 'RPM': current_rpm, 'Flow_Rate': flow}
            
            # Test different parameter combinations
            for wob in wob_range:
                for rpm in rpm_range:
                    # Simple physics-based adjustments for non-independent parameters
                    # In reality, these would be based on more complex drilling models
                    est_torque = torque * (wob / current_wob) * (rpm / current_rpm)
                    est_spp = spp * (flow / current_flow)**2
                    
                    # Skip if constraints are violated
                    if est_torque > self.constraints['max_torque'] or est_spp > self.constraints['max_spp']:
                        continue
                    
                    # Create feature vector for prediction
                    features = np.array([[wob, rpm, flow, est_torque, est_spp, ecd, mse]])
                    
                    # Predict ROP
                    predicted_rop = self.model.predict(features)[0]
                    
                    # Update best parameters if improvement found
                    if predicted_rop > best_rop:
                        best_rop = predicted_rop
                        best_params = {'WOB': wob, 'RPM': rpm, 'Flow_Rate': flow}
            
            # Calculate expected improvement
            expected_improvement = ((best_rop - current_rop) / current_rop * 100) if current_rop > 0 else 0
            
            # Return the optimized parameters
            result = {
                'recommended_parameters': best_params,
                'expected_rop': best_rop,
                'expected_rop_improvement': expected_improvement
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error optimizing ROP parameters: {str(e)}")
            return self._get_default_recommendations(current_data)
    
    def _get_default_recommendations(self, current_data):
        """Provide default recommendations based on physics principles when model is not trained"""
        # Current parameter values
        current_wob = current_data.get('WOB', 20)
        current_rpm = current_data.get('RPM', 120)
        current_flow = current_data.get('Flow_Rate', 600)
        
        # Simple heuristic adjustments
        # If MSE is high, recommend increasing WOB and reducing RPM
        mse = current_data.get('MSE', 30000)
        if mse > 35000:
            wob_rec = min(current_wob * 1.1, self.constraints['max_wob'])
            rpm_rec = max(current_rpm * 0.9, self.constraints['min_rpm'])
        else:
            # Otherwise, recommend small increases to both
            wob_rec = min(current_wob * 1.05, self.constraints['max_wob'])
            rpm_rec = min(current_rpm * 1.05, self.constraints['max_rpm'])
        
        # Always ensure adequate flow rate for hole cleaning
        flow_rec = current_flow
        
        return {
            'recommended_parameters': {
                'WOB': wob_rec,
                'RPM': rpm_rec,
                'Flow_Rate': flow_rec
            },
            'expected_rop': current_data.get('ROP', 0) * 1.1,  # Assume 10% improvement
            'expected_rop_improvement': 10.0  # Default 10% improvement estimation
        }
    
    def predict(self, drilling_data):
        """
        Predict optimal drilling parameters for ROP optimization.
        
        Args:
            drilling_data (dict): Processed drilling data
            
        Returns:
            dict: Optimization results with recommended parameters
        """
        try:
            # Update training data with the new information
            self._update_training_data(drilling_data)
            
            # Get optimization recommendations
            optimization_results = self._optimize_parameters(drilling_data)
            
            # Additional information for the drilling engineer
            current_rop = drilling_data.get('ROP', 0)
            current_wob = drilling_data.get('WOB', 0)
            current_rpm = drilling_data.get('RPM', 0)
            
            # Determine limiting factors
            limiting_factors = []
            
            # Check for MSE-related limitations
            mse = drilling_data.get('MSE', 30000)
            if mse > 35000:
                limiting_factors.append("High mechanical specific energy indicating inefficient drilling")
            
            # Check for pressure limitations
            spp = drilling_data.get('SPP', 0)
            if spp > 0.8 * self.constraints['max_spp']:
                limiting_factors.append("Approaching maximum allowable standpipe pressure")
            
            # Check for torque limitations
            torque = drilling_data.get('Torque', 0)
            if torque > 0.8 * self.constraints['max_torque']:
                limiting_factors.append("Approaching maximum allowable torque")
            
            # Prepare recommendations for explanation
            explanations = []
            rec_params = optimization_results['recommended_parameters']
            
            if abs(rec_params['WOB'] - current_wob) > 1:
                direction = "increase" if rec_params['WOB'] > current_wob else "decrease"
                explanations.append(f"Recommend {direction} in Weight on Bit for better ROP performance")
            
            if abs(rec_params['RPM'] - current_rpm) > 5:
                direction = "increase" if rec_params['RPM'] > current_rpm else "decrease"
                explanations.append(f"Recommend {direction} in RPM to optimize drilling efficiency")
            
            # Prepare result dictionary
            result = {
                'probability': 0.9,  # Confidence in recommendation
                'recommended_parameters': optimization_results['recommended_parameters'],
                'expected_rop': optimization_results['expected_rop'],
                'expected_rop_improvement': optimization_results['expected_rop_improvement'],
                'limiting_factors': limiting_factors,
                'explanations': explanations,
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            logger.info(f"ROP optimization recommendation: WOB={rec_params['WOB']:.1f}, RPM={rec_params['RPM']:.0f}")
            return result
        
        except Exception as e:
            logger.error(f"Error in ROP optimization: {str(e)}")
            return {
                'probability': 0.0,
                'recommended_parameters': {
                    'WOB': drilling_data.get('WOB', 0),
                    'RPM': drilling_data.get('RPM', 0),
                    'Flow_Rate': drilling_data.get('Flow_Rate', 0)
                },
                'expected_rop_improvement': 0,
                'limiting_factors': ["Error in optimization model"],
                'explanations': ["Check system logs for errors"],
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

# Create a singleton instance
agent = ROPOptimizationAgent()

def predict(drilling_data):
    """Wrapper function to call the agent's predict method"""
    return agent.predict(drilling_data)
