"""
ROP optimization agent.

This module implements a machine learning agent for optimizing rate of penetration (ROP).
"""

import logging
import numpy as np
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ROPOptimizationAgent:
    """
    Agent for optimizing rate of penetration (ROP) during drilling operations.
    
    This agent analyzes drilling parameters to recommend optimal settings
    for maximizing ROP while maintaining safe drilling operations.
    """
    
    def __init__(self, aggressiveness=0.6):
        """
        Initialize the ROP optimization agent.
        
        Args:
            aggressiveness (float): Aggressiveness parameter (0-1) that affects
                the agent's optimization approach. Higher values prioritize
                ROP improvement over other considerations.
        """
        self.aggressiveness = aggressiveness
        logger.info(f"Initialized ROP optimization agent with aggressiveness {aggressiveness}")
    
    def predict(self, drilling_data):
        """
        Analyze drilling data and recommend parameter changes for ROP optimization.
        
        Args:
            drilling_data (dict): Dictionary containing drilling parameters
            
        Returns:
            dict: Optimization results including recommendations and expected improvements
        """
        try:
            if not drilling_data:
                logger.warning("Empty drilling data provided to ROP optimization agent")
                return {
                    'optimized': False,
                    'current_rop': 0.0,
                    'expected_rop_improvement': 0.0,
                    'recommended_parameters': {},
                    'contributing_factors': [],
                    'recommendations': []
                }
            
            # Extract relevant parameters
            current_rop = drilling_data.get('ROP', 0)
            wob = drilling_data.get('WOB', 0)
            rpm = drilling_data.get('RPM', 0)
            flow_rate = drilling_data.get('Flow_Rate', 0)
            torque = drilling_data.get('Torque', 0)
            mse = drilling_data.get('MSE', 0)  # Mechanical Specific Energy
            ucs = drilling_data.get('UCS', 20)  # Unconfined Compressive Strength (estimated)
            hole_cleaning_index = drilling_data.get('hole_cleaning_index', 0.8)
            
            # Get formation data if available
            formation_type = drilling_data.get('formation_type', 'Unknown')
            porosity = drilling_data.get('porosity', 0.1)
            
            # Apply physics-informed optimization model for ROP
            
            # 1. Analyze current drilling efficiency
            
            # Calculate theoretical optimal MSE based on formation strength
            optimal_mse = ucs * 0.35  # Rule of thumb: MSE should be ~35% of UCS for efficient drilling
            
            # Drilling efficiency ratio (lower MSE is better)
            efficiency_ratio = 1.0
            if mse > 0 and optimal_mse > 0:
                efficiency_ratio = optimal_mse / mse
                efficiency_ratio = min(1.0, max(0.1, efficiency_ratio))
            
            # 2. Determine parameter adjustments needed
            
            # WOB optimization
            optimal_wob = wob
            wob_adjustment_needed = False
            
            if efficiency_ratio < 0.7:  # Low efficiency
                # Check if WOB is the limiting factor
                if torque / (wob + 0.1) < 0.3:  # Low torque per unit WOB
                    # Increase WOB if MSE is high
                    optimal_wob = wob * 1.2
                    wob_adjustment_needed = True
                elif torque / (wob + 0.1) > 0.7:  # High torque per unit WOB
                    # Decrease WOB if causing excessive torque
                    optimal_wob = wob * 0.85
                    wob_adjustment_needed = True
            
            # RPM optimization
            optimal_rpm = rpm
            rpm_adjustment_needed = False
            
            if efficiency_ratio < 0.7:
                if torque > 0.8 * drilling_data.get('max_torque', 100):
                    # Decrease RPM if torque is approaching limits
                    optimal_rpm = rpm * 0.85
                    rpm_adjustment_needed = True
                elif torque < 0.4 * drilling_data.get('max_torque', 100):
                    # Increase RPM if torque is low
                    optimal_rpm = rpm * 1.15
                    rpm_adjustment_needed = True
            
            # Flow rate optimization
            optimal_flow_rate = flow_rate
            flow_adjustment_needed = False
            
            if hole_cleaning_index < 0.7:
                # Increase flow rate to improve hole cleaning
                optimal_flow_rate = flow_rate * 1.15
                flow_adjustment_needed = True
            
            # 3. Estimate ROP improvement potential
            
            # Base improvement from WOB adjustment
            wob_improvement = 0
            if wob_adjustment_needed:
                if optimal_wob > wob:
                    wob_improvement = (optimal_wob / wob - 1) * 0.7  # 70% of theoretical improvement
                else:
                    wob_improvement = -0.05  # Small negative impact
            
            # Improvement from RPM adjustment
            rpm_improvement = 0
            if rpm_adjustment_needed:
                if optimal_rpm > rpm:
                    rpm_improvement = (optimal_rpm / rpm - 1) * 0.5  # 50% of theoretical improvement
                else:
                    rpm_improvement = -0.03  # Small negative impact
            
            # Improvement from flow rate adjustment (indirect through better hole cleaning)
            flow_improvement = 0
            if flow_adjustment_needed and optimal_flow_rate > flow_rate:
                flow_improvement = 0.05  # Modest improvement from better hole cleaning
            
            # Combined improvement (multiplicative factors)
            total_improvement_factor = (1 + wob_improvement) * (1 + rpm_improvement) * (1 + flow_improvement) - 1
            
            # Apply aggressiveness factor to adjust recommendations
            aggressiveness_factor = 0.5 + 0.5 * self.aggressiveness
            
            # Scale optimizations based on aggressiveness
            if wob_adjustment_needed:
                optimal_wob = wob + (optimal_wob - wob) * aggressiveness_factor
            
            if rpm_adjustment_needed:
                optimal_rpm = rpm + (optimal_rpm - rpm) * aggressiveness_factor
            
            if flow_adjustment_needed:
                optimal_flow_rate = flow_rate + (optimal_flow_rate - flow_rate) * aggressiveness_factor
            
            # Final expected ROP improvement
            expected_rop_improvement = total_improvement_factor * current_rop
            
            # 4. Prepare recommended parameters
            recommended_parameters = {}
            
            if wob_adjustment_needed:
                recommended_parameters['WOB'] = round(optimal_wob, 1)
            
            if rpm_adjustment_needed:
                recommended_parameters['RPM'] = round(optimal_rpm, 0)
            
            if flow_adjustment_needed:
                recommended_parameters['Flow_Rate'] = round(optimal_flow_rate, 0)
            
            # 5. Determine contributing factors and recommendations
            contributing_factors = []
            recommendations = []
            
            if efficiency_ratio < 0.7:
                contributing_factors.append({
                    'factor': 'Low Drilling Efficiency',
                    'value': f"{efficiency_ratio:.2f} ratio"
                })
                
                if mse > optimal_mse * 1.5:
                    recommendations.append("Adjust parameters to reduce MSE and improve drilling efficiency")
            
            if wob_adjustment_needed:
                direction = "Increase" if optimal_wob > wob else "Decrease"
                contributing_factors.append({
                    'factor': 'WOB Adjustment',
                    'value': f"{direction} to {optimal_wob:.1f} klbs"
                })
                
                if direction == "Increase":
                    recommendations.append(f"Gradually increase WOB to {optimal_wob:.1f} klbs")
                else:
                    recommendations.append(f"Gradually decrease WOB to {optimal_wob:.1f} klbs")
            
            if rpm_adjustment_needed:
                direction = "Increase" if optimal_rpm > rpm else "Decrease"
                contributing_factors.append({
                    'factor': 'RPM Adjustment',
                    'value': f"{direction} to {optimal_rpm:.0f} rpm"
                })
                
                if direction == "Increase":
                    recommendations.append(f"Gradually increase RPM to {optimal_rpm:.0f}")
                else:
                    recommendations.append(f"Gradually decrease RPM to {optimal_rpm:.0f}")
            
            if flow_adjustment_needed:
                contributing_factors.append({
                    'factor': 'Flow Rate Adjustment',
                    'value': f"Increase to {optimal_flow_rate:.0f} gpm"
                })
                recommendations.append(f"Increase flow rate to {optimal_flow_rate:.0f} gpm for better hole cleaning")
            
            # Add general recommendations based on formation type
            if formation_type in ["Sandstone", "Sand"]:
                recommendations.append("For sandstone formations, maintain higher RPM and moderate WOB")
            elif formation_type in ["Shale", "Clay"]:
                recommendations.append("For shale formations, maintain moderate RPM and higher WOB")
            elif formation_type in ["Limestone", "Carbonate"]:
                recommendations.append("For limestone formations, use balanced WOB and RPM")
            
            # Prepare prediction result
            prediction = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'optimized': len(recommended_parameters) > 0,
                'current_rop': current_rop,
                'expected_rop_improvement': round(expected_rop_improvement, 1),
                'expected_rop': round(current_rop + expected_rop_improvement, 1),
                'recommended_parameters': recommended_parameters,
                'contributing_factors': contributing_factors,
                'recommendations': recommendations
            }
            
            logger.debug(f"ROP optimization prediction: {expected_rop_improvement:.2f} ft/hr potential improvement")
            return prediction
            
        except Exception as e:
            logger.error(f"Error in ROP optimization: {str(e)}")
            return {
                'optimized': False,
                'current_rop': 0.0,
                'expected_rop_improvement': 0.0,
                'recommended_parameters': {},
                'contributing_factors': [],
                'recommendations': ["Error in optimization model"]
            }