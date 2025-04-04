"""
Orchestrator for the drilling prediction application.

This module orchestrates the ML predictions and generates alerts based on the results.
"""

import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def evaluate_predictions(predictions, thresholds):
    """
    Evaluate the ML agent predictions and generate alerts.
    
    Args:
        predictions (dict): Dictionary of predictions from different ML agents
        thresholds (dict): Dictionary of alert thresholds for different agents
        
    Returns:
        list: Generated alerts
    """
    alerts = []
    
    try:
        # Check mechanical sticking predictions
        if predictions['mechanical_sticking'] is not None:
            prob = predictions['mechanical_sticking'].get('probability', 0)
            threshold = thresholds.get('mechanical_sticking', 0.6)
            
            if prob >= threshold:
                # Generate alert for mechanical sticking
                severity = "HIGH" if prob >= 0.8 else "MEDIUM" if prob >= 0.6 else "LOW"
                
                # Get contributing factors for message
                factors = []
                if 'contributing_factors' in predictions['mechanical_sticking']:
                    for factor in predictions['mechanical_sticking']['contributing_factors']:
                        factors.append(f"{factor['factor']} ({factor['value']})")
                
                # Get recommendations
                recommendations = []
                if 'recommendations' in predictions['mechanical_sticking']:
                    recommendations = predictions['mechanical_sticking']['recommendations']
                
                # Create alert message
                message = f"Mechanical sticking risk detected ({prob:.1%})"
                if factors:
                    message += f". Contributing factors: {', '.join(factors)}"
                
                # Create recommendation message
                recommendation = "; ".join(recommendations) if recommendations else "Monitor drilling parameters closely"
                
                # Create alert dictionary
                alert = {
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'type': "Mechanical Sticking Risk",
                    'severity': severity,
                    'probability': f"{prob:.1%}",
                    'message': message,
                    'recommendation': recommendation
                }
                
                alerts.append(alert)
                logger.info(f"Generated mechanical sticking alert: {severity} ({prob:.1%})")
        
        # Check differential sticking predictions
        if predictions['differential_sticking'] is not None:
            prob = predictions['differential_sticking'].get('probability', 0)
            threshold = thresholds.get('differential_sticking', 0.6)
            
            if prob >= threshold:
                # Generate alert for differential sticking
                severity = "HIGH" if prob >= 0.8 else "MEDIUM" if prob >= 0.6 else "LOW"
                
                # Get contributing factors for message
                factors = []
                if 'contributing_factors' in predictions['differential_sticking']:
                    for factor in predictions['differential_sticking']['contributing_factors']:
                        factors.append(f"{factor['factor']} ({factor['value']})")
                
                # Get recommendations
                recommendations = []
                if 'recommendations' in predictions['differential_sticking']:
                    recommendations = predictions['differential_sticking']['recommendations']
                
                # Create alert message
                message = f"Differential sticking risk detected ({prob:.1%})"
                if factors:
                    message += f". Contributing factors: {', '.join(factors)}"
                
                # Create recommendation message
                recommendation = "; ".join(recommendations) if recommendations else "Monitor ECD and differential pressure"
                
                # Create alert dictionary
                alert = {
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'type': "Differential Sticking Risk",
                    'severity': severity,
                    'probability': f"{prob:.1%}",
                    'message': message,
                    'recommendation': recommendation
                }
                
                alerts.append(alert)
                logger.info(f"Generated differential sticking alert: {severity} ({prob:.1%})")
        
        # Check hole cleaning predictions
        if predictions['hole_cleaning'] is not None:
            prob = predictions['hole_cleaning'].get('probability', 0)
            threshold = thresholds.get('hole_cleaning', 0.65)
            
            if prob >= threshold:
                # Generate alert for hole cleaning
                severity = "HIGH" if prob >= 0.8 else "MEDIUM" if prob >= 0.6 else "LOW"
                
                # Get contributing factors for message
                factors = []
                if 'contributing_factors' in predictions['hole_cleaning']:
                    for factor in predictions['hole_cleaning']['contributing_factors']:
                        factors.append(f"{factor['factor']} ({factor['value']})")
                
                # Get recommendations
                recommendations = []
                if 'recommendations' in predictions['hole_cleaning']:
                    recommendations = predictions['hole_cleaning']['recommendations']
                
                # Create alert message
                message = f"Hole cleaning risk detected ({prob:.1%})"
                if factors:
                    message += f". Contributing factors: {', '.join(factors)}"
                
                # Create recommendation message
                recommendation = "; ".join(recommendations) if recommendations else "Increase flow rate and RPM"
                
                # Create alert dictionary
                alert = {
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'type': "Hole Cleaning Risk",
                    'severity': severity,
                    'probability': f"{prob:.1%}",
                    'message': message,
                    'recommendation': recommendation
                }
                
                alerts.append(alert)
                logger.info(f"Generated hole cleaning alert: {severity} ({prob:.1%})")
        
        # Check washout & mud losses predictions
        if predictions['washout_mud_losses'] is not None:
            prob = predictions['washout_mud_losses'].get('probability', 0)
            threshold = thresholds.get('washout_mud_losses', 0.7)
            
            if prob >= threshold:
                # Determine issue type (washout or mud losses)
                issue_type = predictions['washout_mud_losses'].get('issue_type', 'Washout')
                
                # Generate alert for washout or mud losses
                severity = "HIGH" if prob >= 0.8 else "MEDIUM" if prob >= 0.7 else "LOW"
                
                # Get contributing factors for message
                factors = []
                if 'contributing_factors' in predictions['washout_mud_losses']:
                    for factor in predictions['washout_mud_losses']['contributing_factors']:
                        factors.append(f"{factor['factor']} ({factor['value']})")
                
                # Get recommendations
                recommendations = []
                if 'recommendations' in predictions['washout_mud_losses']:
                    recommendations = predictions['washout_mud_losses']['recommendations']
                
                # Create alert message
                message = f"{issue_type} risk detected ({prob:.1%})"
                if factors:
                    message += f". Contributing factors: {', '.join(factors)}"
                
                # Create recommendation message
                recommendation = "; ".join(recommendations) if recommendations else f"Monitor drilling parameters for {issue_type.lower()} indicators"
                
                # Create alert dictionary
                alert = {
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'type': f"{issue_type} Risk",
                    'severity': severity,
                    'probability': f"{prob:.1%}",
                    'message': message,
                    'recommendation': recommendation
                }
                
                alerts.append(alert)
                logger.info(f"Generated {issue_type.lower()} alert: {severity} ({prob:.1%})")
        
        return alerts
    
    except Exception as e:
        logger.error(f"Error evaluating predictions: {str(e)}")
        return []

def get_recommendations(predictions):
    """
    Generate comprehensive drilling recommendations based on all predictions.
    
    Args:
        predictions (dict): Dictionary of predictions from different ML agents
        
    Returns:
        list: Prioritized recommendations
    """
    try:
        all_recommendations = []
        
        # Collect recommendations from all agents with their probabilities
        agent_types = ['mechanical_sticking', 'differential_sticking', 'hole_cleaning', 'washout_mud_losses']
        
        for agent_type in agent_types:
            if predictions[agent_type] is not None and 'probability' in predictions[agent_type]:
                prob = predictions[agent_type]['probability']
                
                if 'recommendations' in predictions[agent_type]:
                    for rec in predictions[agent_type]['recommendations']:
                        all_recommendations.append({
                            'recommendation': rec,
                            'source': agent_type.replace('_', ' ').title(),
                            'probability': prob,
                            'priority': 'high' if prob >= 0.8 else 'medium' if prob >= 0.6 else 'low'
                        })
        
        # Add ROP optimization recommendations if available
        if (predictions['rop_optimization'] is not None and 
            'recommended_parameters' in predictions['rop_optimization']):
            
            # Create recommendation text from recommended parameters
            rec_params = predictions['rop_optimization']['recommended_parameters']
            rec_text = "Optimize drilling parameters: "
            
            param_texts = []
            for param, value in rec_params.items():
                param_texts.append(f"{param}: {value:.1f}")
            
            rec_text += ", ".join(param_texts)
            
            # Add expected improvement if available
            if 'expected_rop_improvement' in predictions['rop_optimization']:
                imp = predictions['rop_optimization']['expected_rop_improvement']
                rec_text += f" (Expected ROP improvement: {imp:.1f}%)"
            
            all_recommendations.append({
                'recommendation': rec_text,
                'source': 'ROP Optimization',
                'probability': 1.0,  # Always high priority for optimization
                'priority': 'medium'  # Medium priority by default
            })
        
        # Sort recommendations by priority and probability
        priority_values = {'high': 3, 'medium': 2, 'low': 1}
        sorted_recommendations = sorted(
            all_recommendations, 
            key=lambda x: (priority_values.get(x['priority'], 0), x['probability']),
            reverse=True
        )
        
        return sorted_recommendations
    
    except Exception as e:
        logger.error(f"Error generating recommendations: {str(e)}")
        return []