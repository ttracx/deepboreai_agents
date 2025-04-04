import logging
from datetime import datetime
import numpy as np

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
    
    # Get current timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Evaluate Mechanical Sticking predictions
    if predictions['mechanical_sticking'] is not None:
        mech_prob = predictions['mechanical_sticking']['probability']
        mech_threshold = thresholds['mechanical_sticking']
        
        if mech_prob >= mech_threshold:
            # Determine severity
            if mech_prob >= 0.8:
                severity = "HIGH"
            elif mech_prob >= 0.6:
                severity = "MEDIUM"
            else:
                severity = "LOW"
                
            # Create alert
            alert = {
                'timestamp': timestamp,
                'type': 'Mechanical Sticking Risk',
                'severity': severity,
                'probability': f"{mech_prob:.2f}",
                'message': f"Mechanical sticking risk detected ({mech_prob:.2f}). "
                          f"Contributing factors: {', '.join(predictions['mechanical_sticking']['contributing_factors'])}",
                'recommendation': predictions['mechanical_sticking']['recommendations'][0] 
                                if predictions['mechanical_sticking']['recommendations'] else "No specific recommendation"
            }
            alerts.append(alert)
    
    # Evaluate Differential Sticking predictions
    if predictions['differential_sticking'] is not None:
        diff_prob = predictions['differential_sticking']['probability']
        diff_threshold = thresholds['differential_sticking']
        
        if diff_prob >= diff_threshold:
            # Determine severity
            if diff_prob >= 0.8:
                severity = "HIGH"
            elif diff_prob >= 0.6:
                severity = "MEDIUM"
            else:
                severity = "LOW"
                
            # Create alert
            alert = {
                'timestamp': timestamp,
                'type': 'Differential Sticking Risk',
                'severity': severity,
                'probability': f"{diff_prob:.2f}",
                'message': f"Differential sticking risk detected ({diff_prob:.2f}). "
                          f"Contributing factors: {', '.join(predictions['differential_sticking']['contributing_factors'])}",
                'recommendation': predictions['differential_sticking']['recommendations'][0] 
                                if predictions['differential_sticking']['recommendations'] else "No specific recommendation"
            }
            alerts.append(alert)
    
    # Evaluate Hole Cleaning predictions
    if predictions['hole_cleaning'] is not None:
        hole_prob = predictions['hole_cleaning']['probability']
        hole_threshold = thresholds['hole_cleaning']
        
        if hole_prob >= hole_threshold:
            # Determine severity
            if hole_prob >= 0.8:
                severity = "HIGH"
            elif hole_prob >= 0.6:
                severity = "MEDIUM"
            else:
                severity = "LOW"
                
            # Create alert
            alert = {
                'timestamp': timestamp,
                'type': 'Hole Cleaning Risk',
                'severity': severity,
                'probability': f"{hole_prob:.2f}",
                'message': f"Hole cleaning issue detected ({hole_prob:.2f}). "
                          f"Contributing factors: {', '.join(predictions['hole_cleaning']['contributing_factors'])}",
                'recommendation': predictions['hole_cleaning']['recommendations'][0] 
                                if predictions['hole_cleaning']['recommendations'] else "No specific recommendation"
            }
            alerts.append(alert)
    
    # Evaluate Washout & Mud Losses predictions
    if predictions['washout_mud_losses'] is not None:
        wash_prob = predictions['washout_mud_losses']['probability']
        wash_threshold = thresholds['washout_mud_losses']
        
        if wash_prob >= wash_threshold:
            # Determine severity
            if wash_prob >= 0.8:
                severity = "HIGH"
            elif wash_prob >= 0.6:
                severity = "MEDIUM"
            else:
                severity = "LOW"
                
            issue_type = predictions['washout_mud_losses'].get('issue_type', 'Washout/Mud Losses')
            
            # Create alert
            alert = {
                'timestamp': timestamp,
                'type': f'{issue_type} Risk',
                'severity': severity,
                'probability': f"{wash_prob:.2f}",
                'message': f"{issue_type} risk detected ({wash_prob:.2f}). "
                          f"Contributing factors: {', '.join(predictions['washout_mud_losses']['contributing_factors'])}",
                'recommendation': predictions['washout_mud_losses']['recommendations'][0] 
                                if predictions['washout_mud_losses']['recommendations'] else "No specific recommendation"
            }
            alerts.append(alert)
    
    # ROP Optimization doesn't generate alerts in the same way
    # Instead, it provides recommendations
    
    logger.info(f"Generated {len(alerts)} alerts")
    return alerts


def get_recommendations(predictions):
    """
    Generate comprehensive drilling recommendations based on all predictions.
    
    Args:
        predictions (dict): Dictionary of predictions from different ML agents
        
    Returns:
        list: Prioritized recommendations
    """
    recommendations = []
    
    # Skip if predictions are not available
    if any(pred is None for pred in predictions.values()):
        return recommendations
    
    # Check for critical issues first and prioritize them
    critical_issues = []
    
    # Mechanical sticking
    if predictions['mechanical_sticking']['probability'] >= 0.7:
        critical_issues.append({
            'type': 'mechanical_sticking',
            'probability': predictions['mechanical_sticking']['probability'],
            'recommendations': predictions['mechanical_sticking']['recommendations']
        })
    
    # Differential sticking
    if predictions['differential_sticking']['probability'] >= 0.7:
        critical_issues.append({
            'type': 'differential_sticking',
            'probability': predictions['differential_sticking']['probability'],
            'recommendations': predictions['differential_sticking']['recommendations']
        })
    
    # Hole cleaning
    if predictions['hole_cleaning']['probability'] >= 0.7:
        critical_issues.append({
            'type': 'hole_cleaning',
            'probability': predictions['hole_cleaning']['probability'],
            'recommendations': predictions['hole_cleaning']['recommendations']
        })
    
    # Washout & mud losses
    if predictions['washout_mud_losses']['probability'] >= 0.7:
        critical_issues.append({
            'type': 'washout_mud_losses',
            'probability': predictions['washout_mud_losses']['probability'],
            'recommendations': predictions['washout_mud_losses']['recommendations'],
            'issue_type': predictions['washout_mud_losses'].get('issue_type', 'Washout/Mud Losses')
        })
    
    # Process critical issues first (highest probability first)
    if critical_issues:
        critical_issues.sort(key=lambda x: x['probability'], reverse=True)
        
        for issue in critical_issues:
            if issue['type'] == 'mechanical_sticking':
                recommendations.append({
                    'category': 'Mechanical Sticking',
                    'title': 'Critical Risk of Mechanical Sticking',
                    'impact_level': 'High',
                    'description': 'Immediate action required to prevent mechanical sticking, which could lead to significant non-productive time.',
                    'action_items': issue['recommendations'],
                    'expected_benefits': [
                        'Prevent stuck pipe incident',
                        'Avoid fishing operations',
                        'Reduce non-productive time'
                    ]
                })
            
            elif issue['type'] == 'differential_sticking':
                recommendations.append({
                    'category': 'Differential Sticking',
                    'title': 'Critical Risk of Differential Sticking',
                    'impact_level': 'High',
                    'description': 'Immediate action required to prevent differential sticking due to pressure imbalance, which could lead to stuck pipe.',
                    'action_items': issue['recommendations'],
                    'expected_benefits': [
                        'Prevent differential sticking incident',
                        'Maintain well control',
                        'Avoid costly drilling interruptions'
                    ]
                })
            
            elif issue['type'] == 'hole_cleaning':
                recommendations.append({
                    'category': 'Hole Cleaning',
                    'title': 'Critical Hole Cleaning Issue Detected',
                    'impact_level': 'High',
                    'description': 'Immediate action required to improve hole cleaning and prevent packoff or stuck pipe scenarios.',
                    'action_items': issue['recommendations'],
                    'expected_benefits': [
                        'Prevent packoff incidents',
                        'Improve cuttings transport',
                        'Reduce risk of stuck pipe'
                    ]
                })
            
            elif issue['type'] == 'washout_mud_losses':
                title = f'Critical {issue["issue_type"]} Issue Detected'
                if issue['issue_type'] == 'Washout':
                    description = 'Immediate action required to address potential washout in the drillstring.'
                    benefits = [
                        'Prevent complete failure of drillstring components',
                        'Avoid fishing operations',
                        'Maintain well control'
                    ]
                else:  # Mud Losses
                    description = 'Immediate action required to address mud losses to the formation.'
                    benefits = [
                        'Prevent complete loss of returns',
                        'Maintain well control',
                        'Reduce fluid costs'
                    ]
                
                recommendations.append({
                    'category': issue['issue_type'],
                    'title': title,
                    'impact_level': 'High',
                    'description': description,
                    'action_items': issue['recommendations'],
                    'expected_benefits': benefits
                })
    
    # Add ROP optimization recommendations if no critical issues
    # or even if there are critical issues but they don't conflict
    if predictions['rop_optimization'] and 'recommended_parameters' in predictions['rop_optimization']:
        # Only add ROP recommendations if there are no critical issues or if they're lower priority
        if len(critical_issues) == 0 or critical_issues[0]['probability'] < 0.8:
            rec_params = predictions['rop_optimization']['recommended_parameters']
            improvement = predictions['rop_optimization'].get('expected_rop_improvement', 0)
            
            # Format parameter recommendations
            param_recommendations = []
            
            if 'WOB' in rec_params:
                param_recommendations.append(f"Adjust Weight on Bit to {rec_params['WOB']:.1f} klbs")
            
            if 'RPM' in rec_params:
                param_recommendations.append(f"Adjust Rotary Speed to {rec_params['RPM']:.0f} RPM")
            
            if 'Flow_Rate' in rec_params:
                param_recommendations.append(f"Maintain Flow Rate at {rec_params['Flow_Rate']:.0f} gpm")
            
            # Add explanations if available
            if 'explanations' in predictions['rop_optimization']:
                for explanation in predictions['rop_optimization']['explanations']:
                    param_recommendations.append(explanation)
            
            # Create recommendation entry
            impact_level = 'Medium' if improvement > 15 else 'Low'
            
            recommendations.append({
                'category': 'ROP Optimization',
                'title': f'ROP Improvement Opportunity ({improvement:.1f}%)',
                'impact_level': impact_level,
                'description': f'Adjusting drilling parameters can improve Rate of Penetration by approximately {improvement:.1f}%.',
                'action_items': param_recommendations,
                'expected_benefits': [
                    f'Increase ROP by approximately {improvement:.1f}%',
                    'Reduce overall drilling time',
                    'Improve drilling efficiency'
                ]
            })
    
    # Add lower-priority recommendations for moderate risk issues
    for agent_type, prediction in predictions.items():
        # Skip ROP and already processed critical issues
        if agent_type == 'rop_optimization' or prediction is None:
            continue
            
        prob = prediction['probability']
        
        # Check for moderate risk issues (not already covered in critical)
        if 0.4 <= prob < 0.7:
            # Skip if this issue type is already in critical issues
            if any(ci['type'] == agent_type for ci in critical_issues):
                continue
                
            if agent_type == 'mechanical_sticking':
                recommendations.append({
                    'category': 'Mechanical Sticking',
                    'title': 'Moderate Risk of Mechanical Sticking',
                    'impact_level': 'Medium',
                    'description': 'Preventive action recommended to address potential mechanical sticking issues.',
                    'action_items': prediction['recommendations'],
                })
            
            elif agent_type == 'differential_sticking':
                recommendations.append({
                    'category': 'Differential Sticking',
                    'title': 'Moderate Risk of Differential Sticking',
                    'impact_level': 'Medium',
                    'description': 'Preventive action recommended to address potential differential sticking issues.',
                    'action_items': prediction['recommendations'],
                })
            
            elif agent_type == 'hole_cleaning':
                recommendations.append({
                    'category': 'Hole Cleaning',
                    'title': 'Moderate Hole Cleaning Concerns',
                    'impact_level': 'Medium',
                    'description': 'Preventive action recommended to improve hole cleaning efficiency.',
                    'action_items': prediction['recommendations'],
                })
            
            elif agent_type == 'washout_mud_losses':
                title = f'Moderate {prediction.get("issue_type", "Washout/Loss")} Risk'
                recommendations.append({
                    'category': prediction.get('issue_type', 'Washout/Mud Losses'),
                    'title': title,
                    'impact_level': 'Medium',
                    'description': f'Preventive action recommended to address potential {prediction.get("issue_type", "washout or mud loss")} issues.',
                    'action_items': prediction['recommendations'],
                })
    
    logger.info(f"Generated {len(recommendations)} drilling recommendations")
    return recommendations
