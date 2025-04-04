"""
Database models for the drilling prediction application.

This module defines the database models for storing drilling data,
predictions, alerts, and recommendations.
"""

import logging
import json
from datetime import datetime
from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean, ForeignKey, JSON, Text
from sqlalchemy.orm import relationship
from database.connection import Base

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DrillingData(Base):
    """Model for storing raw drilling data."""
    __tablename__ = 'drilling_data'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    depth = Column(Float)
    wob = Column(Float)  # Weight on bit
    rop = Column(Float)  # Rate of penetration
    rpm = Column(Float)  # Rotary speed
    torque = Column(Float)
    spp = Column(Float)  # Standpipe pressure
    flow_rate = Column(Float)
    ecd = Column(Float)  # Equivalent circulating density
    hook_load = Column(Float)
    mse = Column(Float)  # Mechanical specific energy
    drag_factor = Column(Float)
    differential_pressure = Column(Float)
    hole_cleaning_index = Column(Float)
    
    # Change metrics
    wob_change = Column(Float)
    rop_change = Column(Float)
    rpm_change = Column(Float)
    torque_change = Column(Float)
    spp_change = Column(Float)
    flow_rate_change = Column(Float)
    
    # Statistical metrics
    wob_avg = Column(Float)
    wob_std = Column(Float)
    wob_rate = Column(Float)  # Rate of change
    rop_avg = Column(Float)
    rop_std = Column(Float)
    rop_rate = Column(Float)
    rpm_avg = Column(Float)
    rpm_std = Column(Float)
    rpm_rate = Column(Float)
    torque_avg = Column(Float)
    torque_std = Column(Float)
    torque_rate = Column(Float)
    spp_avg = Column(Float)
    spp_std = Column(Float)
    spp_rate = Column(Float)
    flow_rate_avg = Column(Float)
    flow_rate_std = Column(Float)
    flow_rate_rate = Column(Float)
    
    # Relationships
    predictions = relationship("Prediction", back_populates="drilling_data", cascade="all, delete-orphan")
    
    def to_dict(self):
        """Convert model to dictionary."""
        return {
            'id': self.id,
            'timestamp': self.timestamp.strftime("%Y-%m-%d %H:%M:%S") if self.timestamp else None,
            'depth': self.depth,
            'WOB': self.wob,
            'ROP': self.rop,
            'RPM': self.rpm,
            'Torque': self.torque,
            'SPP': self.spp,
            'Flow_Rate': self.flow_rate,
            'ECD': self.ecd,
            'hook_load': self.hook_load,
            'MSE': self.mse,
            'drag_factor': self.drag_factor,
            'differential_pressure': self.differential_pressure,
            'hole_cleaning_index': self.hole_cleaning_index,
            'WOB_change': self.wob_change,
            'ROP_change': self.rop_change,
            'RPM_change': self.rpm_change,
            'Torque_change': self.torque_change,
            'SPP_change': self.spp_change,
            'Flow_Rate_change': self.flow_rate_change,
            'wob_avg': self.wob_avg,
            'wob_std': self.wob_std,
            'wob_rate': self.wob_rate,
            'rop_avg': self.rop_avg,
            'rop_std': self.rop_std,
            'rop_rate': self.rop_rate,
            'rpm_avg': self.rpm_avg,
            'rpm_std': self.rpm_std,
            'rpm_rate': self.rpm_rate,
            'torque_avg': self.torque_avg,
            'torque_std': self.torque_std,
            'torque_rate': self.torque_rate,
            'spp_avg': self.spp_avg,
            'spp_std': self.spp_std,
            'spp_rate': self.spp_rate,
            'flow_rate_avg': self.flow_rate_avg,
            'flow_rate_std': self.flow_rate_std,
            'flow_rate_rate': self.flow_rate_rate
        }
    
    @classmethod
    def from_dict(cls, data_dict):
        """Create model from dictionary."""
        # Map API names to database column names
        mapping = {
            'WOB': 'wob',
            'ROP': 'rop',
            'RPM': 'rpm',
            'Torque': 'torque',
            'SPP': 'spp',
            'Flow_Rate': 'flow_rate',
            'ECD': 'ecd',
            'WOB_change': 'wob_change',
            'ROP_change': 'rop_change',
            'RPM_change': 'rpm_change',
            'Torque_change': 'torque_change',
            'SPP_change': 'spp_change',
            'Flow_Rate_change': 'flow_rate_change'
        }
        
        # Create dictionary with database column names
        db_dict = {}
        for key, value in data_dict.items():
            if key in mapping:
                db_dict[mapping[key]] = value
            elif key.lower() in cls.__table__.columns.keys():
                db_dict[key.lower()] = value
        
        # Handle timestamp conversion
        if 'timestamp' in data_dict and isinstance(data_dict['timestamp'], str):
            try:
                db_dict['timestamp'] = datetime.strptime(data_dict['timestamp'], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                # If timestamp format is different, use current time
                db_dict['timestamp'] = datetime.utcnow()
        
        # Create model instance
        return cls(**db_dict)


class Prediction(Base):
    """Model for storing ML agent predictions."""
    __tablename__ = 'predictions'
    
    id = Column(Integer, primary_key=True)
    drilling_data_id = Column(Integer, ForeignKey('drilling_data.id'), index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    agent_type = Column(String(50), index=True)  # mechanical_sticking, differential_sticking, etc.
    probability = Column(Float)
    
    # For optimization models
    is_optimization = Column(Boolean, default=False)
    recommended_parameters = Column(JSON, nullable=True)
    expected_improvement = Column(Float, nullable=True)
    
    # For washout/mud losses model
    issue_type = Column(String(50), nullable=True)  # Washout or Mud Losses
    
    # Detailed prediction data
    contributing_factors = Column(JSON, nullable=True)
    recommendations = Column(JSON, nullable=True)
    
    # Relationships
    drilling_data = relationship("DrillingData", back_populates="predictions")
    alerts = relationship("Alert", back_populates="prediction", cascade="all, delete-orphan")
    
    def to_dict(self):
        """Convert model to dictionary."""
        result = {
            'id': self.id,
            'timestamp': self.timestamp.strftime("%Y-%m-%d %H:%M:%S") if self.timestamp else None,
            'agent_type': self.agent_type,
            'probability': self.probability
        }
        
        # Add optional fields if they exist
        if self.is_optimization:
            result['is_optimization'] = True
            
            if self.recommended_parameters:
                try:
                    if isinstance(self.recommended_parameters, str):
                        result['recommended_parameters'] = json.loads(self.recommended_parameters)
                    else:
                        result['recommended_parameters'] = self.recommended_parameters
                except:
                    result['recommended_parameters'] = {}
                    
            if self.expected_improvement:
                result['expected_rop_improvement'] = self.expected_improvement
                
        if self.issue_type:
            result['issue_type'] = self.issue_type
            
        if self.contributing_factors:
            try:
                if isinstance(self.contributing_factors, str):
                    result['contributing_factors'] = json.loads(self.contributing_factors)
                else:
                    result['contributing_factors'] = self.contributing_factors
            except:
                result['contributing_factors'] = []
                
        if self.recommendations:
            try:
                if isinstance(self.recommendations, str):
                    result['recommendations'] = json.loads(self.recommendations)
                else:
                    result['recommendations'] = self.recommendations
            except:
                result['recommendations'] = []
        
        return result
    
    @classmethod
    def from_dict(cls, data_dict, agent_type, drilling_data_id=None):
        """Create model from dictionary."""
        # Basic data
        model_data = {
            'agent_type': agent_type,
            'probability': data_dict.get('probability', 0),
            'drilling_data_id': drilling_data_id
        }
        
        # Handle timestamp
        if 'timestamp' in data_dict and isinstance(data_dict['timestamp'], str):
            try:
                model_data['timestamp'] = datetime.strptime(data_dict['timestamp'], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                # If timestamp format is different, use current time
                model_data['timestamp'] = datetime.utcnow()
        
        # Handle optimization models
        if data_dict.get('is_optimization', False):
            model_data['is_optimization'] = True
            
            if 'recommended_parameters' in data_dict:
                model_data['recommended_parameters'] = data_dict['recommended_parameters']
                
            if 'expected_rop_improvement' in data_dict:
                model_data['expected_improvement'] = data_dict['expected_rop_improvement']
        
        # Handle washout/mud losses models
        if 'issue_type' in data_dict:
            model_data['issue_type'] = data_dict['issue_type']
        
        # Handle detailed prediction data
        if 'contributing_factors' in data_dict:
            model_data['contributing_factors'] = data_dict['contributing_factors']
            
        if 'recommendations' in data_dict:
            model_data['recommendations'] = data_dict['recommendations']
        
        # Create model instance
        return cls(**model_data)


class Alert(Base):
    """Model for storing alerts generated from predictions."""
    __tablename__ = 'alerts'
    
    id = Column(Integer, primary_key=True)
    prediction_id = Column(Integer, ForeignKey('predictions.id'), index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    alert_type = Column(String(100))  # e.g., "Mechanical Sticking Risk"
    severity = Column(String(20))  # HIGH, MEDIUM, LOW
    probability = Column(String(10))
    message = Column(Text)
    recommendation = Column(Text)
    acknowledged = Column(Boolean, default=False)
    
    # Relationships
    prediction = relationship("Prediction", back_populates="alerts")
    
    def to_dict(self):
        """Convert model to dictionary."""
        return {
            'id': self.id,
            'timestamp': self.timestamp.strftime("%Y-%m-%d %H:%M:%S") if self.timestamp else None,
            'type': self.alert_type,
            'severity': self.severity,
            'probability': self.probability,
            'message': self.message,
            'recommendation': self.recommendation,
            'acknowledged': self.acknowledged
        }
    
    @classmethod
    def from_dict(cls, data_dict, prediction_id=None):
        """Create model from dictionary."""
        model_data = {
            'prediction_id': prediction_id,
            'alert_type': data_dict.get('type', ''),
            'severity': data_dict.get('severity', 'MEDIUM'),
            'probability': data_dict.get('probability', '0%'),
            'message': data_dict.get('message', ''),
            'recommendation': data_dict.get('recommendation', ''),
            'acknowledged': data_dict.get('acknowledged', False)
        }
        
        # Handle timestamp
        if 'timestamp' in data_dict and isinstance(data_dict['timestamp'], str):
            try:
                model_data['timestamp'] = datetime.strptime(data_dict['timestamp'], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                # If timestamp format is different, use current time
                model_data['timestamp'] = datetime.utcnow()
        
        # Create model instance
        return cls(**model_data)