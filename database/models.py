"""
Database models for the drilling prediction application.

This module defines the database models for storing drilling data,
predictions, alerts, and recommendations.
"""

import json
from datetime import datetime
from sqlalchemy import Column, Integer, Float, String, DateTime, Text, Boolean, ForeignKey, JSON
from sqlalchemy.orm import relationship
from database.connection import Base


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
    
    # Derived values and statistics
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
            'Hook_Load': self.hook_load,
            'MSE': self.mse,
            'Drag_Factor': self.drag_factor,
            'differential_pressure': self.differential_pressure,
            'hole_cleaning_index': self.hole_cleaning_index,
            'WOB_avg': self.wob_avg,
            'WOB_std': self.wob_std,
            'WOB_rate': self.wob_rate,
            'ROP_avg': self.rop_avg,
            'ROP_std': self.rop_std,
            'ROP_rate': self.rop_rate,
            'RPM_avg': self.rpm_avg,
            'RPM_std': self.rpm_std,
            'RPM_rate': self.rpm_rate,
            'Torque_avg': self.torque_avg,
            'Torque_std': self.torque_std,
            'Torque_rate': self.torque_rate,
            'SPP_avg': self.spp_avg,
            'SPP_std': self.spp_std,
            'SPP_rate': self.spp_rate,
            'Flow_Rate_avg': self.flow_rate_avg,
            'Flow_Rate_std': self.flow_rate_std,
            'Flow_Rate_rate': self.flow_rate_rate
        }
    
    @classmethod
    def from_dict(cls, data_dict):
        """Create model from dictionary."""
        drilling_data = cls()
        
        drilling_data.timestamp = datetime.strptime(data_dict.get('timestamp'), "%Y-%m-%d %H:%M:%S") if data_dict.get('timestamp') else datetime.utcnow()
        drilling_data.depth = data_dict.get('depth')
        drilling_data.wob = data_dict.get('WOB')
        drilling_data.rop = data_dict.get('ROP')
        drilling_data.rpm = data_dict.get('RPM')
        drilling_data.torque = data_dict.get('Torque')
        drilling_data.spp = data_dict.get('SPP')
        drilling_data.flow_rate = data_dict.get('Flow_Rate')
        drilling_data.ecd = data_dict.get('ECD')
        drilling_data.hook_load = data_dict.get('Hook_Load')
        drilling_data.mse = data_dict.get('MSE')
        drilling_data.drag_factor = data_dict.get('Drag_Factor')
        drilling_data.differential_pressure = data_dict.get('differential_pressure')
        drilling_data.hole_cleaning_index = data_dict.get('hole_cleaning_index')
        
        # Derived values and statistics
        drilling_data.wob_avg = data_dict.get('WOB_avg')
        drilling_data.wob_std = data_dict.get('WOB_std')
        drilling_data.wob_rate = data_dict.get('WOB_rate')
        drilling_data.rop_avg = data_dict.get('ROP_avg')
        drilling_data.rop_std = data_dict.get('ROP_std')
        drilling_data.rop_rate = data_dict.get('ROP_rate')
        drilling_data.rpm_avg = data_dict.get('RPM_avg')
        drilling_data.rpm_std = data_dict.get('RPM_std')
        drilling_data.rpm_rate = data_dict.get('RPM_rate')
        drilling_data.torque_avg = data_dict.get('Torque_avg')
        drilling_data.torque_std = data_dict.get('Torque_std')
        drilling_data.torque_rate = data_dict.get('Torque_rate')
        drilling_data.spp_avg = data_dict.get('SPP_avg')
        drilling_data.spp_std = data_dict.get('SPP_std')
        drilling_data.spp_rate = data_dict.get('SPP_rate')
        drilling_data.flow_rate_avg = data_dict.get('Flow_Rate_avg')
        drilling_data.flow_rate_std = data_dict.get('Flow_Rate_std')
        drilling_data.flow_rate_rate = data_dict.get('Flow_Rate_rate')
        
        return drilling_data


class Prediction(Base):
    """Model for storing ML agent predictions."""
    __tablename__ = 'predictions'
    
    id = Column(Integer, primary_key=True)
    drilling_data_id = Column(Integer, ForeignKey('drilling_data.id'), index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    agent_type = Column(String(50), index=True)  # mechanical_sticking, differential_sticking, etc.
    probability = Column(Float)
    
    # For ROP optimization agent
    is_optimization = Column(Boolean, default=False)
    recommended_parameters = Column(JSON, nullable=True)
    expected_improvement = Column(Float, nullable=True)
    
    # For washout/mud losses agent
    issue_type = Column(String(50), nullable=True)  # Washout or Mud Losses
    
    # For all prediction types
    contributing_factors = Column(JSON, nullable=True)
    recommendations = Column(JSON, nullable=True)
    
    # Relationships
    drilling_data = relationship("DrillingData", back_populates="predictions")
    alerts = relationship("Alert", back_populates="prediction", cascade="all, delete-orphan")
    
    def to_dict(self):
        """Convert model to dictionary."""
        return {
            'id': self.id,
            'drilling_data_id': self.drilling_data_id,
            'timestamp': self.timestamp.strftime("%Y-%m-%d %H:%M:%S") if self.timestamp else None,
            'agent_type': self.agent_type,
            'probability': self.probability,
            'is_optimization': self.is_optimization,
            'recommended_parameters': self.recommended_parameters,
            'expected_improvement': self.expected_improvement,
            'issue_type': self.issue_type,
            'contributing_factors': self.contributing_factors,
            'recommendations': self.recommendations
        }
    
    @classmethod
    def from_dict(cls, data_dict, agent_type, drilling_data_id=None):
        """Create model from dictionary."""
        prediction = cls()
        
        prediction.drilling_data_id = drilling_data_id
        prediction.timestamp = datetime.strptime(data_dict.get('timestamp'), "%Y-%m-%d %H:%M:%S") if data_dict.get('timestamp') else datetime.utcnow()
        prediction.agent_type = agent_type
        prediction.probability = data_dict.get('probability', 0.0)
        
        # ROP optimization specific fields
        prediction.is_optimization = (agent_type == 'rop_optimization')
        prediction.recommended_parameters = data_dict.get('recommended_parameters')
        prediction.expected_improvement = data_dict.get('expected_rop_improvement')
        
        # Washout/mud losses specific fields
        prediction.issue_type = data_dict.get('issue_type')
        
        # Common fields
        prediction.contributing_factors = data_dict.get('contributing_factors', [])
        prediction.recommendations = data_dict.get('recommendations', [])
        
        return prediction


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
            'prediction_id': self.prediction_id,
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
        alert = cls()
        
        alert.prediction_id = prediction_id
        alert.timestamp = datetime.strptime(data_dict.get('timestamp'), "%Y-%m-%d %H:%M:%S") if data_dict.get('timestamp') else datetime.utcnow()
        alert.alert_type = data_dict.get('type', '')
        alert.severity = data_dict.get('severity', 'LOW')
        alert.probability = data_dict.get('probability', '0.0')
        alert.message = data_dict.get('message', '')
        alert.recommendation = data_dict.get('recommendation', '')
        alert.acknowledged = data_dict.get('acknowledged', False)
        
        return alert