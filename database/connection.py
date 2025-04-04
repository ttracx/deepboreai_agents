"""
Database connection module for the drilling prediction application.

This module handles database connection management and session creation.
"""

import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get database URL from environment variable or use default
DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///drilling_data.db')

# Create engine
try:
    engine = create_engine(DATABASE_URL)
    logger.info(f"Database engine created with URL: {DATABASE_URL}")
except Exception as e:
    logger.error(f"Error creating database engine: {str(e)}")
    engine = None

# Create session factory
if engine:
    session_factory = sessionmaker(bind=engine)
    Session = scoped_session(session_factory)
    logger.info("Database session factory created")
else:
    Session = None
    logger.error("No session factory created because engine initialization failed")

# Create base class for declarative models
Base = declarative_base()

def get_session():
    """
    Get a database session.
    
    Returns:
        Session: A database session object
    """
    if Session is None:
        logger.error("Cannot create session, Session factory is None")
        return None
    
    try:
        session = Session()
        return session
    except Exception as e:
        logger.error(f"Error creating session: {str(e)}")
        return None

def init_db():
    """
    Initialize database schema.
    
    Creates all tables defined in the models.
    """
    if engine is None:
        logger.error("Cannot initialize database, engine is None")
        return False
    
    try:
        # Import models here to avoid circular imports
        from database.models import DrillingData, Prediction, Alert
        
        # Create tables
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully")
        return True
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return False