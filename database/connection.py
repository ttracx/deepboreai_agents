"""
Database connection module for the drilling prediction application.

This module handles database connection management and session creation.
"""

import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get database connection URL from environment variables
DATABASE_URL = os.environ.get("DATABASE_URL")

# Create engine and session factory
try:
    engine = create_engine(DATABASE_URL)
    SessionFactory = sessionmaker(bind=engine)
    Session = scoped_session(SessionFactory)
    logger.info("Database connection established successfully")
except Exception as e:
    logger.error(f"Error connecting to database: {str(e)}")
    # Fallback to None for connections - application should handle this gracefully
    engine = None
    Session = None

# Create base class for declarative models
Base = declarative_base()

def get_session():
    """
    Get a database session.
    
    Returns:
        Session: A database session object
    """
    if Session is None:
        logger.error("Cannot create session - database connection failed")
        return None
    return Session()


def init_db():
    """
    Initialize database schema.
    
    Creates all tables defined in the models.
    """
    if engine is None:
        logger.error("Cannot initialize database - connection failed")
        return False
    
    try:
        # Create all tables
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully")
        return True
    except Exception as e:
        logger.error(f"Error creating database tables: {str(e)}")
        return False