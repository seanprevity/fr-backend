# extensions.py
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(
    os.getenv("DATABASE_URL"),
    pool_size=10,            # Default connections kept open
    max_overflow=5,          # Extra connections allowed temporarily
    pool_recycle=3600,       # Recycle connections after 1 hour
    pool_pre_ping=True,      # Test connections before use
    pool_timeout=30          # Wait 30 seconds for a connection
)

# Session factory - will create thread-local sessions
Session = scoped_session(sessionmaker(bind=engine))