






from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from config import build_database_url

DATABASE_URL = build_database_url()

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autoflush=False, autocommit=False, bind=engine)
Base = declarative_base()
