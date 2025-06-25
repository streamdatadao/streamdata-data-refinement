from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base

# Base model for SQLAlchemy
Base = declarative_base()

class UserActivity(Base):
    __tablename__ = 'user_activities'
    id = Column(Integer, primary_key=True, autoincrement=True)
    address = Column(String, nullable=False)
    profile_name = Column(String, nullable=False)
    start_time = Column(String, nullable=False)
    duration = Column(String, nullable=True)
    attributes = Column(String, nullable=True)
    title = Column(String, nullable=True)
    supplemental_video_type = Column(String, nullable=True)
    device_type = Column(String, nullable=True)
    bookmark = Column(String, nullable=True)
    latest_bookmark = Column(String, nullable=True)
    country = Column(String, nullable=True)
