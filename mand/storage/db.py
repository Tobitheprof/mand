from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from mand.config.settings import settings

engine = create_engine(settings.PG_DSN, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, autoflush=False, future=True)
