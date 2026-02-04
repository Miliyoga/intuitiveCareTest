# app/core/db.py
from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

def build_engine(database_url: str):
    return create_engine(
        database_url,
        pool_pre_ping=True,
        future=True,
    )


def build_session_factory(engine) -> sessionmaker[Session]:
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        future=True,
    )


@contextmanager
def db_session(session_factory: sessionmaker[Session]):
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
