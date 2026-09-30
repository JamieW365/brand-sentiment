from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from typing import AsyncGenerator
from shared.config.settings import get_settings
from functools import lru_cache

@lru_cache  # ensures the engine is only created once and is reused on subsequent calls
def get_engine():  # factory function — only creates the engine when explicitly called
    return create_async_engine(
        get_settings().database_url,  # pulls database URL from settings at call time, not import time
        echo=False,                   # suppresses SQL query logging in normal operation
        pool_size=5,                  # number of persistent connections to maintain
        max_overflow=10,              # additional connections allowed under peak load
    )


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    # create the session factory using the cached engine
    session_factory = async_sessionmaker(
        bind=get_engine(),       # uses the cached engine factory
        class_=AsyncSession,     # produces AsyncSession instances
        expire_on_commit=False,  # keeps data accessible after commit without extra DB round trip
    )
    # use the factory to open a session as a context manager
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()