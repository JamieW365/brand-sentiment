from functools import lru_cache
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from shared.config.settings import get_settings


@lru_cache
def get_engine():
    return create_async_engine(
        get_settings().database_url,
        echo=False,
        pool_size=5,
        max_overflow=10,
    )


@lru_cache
def get_session_factory() -> async_sessionmaker:
    return async_sessionmaker(
        bind=get_engine(),
        class_=AsyncSession,
        expire_on_commit=False,
    )


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with get_session_factory()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
