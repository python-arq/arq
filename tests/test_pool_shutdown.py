import asyncio
import gc
import io
from contextlib import redirect_stderr

from arq.connections import RedisSettings, create_pool


def test_create_pool_close_before_loop_shutdown(test_redis_settings: RedisSettings) -> None:
    async def main() -> None:
        redis = await create_pool(test_redis_settings)
        await redis.ping()
        await redis.close()
        connections = [
            *redis.connection_pool._available_connections,
            *redis.connection_pool._in_use_connections,
        ]
        assert all(getattr(connection, '_writer', None) is None for connection in connections)

    loop = asyncio.new_event_loop()
    stderr = io.StringIO()
    try:
        loop.run_until_complete(main())
    finally:
        with redirect_stderr(stderr):
            loop.close()
            gc.collect()
    assert 'Event loop is closed' not in stderr.getvalue()
