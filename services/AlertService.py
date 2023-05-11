import redis.asyncio as redis


class Alert:
    """Handles database operations for MetagameEvents"""
    def __init__(self) -> None:
        self.is_ready = False

    async def setup(self, redis_url: str):
        """Creates a Redis connection

        Args:
            redis_url: :class:`str` Redis connection URL
        """
        self.redis_conn = await redis.Redis.from_url(
            url=redis_url
        )
        self.is_ready = True

    async def create(self, world: str, instance_id: int, event_json: str):
        """Create a new MetagameEvent instance in the database

        Args:
            world: :class:`str`: the world where the event originated \n
            instance_id: :class:`int` a MetagameEvent instance_id \n
            event_json: :class:`str` event data in JSON
        """
        await self.redis_conn.hset(name=f'{world}-alerts', key=instance_id, value=event_json)

    async def remove(self, world: str, instance_id: int):
        """Remove a MetagameEvent instance from the database

        Args:
            world: :class:`str` the world where the event originated \n
            instance_id: :class:`int` a MetagameEvent instance_id \n
        """
        await self.redis_conn.hdel(f'{world}-alerts', instance_id)

    async def close_connection(self):
        """Closes the Redis connection"""
        await self.redis_conn.close()