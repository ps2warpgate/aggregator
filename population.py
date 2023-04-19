import os
import aiohttp
import redis.asyncio as redis
import asyncio
from dotenv import load_dotenv

from .utils import is_docker, log


if is_docker() is False:  # Use .env file for secrets if outside of a container
    load_dotenv()


REDIS_HOST = os.getenv('REDIS_HOST') or 'localhost'
REDIS_PORT = os.getenv('REDIS_PORT') or 6379
REDIS_DB = os.getenv('REDIS_DB') or 0
REDIS_PASS = os.getenv('REDIS_PASS') or None


WORLD_IDS = {
    'connery': 1,
    'miller': 10,
    'cobalt': 13,
    'emerald': 17,
    'jaeger': 19,
    'soltech': 40
}


# def _get_from_api(world_id: int) -> dict:
#     url = f'https://agg.ps2.live/population/{world_id}'
#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#     except requests.RequestException as error:
#         log.error(error)
#     json = response.json()
#     return json

async def _get_from_api(world_id: int) -> dict:
    url = f'https://agg.ps2.live/population/{world_id}'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            log.debug(response.raise_for_status())
            json = await response.json()
    return json


async def database(redis_host=REDIS_HOST,
                   redis_port=REDIS_PORT, 
                   redis_db=REDIS_DB, 
                   redis_pass=REDIS_PASS
    ):
    conn = await redis.Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_pass
    )
    async with conn.pipeline(transaction=True) as pipe:
        #while True:
        for world in WORLD_IDS:
            try:
                pop = await _get_from_api(world_id=WORLD_IDS[world])
            except asyncio.exceptions.CancelledError as e:
                raise SystemExit(e)
            dict_response = {
                'id': pop['id'],
                'average': pop['average'],
                'nc': pop['factions']['nc'],  # Get faction values from nested dictionary
                'tr': pop['factions']['tr'],
                'vs': pop['factions']['vs']
            }
            command = await pipe.hset(  # Create redis command
                name=f'{world}-population', 
                mapping=dict_response).execute()
            assert command  # Execute redis command
            log.debug(f"Updated {world} population")
        await asyncio.sleep(1)


if __name__=='__main__':
    try:
        asyncio.run(database())
    except asyncio.exceptions.CancelledError as e:
        raise SystemExit(e)
