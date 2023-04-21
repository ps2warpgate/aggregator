import asyncio
import logging
import logging.handlers
import os
from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

import aiohttp
import auraxium
import redis.asyncio as redis
from dotenv import load_dotenv

from utils import is_docker, log

# Change secrets variables accordingly
if is_docker() is False:  # Use .env file for secrets
    load_dotenv()


API_KEY = os.getenv('API_KEY') or 's:example'
LOG_LEVEL = os.getenv('LOG_LEVEL') or 'INFO'
REDIS_HOST = os.getenv('REDIS_HOST') or 'localhost'
REDIS_PORT = os.getenv('REDIS_PORT') or 6379
REDIS_DB = os.getenv('REDIS_DB') or 0
REDIS_PASS = os.getenv('REDIS_PASS') or None


auraxium_log = logging.getLogger('auraxium')

if LOG_LEVEL == 'INFO':
    auraxium_log.setLevel(logging.WARNING)
else:
    auraxium_log.setLevel(LOG_LEVEL)

auraxium_log.addHandler(logging.StreamHandler)


# Server IDs
WORLD_IDS = {
    'connery': 1,
    'miller': 10,
    'cobalt': 13,
    'emerald': 17,
    'jaeger': 19,
    'soltech': 40
}

# A mapping of zone IDs to the region IDs of their warpgates
_WARPGATE_IDS: Dict[int, List[int]] = {
    # Indar
    2: [
        2201,  # Northern Warpgate
        2202,  # Western Warpgate
        2203,  # Eastern Warpgate
    ],
    # Hossin
    4: [
        4230,  # Western Warpgate
        4240,  # Eastern Warpgate
        4250,  # Southern Warpgate
    ],
    # Amerish
    6: [
        6001,  # Western Warpgate
        6002,  # Eastern Warpgate
        6003,  # Southern Warpgate
    ],
    # Esamir
    8: [
        18029,  # Northern Warpgate
        18030,  # Southern Warpgate
        18062,  # Eastern Warpgate
    ],
    # Oshur
    344: [
        18303,  # Northern Flotilla
        18304,  # Southwest Flotilla
        18305,  # Southeast Flotilla
    ],
}

# A mapping of zone IDs to their names since Oshur is not in the API
_ZONE_NAMES: Dict[int, str] = {
    2: "Indar",
    4: "Hossin",
    6: "Amerish",
    8: "Esamir",
    344: "Oshur",
}


def _magic_iter(
        region_data: Dict[str, Any]) -> Iterator[Tuple[int, int]]:
    # DBG returns map data in a really weird data; this iterator just
    # flattens that returned tree into a simple list of (regionId, factionId)
    for row in region_data['Row']:
        row_data = row['RowData']
        yield int(row_data['RegionId']), int(row_data['FactionId'])


async def _get_open_zones(client: auraxium.Client, world_id: int) -> List[int]:

    # Get the queried world
    world = await client.get_by_id(auraxium.ps2.World, world_id)
    if world is None:
        raise RuntimeError(f'Unable to find world: {world_id}')

    # Get the map info for all zones on the given world
    map_data = await world.map(*_WARPGATE_IDS.keys())
    if not map_data:
        raise RuntimeError('Unable to query map endpoint')

    # For each world, check if the owners of the warpgates are the same
    open_zones: List[int] = []
    for zone_map_data in cast(Any, map_data):
        zone_id = int(zone_map_data['ZoneId'])

        owner: Optional[int] = None
        for facility_id, faction_id in _magic_iter(zone_map_data['Regions']):

            # Skip non-warpgate regions
            if facility_id not in _WARPGATE_IDS[zone_id]:
                continue

            if owner is None:
                owner = faction_id
            elif owner != faction_id:
                # Different factions, so this zone is open
                open_zones.append(zone_id)
                break
        else:
            # "break" was never called, so all regions were owned by
            # one faction; zone is closed, nothing to do here
            pass

    return open_zones


# Get population data from agg.ps2.live
async def _get_from_api(world_id: int) -> dict:
    url = f'https://agg.ps2.live/population/{world_id}'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            log.debug(response.raise_for_status())
            json = await response.json()
    return json


async def main():
    conn = await redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASS
    )
    async with conn.pipeline(transaction=True) as pipe:
        while True:
            for i in WORLD_IDS:
                server_id = WORLD_IDS[i]
                async with auraxium.Client(service_id=API_KEY) as client:
                    try:
                        open_continents = await _get_open_zones(client, server_id)
                    except auraxium.errors.ServerError as ServerError:
                        log.warning(ServerError)
                        break
                    except RuntimeError as re:
                        log.error(re)
                        break
                named_open_continents = []
                for s in open_continents:
                    named_open_continents.append(_ZONE_NAMES[s])
                continent_status = {
                    'Amerish': 'closed',
                    'Esamir': 'closed',
                    'Hossin': 'closed',
                    'Indar': 'closed',
                    'Oshur': 'closed',
                }
                for s in named_open_continents:
                    if s in continent_status:
                        continent_status[s] = 'open'
                try:
                    pop = await _get_from_api(world_id=WORLD_IDS[i])
                except asyncio.exceptions.CancelledError as e:
                    raise SystemExit(e)
                json_obj = {
                    "id": pop['id'],
                    "population": {
                        "total": pop['average'],
                        "nc": pop['factions']['nc'],
                        "tr": pop['factions']['tr'],
                        "vs": pop['factions']['vs']
                    },
                    "continents": {
                        "Amerish": continent_status['Amerish'],
                        "Esamir": continent_status['Esamir'],
                        "Hossin": continent_status['Hossin'],
                        "Indar": continent_status['Indar'],
                        "Oshur": continent_status['Oshur']
                    }
                }
                command = await pipe.json().set(i, ".", json_obj).execute()
                assert command
                log.debug(f"Updated {i}")
                await asyncio.sleep(6)
            await asyncio.sleep(30)   


if __name__=='__main__':
    try:
        asyncio.run(main())
    except asyncio.exceptions.CancelledError as e:
        raise SystemExit(e)