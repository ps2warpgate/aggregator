import asyncio
import logging.handlers
import os
from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

import aiohttp
import auraxium
from motor import motor_asyncio
from dotenv import load_dotenv
from dataclasses import asdict

from constants import models
from constants.utils import is_docker, CustomFormatter

# Change secrets variables accordingly
if is_docker() is False:  # Use .env file for secrets
    load_dotenv()


APP_VERSION = os.getenv('APP_VERSION', 'dev')
API_KEY = os.getenv('API_KEY', 's:example')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
MONGODB_URL = os.getenv('MONGODB_URL', 'mongodb://localhost:27017')
MONGODB_DB = os.getenv('MONGODB_DB', 'warpgate')


log = logging.getLogger('aggregator')
log.setLevel(LOG_LEVEL)
handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
log.addHandler(handler)


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
_WARPGATE_IDS: dict[int, List[int]] = {
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
_ZONE_NAMES: dict[int, str] = {
    2: "indar",
    4: "hossin",
    6: "amerish",
    8: "esamir",
    344: "oshur",
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


async def get_continents(world_id: int) -> dict:
    """Get continent states from census"""
    async with auraxium.Client(service_id=API_KEY) as client:
        open_zones = await _get_open_zones(client, world_id)
        zone_states = asdict(models.WorldZones(world_id))
        named_open_zones: list[str] = []
        for i in open_zones:
            named_open_zones.append(_ZONE_NAMES[i])
        for zone in named_open_zones:
            zone_states[zone] = 'open'
    return zone_states


async def get_population(world_id: int) -> dict:
    """Get population data from https://agg.ps2.live"""
    url = f'https://agg.ps2.live/population/{world_id}'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            log.debug(response.raise_for_status())
            json = await response.json()

    population = models.WorldPopulation(
        world_id=json['id'],
        total=json['average'],
        nc=json['factions']['nc'],
        tr=json['factions']['tr'],
        vs=json['factions']['vs'],
    )
    return asdict(population)


async def main() -> None:
    log.info(f'Starting aggregator version {APP_VERSION}')

    client = motor_asyncio.AsyncIOMotorClient(MONGODB_URL)
    db = client[MONGODB_DB]
    db.continents.create_index([('world_id', 1)], unique=True, background=True)
    db.population.create_index([('world_id', 1)], unique=True, background=True)
    while True:
        continent_data: list[dict] = []
        population_data: list[dict] = []

        log.info('fetching data...')

        for i in WORLD_IDS.values():
            continent_data.append(await get_continents(i))
            population_data.append(await get_population(i))

        log.info('data fetched, updating database...')

        for data in continent_data:
            result = await db.continents.update_many(
                filter={'world_id': data['world_id']},
                update={'$set': data},
                upsert=False,
                hint=[('world_id', 1)]
            )
            log.debug(f'matched {result.matched_count}, modified {result.modified_count}')

        for data in population_data:
            result = await db.population.update_many(
                filter={'world_id': data['world_id']},
                update={'$set': data},
                upsert=False,
                hint=[('world_id', 1)]
            )
            log.debug(f'matched {result.matched_count}, modified {result.modified_count}')

        log.info('database updated!')

        await asyncio.sleep(60)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except asyncio.exceptions.CancelledError:
        pass
