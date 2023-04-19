import asyncio
import logging
import logging.handlers
import os
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

import aiosqlite
import auraxium
# import redis.asyncio as redis
from dotenv import load_dotenv
from utils import log, is_docker


# Change secrets variables accordingly
if is_docker() is False:  # Use .env file for secrets
    load_dotenv()


API_KEY = os.getenv('API_KEY') or 's:example'
LOG_LEVEL = os.getenv('LOG_LEVEL') or 'INFO'
REDIS_HOST = os.getenv('REDIS_HOST') or 'localhost'
REDIS_PORT = os.getenv('REDIS_PORT') or 6379
REDIS_DB = os.getenv('REDIS_DB') or 0
REDIS_PASS = os.getenv('REDIS_PASS') or None


# Configure logging
# class CustomFormatter(logging.Formatter):

#     grey = "\x1b[38;20m"
#     blue = "\x1b[34m"
#     yellow = "\x1b[33;20m"
#     red = "\x1b[31;20m"
#     bold_red = "\x1b[31;1m"
#     reset = "\x1b[0m"
#     format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

#     FORMATS = {
#         logging.DEBUG: grey + format + reset,
#         logging.INFO: blue + format + reset,
#         logging.WARNING: yellow + format + reset,
#         logging.ERROR: red + format + reset,
#         logging.CRITICAL: bold_red + format + reset
#     }

#     def format(self, record):
#         log_fmt = self.FORMATS.get(record.levelno)
#         dt_fmt = '%m/%d/%Y %I:%M:%S'
#         formatter = logging.Formatter(log_fmt, dt_fmt)
#         return formatter.format(record)


# # Create logger
# log = logging.getLogger('census')
# if not LOG_LEVEL:
#     log.setLevel(logging.INFO)
# else:
#     log.setLevel(LOG_LEVEL)

# handler = logging.StreamHandler()
# handler.setFormatter(CustomFormatter())
# log.addHandler(handler)

auraxium_log = logging.getLogger('auraxium')

if LOG_LEVEL == 'INFO':
    auraxium_log.setLevel(logging.WARNING)
else:
    auraxium_log.setLevel(LOG_LEVEL)

auraxium_log.addHandler(logging.StreamHandler)

# Setup sqlite
sql_create_connery_table = """ CREATE TABLE IF NOT EXISTS connery (
                                    id integer PRIMARY KEY,
                                    continent text,
                                    status text,
                                    time float
                                ); """
sql_create_miller_table = """ CREATE TABLE IF NOT EXISTS miller (
                                    id integer PRIMARY KEY,
                                    continent text,
                                    status text,
                                    time float
                                ); """

sql_create_cobalt_table = """ CREATE TABLE IF NOT EXISTS cobalt (
                                    id integer PRIMARY KEY,
                                    continent text,
                                    status text,
                                    time float
                                ); """
sql_create_emerald_table = """ CREATE TABLE IF NOT EXISTS emerald (
                                    id integer PRIMARY KEY,
                                    continent text,
                                    status text,
                                    time float
                                ); """
sql_create_jaeger_table = """ CREATE TABLE IF NOT EXISTS jaeger (
                                    id integer PRIMARY KEY,
                                    continent text,
                                    status text,
                                    time float
                                ); """

sql_create_soltech_table = """ CREATE TABLE IF NOT EXISTS soltech (
                                    id integer PRIMARY KEY,
                                    continent text,
                                    status text,
                                    time float
                                ); """

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


async def db_setup():
    try:
        f = open("continents.db", "x")
        f.close
    except FileExistsError:
        log.debug("Found existing database!")
    else:
        log.info("Database not found. Created a new one!")

    async with aiosqlite.connect('continents.db') as db:
        await db.execute(sql_create_connery_table)
        await db.execute(sql_create_miller_table)
        await db.execute(sql_create_cobalt_table)
        await db.execute(sql_create_emerald_table)
        await db.execute(sql_create_jaeger_table)
        await db.execute(sql_create_soltech_table)
        timestamp = time.time()
        rows = [
            ('1', 'amerish', 'closed', timestamp),
            ('2', 'esamir', 'closed', timestamp),
            ('3', 'hossin', 'closed', timestamp),
            ('4', 'indar', 'closed', timestamp),
            ('5', 'oshur', 'closed', timestamp)
            ]
        try:
            for world in WORLD_IDS:
                await db.executemany(f"INSERT INTO {world} VALUES(?, ?, ?, ?);", rows)
            await db.commit()
        except aiosqlite.Error as error:
            if type(error) == aiosqlite.IntegrityError:
                log.debug(error)
            else:
                log.error(error)


async def main(redis_host: str,
               redis_port: int,
               redis_db: int,
               redis_pass: str,
               loop: bool = True,
               ):
    await db_setup()
    # while True:
    async with auraxium.Client(service_id=API_KEY) as client:
        log.debug("Querying API...")
        t = time.perf_counter()
        for i in WORLD_IDS:  # Server names
            server_id = WORLD_IDS[i]  # Save server id
            try:
                # Get open continents of server_id
                open_continents = await _get_open_zones(client, server_id)
            except auraxium.errors.ServerError as ServerError:  # Handle Unknown server error
                log.warning(ServerError)
                break
            # List open continents with names
            named_open_continents: list = []
            for s in open_continents:
                named_open_continents.append(_ZONE_NAMES[s])
            continent_status = {
                'Amerish': 'closed',
                'Esamir': 'closed',
                'Hossin': 'closed',
                'Indar': 'closed',
                'Oshur': 'closed'
            }
            for s in named_open_continents:
                if s in continent_status:
                    continent_status[s] = 'open'
            db = await aiosqlite.connect('continents.db')
            timestamp = time.time()
            server_table = [
                (continent_status['Amerish'], timestamp, '1'),
                (continent_status['Esamir'], timestamp, '2'),
                (continent_status['Hossin'], timestamp, '3'),
                (continent_status['Indar'], timestamp, '4'),
                (continent_status['Oshur'], timestamp, '5')
            ]
            await db.executemany(
                f"UPDATE {i} SET status = ?, time = ? WHERE id = ?;", server_table)
            log.debug(f"Updated {i}")
            await db.commit()
            await db.close()
            await asyncio.sleep(6)
        elapsed = time.perf_counter() - t
        log.debug(f"Query completed in {round(elapsed, 2)}s")
        await asyncio.sleep(1)


# For running census_client.py independently of bot.py for development
if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main(True))  # True: main() runs forever, False: main() runs once
