import asyncio
import logging
import logging.handlers
import os
from typing import Dict

import auraxium
import redis.asyncio as redis
from auraxium import event
from auraxium.endpoints import NANITE_SYSTEMS
from dotenv import load_dotenv

from utils import is_docker, CustomFormatter

# Change secrets variables accordingly
if is_docker() is False:  # Use .env file for secrets
    load_dotenv()


API_KEY = os.getenv('API_KEY') or 's:example'
LOG_LEVEL = os.getenv('LOG_LEVEL') or 'INFO'
REDIS_HOST = os.getenv('REDIS_HOST') or 'localhost'
REDIS_PORT = os.getenv('REDIS_PORT') or 6379
REDIS_DB = os.getenv('REDIS_DB') or 0
REDIS_PASS = os.getenv('REDIS_PASS') or None


log = logging.getLogger(__name__)
log.setLevel(LOG_LEVEL)
handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
log.addHandler(handler)


auraxium_log = logging.getLogger('auraxium')
auraxium_log.setLevel(LOG_LEVEL)

# if LOG_LEVEL == 'INFO':
#     auraxium_log.setLevel(logging.WARNING)
# else:
#     auraxium_log.setLevel(LOG_LEVEL)


WORLD_NAMES: Dict[int, str] = {
    1: 'connery',
    10: 'miller',
    13: 'cobalt',
    17: 'emerald',
    19: 'jaeger',
    40: 'soltech'
}

ZONE_NAMES: Dict[int, str] = {
    2: 'indar',
    4: 'hossin',
    6: 'amerish',
    8: 'esamir',
    344: 'oshur',
}

METAGAME_STATES: Dict[int, str] = {
    135: 'started',
    136: 'restarted',
    137: 'cancelled',
    138: 'ended',
    139: 'xp_bonus_changed'
}


async def main() -> None:
    async with auraxium.EventClient(service_id=API_KEY, ess_endpoint=NANITE_SYSTEMS) as client:
        @client.trigger(event.MetagameEvent)
        async def metagame_event(evt: event.MetagameEvent) -> None:
            log.info("Event recieved")

            print(f"""
            Event ID: {evt.metagame_event_id}
            State: {evt.metagame_event_state} ({evt.metagame_event_state_name})
            World: {evt.world_id} ({WORLD_NAMES[evt.world_id]})
            Zone: {evt.zone_id} ({ZONE_NAMES[evt.zone_id]})
            NC: {evt.faction_nc}
            TR: {evt.faction_tr}
            VS: {evt.faction_vs}
            XP Bonus: {evt.experience_bonus}
            Timestamp: {evt.timestamp}
            """)
    
    _ = metagame_event


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.create_task(main())
    loop.run_forever()
