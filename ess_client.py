import asyncio
import json
import logging
import logging.handlers
import os
from typing import Dict

import auraxium
from auraxium import event
from auraxium.endpoints import NANITE_SYSTEMS
from dotenv import load_dotenv

from constants.utils import CustomFormatter, is_docker
from services import Alert, Rabbit

# Change secrets variables accordingly
if is_docker() is False:  # Use .env file for secrets
    load_dotenv()


APP_VERSION = os.getenv('APP_VERSION') or 'undefined'
API_KEY = os.getenv('API_KEY') or 's:example'
LOG_LEVEL = os.getenv('LOG_LEVEL') or 'INFO'
RABBITMQ_URL = os.getenv('RABBITMQ_URL') or None
REDIS_URL = os.getenv('REDIS_URL') or None


log = logging.getLogger('ess')
log.setLevel(LOG_LEVEL)
handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
log.addHandler(handler)


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


rabbit = Rabbit()
alert = Alert()


async def main() -> None:
    log.info(f'Starting ESS client version: {APP_VERSION}')
    log.info('Starting Services...')
    if not rabbit.is_ready:
        await rabbit.setup(RABBITMQ_URL)
    
    log.info('RabbitMQ Service ready!')

    if not alert.is_ready:
        await alert.setup(REDIS_URL)
    
    log.info('Alert Service ready!')

    async with auraxium.EventClient(service_id=API_KEY, ess_endpoint=NANITE_SYSTEMS) as client:
        log.info('Listening for Census Events...')
        @client.trigger(event.MetagameEvent)
        async def on_metagame_event(evt: event.MetagameEvent) -> None:
            log.info(f'Received {evt.event_name} id: {evt.world_id}-{evt.instance_id}')

            print(f"""
            ESS Data:
            Instance ID: {evt.instance_id}
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

            # event data as json object
            event_data = {
                'event_id': evt.metagame_event_id,
                'state': evt.metagame_event_state_name,
                'world': evt.world_id,
                'zone': evt.zone_id,
                'nc': evt.faction_nc,
                'tr': evt.faction_tr,
                'vs': evt.faction_vs,
                'xp': evt.experience_bonus,
                'timestamp': evt.timestamp.timestamp()
            }
            # Convert to string
            json_event = json.dumps(event_data)
            
            # Publish to RabbitMQ
            await rabbit.publish(bytes(json_event, encoding='utf-8'))
            log.info('Published event')

            # Add or remove from database
            if evt.metagame_event_state_name == 'started':
                await alert.create(
                    world=WORLD_NAMES[evt.world_id], 
                    instance_id=evt.instance_id, 
                    event_json=json_event
                )
                log.info(f'Created alert {evt.world_id}-{evt.instance_id}')
            elif evt.metagame_event_state_name == 'ended' or 'cancelled':
                await alert.remove(
                    world=WORLD_NAMES[evt.world_id],
                    instance_id=evt.instance_id,
                )
                log.info(f'Removed alert {evt.world_id}-{evt.instance_id}')

    
    _ = on_metagame_event


loop = asyncio.new_event_loop()
loop.create_task(main())
try:
    loop.run_forever()
except asyncio.exceptions.CancelledError:
    loop.stop()
except KeyboardInterrupt:
    loop.stop()


# if __name__ == '__main__':
#     loop = asyncio.new_event_loop()
#     loop.create_task(main())
#     loop.run_forever()
