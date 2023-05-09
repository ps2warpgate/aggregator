import asyncio
import logging
import logging.handlers
import os
from typing import Dict
import json

import auraxium
from auraxium import event
from auraxium.endpoints import NANITE_SYSTEMS
from dotenv import load_dotenv
from aio_pika import DeliveryMode, ExchangeType, Message, connect

# from utils import is_docker, CustomFormatter
from constants.utils import is_docker, CustomFormatter

# Change secrets variables accordingly
if is_docker() is False:  # Use .env file for secrets
    load_dotenv()


API_KEY = os.getenv('API_KEY') or 's:example'
LOG_LEVEL = os.getenv('LOG_LEVEL') or 'INFO'
REDIS_HOST = os.getenv('REDIS_HOST') or 'localhost'
REDIS_PORT = os.getenv('REDIS_PORT') or 6379
REDIS_DB = os.getenv('REDIS_DB') or 0
REDIS_PASS = os.getenv('REDIS_PASS') or None
RABBITMQ_URL = os.getenv('RABBITMQ_URL') or None


log = logging.getLogger('auraxium')
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


async def sendMessage(event_data: str):
    connection = await connect(RABBITMQ_URL)
    
    async with connection:
        channel = await connection.channel()

        event_exchange = await channel.declare_exchange(
            name = 'events',
            type = ExchangeType.DIRECT,
        )

        message = Message(
            body = event_data,
            content_encoding = 'application/json',
            delivery_mode = DeliveryMode.PERSISTENT,
        )
        log.info('Sending event')
        await event_exchange.publish(message=message, routing_key='metagame')
        log.info(f'Message was: {message.body}')


async def main() -> None:
    async with auraxium.EventClient(service_id=API_KEY, ess_endpoint=NANITE_SYSTEMS) as client:
        @client.trigger(event.MetagameEvent)
        async def metagame_event(evt: event.MetagameEvent) -> None:
            log.info("Event recieved")

            print(f"""
            ESS Data:
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
            # rest_data = await client.get_by_id(ps2.MetagameEvent, evt.metagame_event_id)

            # print(f"""
            # REST Data:
            # Event ID: {rest_data.id}
            # Event Name: {rest_data.name}
            # Description: {rest_data.description}
            # Type: {rest_data.type}
            # XP Bonus: {rest_data.experience_bonus}
            # """)
            # event data as json object

            event_data = {
                'id': evt.metagame_event_id,
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
            
            await sendMessage(bytes(json_event, encoding='utf-8'))
    
    _ = metagame_event


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.create_task(main())
    loop.run_forever()
