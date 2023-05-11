import asyncio
import logging
import logging.handlers
import os
from typing import Dict
import json
import redis.asyncio as redis

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


class Alert:
    def __init__(self) -> None:
        self.is_ready = False

    async def setup(self):
        self.redis_conn = await redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASS
        )
        self.is_ready = True

    async def create(self, world: str, instance_id: int, event_json: str):
        await self.redis_conn.hset(name=world, key=instance_id, value=event_json)

    async def remove(self, world: str, instance_id: int):
        await self.redis_conn.hdel(world, instance_id)


alert = Alert()


async def send_message(event_data: str) -> None:
    rabbit = await connect(RABBITMQ_URL)
    
    async with rabbit:
        channel = await rabbit.channel()

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
    if not alert.is_ready:
        await alert.setup()
    
    async with auraxium.EventClient(service_id=API_KEY, ess_endpoint=NANITE_SYSTEMS) as client:
        @client.trigger(event.MetagameEvent)
        async def on_metagame_event(evt: event.MetagameEvent) -> None:
            log.info("Event recieved")

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
            
            await send_message(bytes(json_event, encoding='utf-8'))

            if evt.metagame_event_state_name == 'started':
                await alert.create(
                    world=WORLD_NAMES[evt.world_id], 
                    instance_id=evt.instance_id, 
                    event_json=json_event
                )
                log.info(f'Created alert {evt.instance_id}')
            elif evt.metagame_event_state_name == 'ended' or 'cancelled':
                await alert.remove(
                    world=WORLD_NAMES[evt.world_id],
                    instance_id=evt.instance_id,
                )
                log.info(f'Removed alert {evt.instance_id}')

    
    _ = on_metagame_event


loop = asyncio.new_event_loop()
loop.create_task(main())
loop.run_forever()


# if __name__ == '__main__':
#     loop = asyncio.new_event_loop()
#     loop.create_task(main())
#     loop.run_forever()
