import asyncio
import sys

from aio_pika import DeliveryMode, ExchangeType, Message, connect


async def main() -> None:
    # Perform connection
    connection = await connect('amqp://guest:guest@192.168.2.202:5672/')

    async with connection:
        # Create channel
        channel = await connection.channel()

        event_exchange = await channel.declare_exchange(
            name = 'events',
            type = ExchangeType.DIRECT,
        )

        message_body = b' '.join(
            arg.encode() for arg in sys.argv[2:]
        ) or b'Hello World'

        message = Message(
            body = message_body,
            delivery_mode = DeliveryMode.PERSISTENT,
        )

        # Send message
        routing_key = sys.argv[1] if len(sys.argv) > 2 else "info"
        await event_exchange.publish(message = message, routing_key = routing_key)

        print(f'[x] Sent {message.body!r}')

if __name__ == '__main__':
    asyncio.run(main())