import asyncio
import sys


from aio_pika import ExchangeType, connect
from aio_pika.abc import AbstractIncomingMessage


async def on_message(message: AbstractIncomingMessage) -> None:
    async with message.process():
        print(f" [x] {message.routing_key}: {message.body}")


async def main():
    # Perform connection
    connection = await connect('amqp://guest:guest@192.168.2.202:5672/')

    async with connection:
        # Create channel
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        severities = sys.argv[1:]

        if not severities:
            sys.stderr.write(
                f"Usage: {sys.argv[0]} [info] [warning] [error]\n"
            )
            sys.exit(1)

        # Declare exchange
        event_exchange = await channel.declare_exchange(
            name='events',
            type=ExchangeType.DIRECT,
        )

        # Declare random queue
        queue = await channel.declare_queue(durable=True)

        for severity in severities:
            await queue.bind(exchange=event_exchange, routing_key=severity)

        # Start listening the random queue
        await queue.consume(callback=on_message)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())