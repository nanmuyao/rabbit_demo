import asyncio
import aio_pika
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def callback(message: aio_pika.IncomingMessage):
    async with message.process():
        logging.info(f"Received {message.body} with priority {message.priority}")
        await asyncio.sleep(1)  # 添加延迟，单位为秒
        # await message.ack()
        # await message.nack(requeue=True)  # 显式拒绝消息并重新放回队列
        logging.info("Message not acknowledged")


async def main():
    connection = await aio_pika.connect_robust("amqp://rabbitmq/")
    channel = await connection.channel()

    await channel.set_qos(prefetch_count=1)
    queue = await channel.declare_queue(
        "priority_queue", arguments={"x-max-priority": 10}
    )

    await queue.consume(callback, no_ack=False)

    print("Waiting for messages. To exit press CTRL+C")
    try:
        await asyncio.Future()  # Run forever
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    finally:
        await connection.close()
        logging.info("Connection closed")


if __name__ == "__main__":
    asyncio.run(main())
