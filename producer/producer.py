import asyncio
import aio_pika
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def main(producer_id):
    logging.info(f"Starting producer {producer_id}...")

    try:
        logging.info(f"Producer {producer_id} connecting to RabbitMQ...")
        connection = await aio_pika.connect_robust("amqp://rabbitmq/")
        logging.info(f"Producer {producer_id} connected to RabbitMQ")
        channel = await connection.channel()

        logging.info(f"Producer {producer_id} declaring queue with priority...")
        await channel.declare_queue('priority_queue', arguments={'x-max-priority': 10})
        logging.info(f"Producer {producer_id} queue declared")

        async def produce_high_priority_messages():
            while True:
                high_priority_message = f'High Priority Message from producer {producer_id}!'
                logging.info(f"Producer {producer_id} publishing high priority message...")
                await channel.default_exchange.publish(
                    aio_pika.Message(
                        body=high_priority_message.encode(),
                        priority=10
                    ),
                    routing_key='priority_queue'
                )
                logging.info(f"Producer {producer_id} sent '{high_priority_message}' with priority 10")
                await asyncio.sleep(2)

        if producer_id == 5:
            asyncio.create_task(produce_high_priority_messages())

        try:
            while True:
                message = f'Hello Priority Queue from producer {producer_id}!'
                logging.info(f"Producer {producer_id} publishing message...")
                await channel.default_exchange.publish(
                    aio_pika.Message(
                        body=message.encode(),
                        priority=5 if producer_id != 5 else 10
                    ),
                    routing_key='priority_queue'
                )
                logging.info(f"Producer {producer_id} sent '{message}' with priority {5 if producer_id != 5 else 10}")

                # Get the length of the queue
                queue = await channel.declare_queue('priority_queue', passive=True)
                queue_length = queue.declaration_result.message_count
                logging.info(f"Queue length: {queue_length}")

                # Sleep for 3 seconds if queue_length > 10, otherwise sleep for 1 second
                if queue_length > 10:
                    await asyncio.sleep(15)
                else:
                    await asyncio.sleep(0.0001)
        except asyncio.CancelledError:
            logging.info(f"Producer {producer_id} stopping...")
        finally:
            await connection.close()
            logging.info(f"Producer {producer_id} connection closed.")
    except Exception as e:
        logging.error(f"Producer {producer_id} error: {e}")

async def run_producers(num_producers):
    await asyncio.gather(
        *(main(i) for i in range(1, num_producers + 1))
    )

if __name__ == "__main__":
    num_producers = 5  # 这里可以设置生产者的数量
    asyncio.run(run_producers(num_producers))