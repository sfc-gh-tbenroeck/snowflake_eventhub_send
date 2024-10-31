# pip install azure-eventhub
import os
import asyncio
from azure.eventhub.aio import EventHubConsumerClient

# Retrieve environment variables
EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_CONSUMER_GROUP = os.getenv("EVENT_HUB_CONSUMER_GROUP")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

# Optional: Check if values are retrieved successfully
if not EVENT_HUB_CONNECTION_STR:
    raise EnvironmentError("EVENT_HUB_CONNECTION_STR is not set in the environment")
if not EVENT_HUB_CONSUMER_GROUP:
    raise EnvironmentError("EVENT_HUB_CONSUMER_GROUP is not set in the environment")
if not EVENT_HUB_NAME:
    raise EnvironmentError("EVENT_HUB_NAME is not set in the environment")


async def on_event(partition_context, event):
    # Print the event data.
    print(
        'Received the event: "{}" from the partition with ID: "{}"'.format(
            event.body_as_str(encoding="UTF-8"), partition_context.partition_id
        )
    )

async def on_event_batch(partition_context, events):
    # Print the header with the number of events.
    print(f"--- Received new event batch with {len(events)} events ----------------------------")

    # Iterate over the events and print each one.
    for i, event in enumerate(events, start=1):
        print(f"       ---> Event {i}: {event.body_as_str(encoding='UTF-8')}")
    print(f"------------------------------------------------------------------------------------")

async def main():
    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string(
        EVENT_HUB_CONNECTION_STR,
        consumer_group=EVENT_HUB_CONSUMER_GROUP,
        eventhub_name=EVENT_HUB_NAME,
    )

    async with client:
        """
        Start receiving from this event position if there is no checkpoint data for a partition. Checkpoint data will be used if available. This can be a dict with partition ID as the key and position as the value for individual partitions, or a single value for all partitions. The value type can be str, int or datetime.datetime. Also supported are the values "-1" for receiving from the beginning of the stream, and "@latest" for receiving only new events.
        """
        await client.receive_batch(
            on_event_batch=on_event_batch,
            starting_position="@latest",
        )

if __name__ == "__main__":
    asyncio.run(main())
