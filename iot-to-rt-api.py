import os
import json
import requests
from azure.eventhub import EventHubConsumerClient

# IoT Hub connection
connection_str = "connection_string_from_portal_or_load_from_local_env"
CONSUMER_CLIENT: EventHubConsumerClient = EventHubConsumerClient.from_connection_string(
    conn_str=connection_str, consumer_group="$Default"
)

# API headers
headers = {
    'Content-Type': 'application/json',
    'access_token': 'access_key'
}

# Checkpoint file
CHECKPOINT_FILE = "checkpoint.json"

# Load the latest checkpoints (per partition)
if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as f:
        checkpoint_offsets = json.load(f)
else:
    checkpoint_offsets = {}

def save_checkpoint(partition_id, offset):
    checkpoint_offsets[partition_id] = offset
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint_offsets, f)

def insert_request(payload: dict):
    url = "https://api.subdomain.domain.com/api/to/insert"
    payload_str = json.dumps({
        "values": "('%s', '%s', '%s')" % (
            payload['key1'],
            payload['key2'],
            json.dumps(payload)
        )
    })

    response = requests.request("POST", url, headers=headers, data=payload_str)
    return response

def on_event(partition_context, event):
    try:
        partition_id = partition_context.partition_id
        event_offset = event.offset

        # Skip if this offset was already processed
        if partition_id in checkpoint_offsets:
            if event_offset <= checkpoint_offsets[partition_id]:
                print(f"Skipping already processed event at offset {event_offset} in partition {partition_id}")
                return

        payload = event.body_as_json()
        response = insert_request(payload)

        if response.status_code == 200:
            print(f"Successfully inserted from partition {partition_id}, offset {event_offset}")
            save_checkpoint(partition_id, event_offset)
        else:
            print(f"Failed to insert data: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"Error handling event: {e}")

    finally:
        partition_context.update_checkpoint(event)

def on_error(partition_context, error):
    print(f"Error in partition {partition_context.partition_id if partition_context else 'N/A'}: {error}")

try:
    with CONSUMER_CLIENT:
        CONSUMER_CLIENT.receive(
            on_event=on_event,
            starting_position="-1",  # "-1" = from beginning of stream if no checkpoint
            on_error=on_error
        )
except Exception as e:
    print("Fatal error in EventHub consumer:", str(e))