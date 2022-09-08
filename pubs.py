from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
import json

def create_topic(projectid,topicid):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(projectid,topicid)

    topic = publisher.create_topic(request={"name": topic_path})

    print(f"Created topic: {topic.name}")

def read_message(projectid,subid,jsonname):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(projectid, subid)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {(message.data).decode('utf8')}")
        message.ack()
        file=json.dumps(json.loads((message.data).decode('utf8')))
        with open(jsonname,'a') as jsonf:
            jsonf.write(file)
            jsonf.write('\n')
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
            streaming_pull_future.result(timeout=5)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()

read_message("lateral-raceway-360606","nilay-sub2","myjson_file_from_sub3.json")