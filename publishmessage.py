from google.cloud import pubsub_v1
project_id = "lateral-raceway-360606"
topic_id = "nilay-topic1"

def publish_message(projectid,topicid,data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(projectid, topicid)
    data = data.encode('utf8')

    future = publisher.publish(topic_path, data)

    future.result()
    print(f"Published messages to {topic_path}.")

"""publish_message(project_id,topic_id,'{"id":1,"name":"abc"}')
publish_message(project_id,topic_id,'{"id":2,"name":"bcd"}')
publish_message(project_id,topic_id,'{"id":3,"name":"cde"}')
publish_message(project_id,topic_id,'{"id":4,"name":"def"}')
publish_message(project_id,topic_id,'{"id":5,"name":"efg"}')"""

publish_message(project_id,topic_id,'{"id":6,"name":"fgh"}')
publish_message(project_id,topic_id,'{"id":7,"name":"ghi"}')
publish_message(project_id,topic_id,'{"id":8,"name":"hij"}')
publish_message(project_id,topic_id,'{"id":9,"name":"ijk"}')
publish_message(project_id,topic_id,'{"id":10,"name":"jkl"}')