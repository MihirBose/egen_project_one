from requests import Session
import requests
from os import environ
from time import sleep
import logging
from concurrent import futures
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future



guardian_url = " https://content.guardianapis.com/search"

class PublishToPubsub:
    def __init__(self):
        self.project_id = "egen-project-1"
        self.topic_id = "guardian_stream"
        self.publisher_client = PublisherClient()
        self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []

    def get_guardian_data(self) -> str:

        params = {
            "api-key" : "d36c74d5-11af-4020-a0d1-988489020d11"
        }
        ses = Session()
        res = ses.get(guardian_url, params=params, stream=True)

        if 200 <= res.status_code <= 400:
            logging.info(f"Response - {res.status_code}:{res.text}")
            print (f"SUCCESS!!!")
            return res.text
        else:
            raise Exception(f"failed to fetch API data - {res.status_code}:{res.text}")

    def get_callback(self, publish_future: Future, data: str) -> callable:
        def callback(publish_future):
            try:
                # Wait for 60 seconds for the publish call to succeed.
                logging.info(publish_future.result(timeout=60))
            except futures.TimeoutError:
                logging.error(f"Publishing {data} timed out.")
        return callback

    def publish_message_to_topic(self, message: str) -> None:
        """publish message to a pubsub topic with an error handler"""

        publish_future = self.publisher_client.publish(self.topic_path, message.encode("utf-8"))

        publish_future.add_done_callback(self.get_callback(publish_future, message))
        self.publish_futures.append(publish_future)

        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)
        logging.info(f"Published messages with error handler to {self.topic_path}.")

if __name__ == "__main__":

    for i in range(3):
        print (f"hello!!!")
        svc = PublishToPubsub()
        message = svc.get_guardian_data()
        svc.publish_message_to_topic(message)
        #print (message)
        sleep(1)