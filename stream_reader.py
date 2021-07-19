from requests import Session
import requests
from os import environ
from time import sleep
import logging
# from concurrent import futures
# from google.cloud.pubsub_v1 import PublisherClient
# from google.cloud.pubsub_v1.publisher.futures import Future



guardian_url = " https://content.guardianapis.com/search"

class PublishToPubsub:
    def __init__(self):
        self.project_id = "mihir_bose_project"
        self.topic_id = "guardian_stream"
        # self.publisher_client = PublisherClient()
        # self.topic_path = self.publisher_client.


    def get_guardian_data(self) -> str:

        params = {
            "api-key" : "d36c74d5-11af-4020-a0d1-988489020d11"
        }
        ses = Session()
        res = ses.get(guardian_url, params=params, stream=True)

        if 200 <= res.status_code <= 400:
            print ("SUCCESS!!!")
            return res.text
        else:
            raise Exception(f"failed to fetch API data - {res.status_code}:{res.text}")


if __name__ == "__main__":

    for i in range(3):
        svc = PublishToPubsub()
        message = svc.get_guardian_data()
        print (message)
        sleep(3)