import logging
import datetime
from base64 import b64decode
from pandas import DataFrame
from json import loads
from google.cloud.storage import Client

class LoadtoStorge:
     def __init__(self, event, context):
          self.event = event
          self.context = context
          self.bucket_name = "guardian_data_storage"

     def get_message_data(self) -> str:

          logging.info(
               f"This function was triggered by message id {self.context.event_id} published at {self.context.timestamp} "
               f"to {self.context.resource['name']}"
          )

          if 'data' in self.event:
               pubsub_message = b64decode(self.event['data']).decode("utf-8")
               logging.info(pubsub_message)
               return pubsub_message
          else:
               logging.error("Incorrect format")
               return ""

     def transform_payload_to_dataframe(self, message: str) -> DataFrame:

          try:
               df = DataFrame(loads(message))
               if not df.empty:
                    logging.info(f"Created dataframe with{df.shape[0]} rows and {df.shape[0]} columns")
               else:
                    logging.warning(f"Created empty dataframe")
               return df
          except Exception as e:
               logging.error(f"Encountered error creating dataframe - {str(e)}")
               raise

     def upload_to_bucket(self, df: DataFrame, file_name: str = 'payload') -> None:

          storage_client = Client()
          bucket = storage_client.bucket(self.bucket_name)
          blob = bucket.blob(f"{file_name}.csv")
          blob.upload_from_string(data=df.to_csv(index=False), content_type="text/csv")
          logging.info(f"File uploaded to {self.bucket_name}")

def process(event, context):

     root = logging.getLogger()
     root.setLevel(logging.INFO)

     svc = LoadtoStorge(event, context)

     message = svc.get_message_data()
     upload_df = svc.transform_payload_to_dataframe(message)
     #payload_timestamp = upload_df['']
     svc.upload_to_bucket(upload_df, 'news_data'+str(datetime.datetime.now()))