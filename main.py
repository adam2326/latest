# python
import os
import json
from datetime import datetime
import urllib.parse
import urllib.request
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

# google
from google.cloud import storage

# retrieve env vars
bucket_name = os.environ['BUCKET'] #without gs://
api_key = os.environ['API_KEY']
series_id = os.environ['SERIES_ID']
file_type = os.environ['FILE_TYPE']

# build the URL
getVars = {'series_id': series_id, 'api_key': api_key, 'file_type': 'json'}
base_url = 'https://api.stlouisfed.org/fred/series/observations?'
url = base_url + urllib.parse.urlencode(getVars)

# avro setup
schema = """{"namespace": "example.avro",
  "type": "record",
  "name": "Fred",
  "fields": [
    {"name": "date", "type": "int", "logicalType": "date"},
    {"name": "value",  "type": ["float", "null"]}
    ]
  }"""

schema = avro.schema.parse(schema)

# convert data to avro types
def convert_data_types(record):
  # dates
  date_time_str = record["date"]
  date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d')
  days_since_epoch = (datetime(1970,1,1) - date_time_obj).days
  # floats
  data_point = float(record["value"])
  # return
  return days_since_epoch, data_point

def get_datetime():
  #return str like: 2020_10_25__21_34_43
  return datetime.now().strftime("%Y_%m_%d__%H_%M_%S")


# function
def hello_gcs(event, context):
  # set storage client
  client = storage.Client()

  # get bucket
  bucket = client.get_bucket(bucket_name)

  # get the data
  print('URL: {}'.format(url))
  response = urllib.request.urlopen(url)
  data = json.loads(response.read())

  # remove unneeded data AND convert to bytes
  #small_data = json.dumps( data['observations'] ).encode('utf-8')

  # write to local file
  file_name = '{}.{}'.format(series_id, file_type)
  local_path = '/tmp/{}'.format(file_name)
  writer = DataFileWriter(open(local_path, "wb"), DatumWriter(), schema)
  for record in data['observations']:
    days_since_epoch, data_point = convert_data_types(record)
    writer.append({"date": days_since_epoch , "value": data_point })
  writer.close()

  # set Blob
  file_name = '{}_{}.{}'.format(series_id, get_datetime(), file_type)
  blob = storage.Blob(file_name, bucket)

  # upload the file to GCS
  blob.upload_from_filename(local_path)

  print('Event ID: {}'.format(context.event_id))
  print('Event type: {}'.format(context.event_type))
  print("""This Function was triggered by messageId {} published at {}
  """.format(context.event_id, context.timestamp))
