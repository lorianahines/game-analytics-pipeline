######################################################################################################################
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
######################################################################################################################

import boto3.session
import json
import random
from random import choice
import time
from datetime import datetime
import uuid
import os
import numpy
import argparse

# Event Payload defaults
DEFAULT_EVENT_VERSION = '1.0.0'
DEFAULT_BATCH_SIZE = 100

def parse_cmd_line():
    """Parse the command line and extract the necessary values."""

    parser = argparse.ArgumentParser(description='Send data to a Kinesis stream for analytics. By default, the script '
                                                 'will send events infinitely. If an input file is specified, the '
                                                 'script will instead read and transmit all of the events contained '
                                                 'in the file and then terminate.')

    # REQUIRED arguments
    kinesis_regions = boto3.session.Session().get_available_regions('kinesis')
    parser.add_argument('--region', required=True, choices=kinesis_regions, type=str,
                        dest='region_name', metavar='kinesis_aws_region',
                        help='The AWS region where the Kinesis stream is located.')
    parser.add_argument('--stream-name', required=True, type=str, dest='stream_name',
                        help='The name of the Kinesis stream to publish to. Must exist in the specified region.')
    parser.add_argument('--application-id', required=True, type=str, dest='application_id',
                        help='The application_id to use when submitting events to ths stream (i.e. You can use the default application for testing).')
    # OPTIONAL arguments
    parser.add_argument('--batch-size', type=int, dest='batch_size', default=DEFAULT_BATCH_SIZE,
                        help='The number of events to send at once using the Kinesis PutRecords API.')
    parser.add_argument('--input-filename', type=str, dest='input_filename',
                        help='Send events from a file rather than randomly generate them. The format of the file'
                             ' should be one JSON-formatted event per line.')

    return parser.parse_args()

# Returns array of UUIDS. Used for generating sets of random event data
def getUUIDs(dataType, count):
    uuids = []
    for i in range(0, count):
        uuids.append(str(uuid.uuid4()))
    return uuids    

# Randomly choose an event type from preconfigured options
def getEventType():
  event_types = {
        1: 'user_signup',
        2: 'login',
        3: 'logout',
        4: 'item_viewed',
        5: 'add_item_to_cart',
        6: 'transaction',
        7: 'search',
        8: 'clicked_ad',
        9: 'reviewed_item'
  }
  return event_types[numpy.random.choice([1, 2, 3, 4, 5, 6, 7, 8, 9], 1, p=[0.11, 0.07, 0.18, 0.16, 0.05, 0.08, 0.19, 0.08, 0.08])[0]]
  
# Generate a randomized event from preconfigured sample data
def getEvent(event_type):
    

    # items = getUUIDs('items', 10)

    items = [
        'coat',
        'boots',
        'swimsuit',
        'scarf',
        'sandals',
        'sweatsuit',
        'joggers',
        'raincoat',
        'jeans',
        'shorts'

    ]
    
    device = [
        'iOS',
        'android',
        'desktop'
    ]
    
    review_comment = [
        'This item was amazing. Saved my life!',
        'Never had clothing as functional as this before. Amazing.',
        'It is decent but could honestly better. You get what you paid for.',
        'Would not buy again.'
        
    ]

    brand_ad = [
        'Beauty Ad',
        'Dog Toys Ad',
        'Sporting Goods Ad'
    ]
    
    switcher = {
        'user_signup': {
            'event_data': {
                'device': str(numpy.random.choice(device, 1, p=[0.3, 0.5, 0.2])[0])
            }
        },

        'login': {
            'event_data': {
                'device': str(numpy.random.choice(device, 1, p=[0.3, 0.5, 0.2])[0]),
                'last_login_time': int(time.time())-random.randint(40000,4000000)
            }
        },
        
        'logout': {
            'event_data': {
                'last_screen_seen': 'the last screen'
            }
        },
        
        'item_viewed': {
             'event_data': {
                'item': str(numpy.random.choice(items, 1, p=[0.125, 0.11, 0.35, 0.125, 0.04, 0.01, 0.07, 0.1, 0.05, 0.02])[0])
            }
        },

        'add_item_to_cart': {
             'event_data': {
                'item': str(numpy.random.choice(items, 1, p=[0.125, 0.11, 0.35, 0.125, 0.04, 0.01, 0.07, 0.1, 0.05, 0.02])[0])
            }
        },

        'transaction': {
            'event_data': {
                'item': str(numpy.random.choice(items, 1, p=[0.125, 0.11, 0.35, 0.125, 0.04, 0.01, 0.07, 0.1, 0.05, 0.02])[0]),
                'item_quantity': random.randint(1,4),
                'currency_amount': random.randint(1,10),
                'transaction_id': str(uuid.uuid4())
            }
        },
        
        'search': {
            'event_data': {
                'item': str(numpy.random.choice(items, 1, p=[0.125, 0.11, 0.35, 0.125, 0.04, 0.01, 0.07, 0.1, 0.05, 0.02])[0]),
                'device': str(numpy.random.choice(device, 1, p=[0.3, 0.5, 0.2])[0])
            }
        },

        'clicked_ad': {
            'event_data': {
                'ad': str(numpy.random.choice(brand_ad, 1, p=[0.3, 0.5, 0.2])[0]),
                'device': str(numpy.random.choice(device, 1, p=[0.3, 0.5, 0.2])[0])
            }
        },

        'reviewed_item': {
            'event_data': {
                'item': str(numpy.random.choice(items, 1, p=[0.125, 0.11, 0.35, 0.125, 0.04, 0.01, 0.07, 0.1, 0.05, 0.02])[0]),
                'comment': str(numpy.random.choice(review_comment, 1, p=[0.2, 0.5, 0.1, 0.2])[0])
            }
        },
        
    }
    
    return switcher[event_type]
    

# Take an event type, get event data for it and then merge that event-specific data with the default event fields to create a complete event
def generate_event():
    event_type = getEventType()
    # Within the demo script the event_name is set same as event_type for simplicity.
    # In many use cases multiple events could exist under a common event type which can enable you to build a richer data taxonomy.
    event_name = event_type
    event_data = getEvent(event_type)
    event = {
        'event_version': DEFAULT_EVENT_VERSION,
        'event_id': str(uuid.uuid4()),
        'event_type': event_type,
        'event_name': event_name,
        'event_timestamp': int(time.time()),
        'app_version': str(numpy.random.choice(['1.0.0', '1.1.0', '1.2.0'], 1, p=[0.05, 0.80, 0.15])[0])
    }
    
    event.update(event_data)
    return event
    
def send_record_batch(kinesis_client, stream_name, raw_records):
    """Send a batch of records to Amazon Kinesis."""

    # Translate input records into the format needed by the boto3 SDK
    formatted_records = []
    for rec in raw_records:
        formatted_records.append({'PartitionKey': rec['event']['event_id'], 'Data': json.dumps(rec)})
    kinesis_client.put_records(StreamName=stream_name, Records=formatted_records)
    print('Sent %d records to stream %s.' % (len(formatted_records), stream_name))

def send_events_infinite(kinesis_client, stream_name, batch_size, application_id):
    """Send a batches of randomly generated events to Amazon Kinesis."""
    
    while True:
        records = []
        # Create a batch of random events to send
        for i in range(0, batch_size):
            event_dict = generate_event()
            record = {
                'event': event_dict,
                'application_id': application_id
            }
            records.append(record)
        send_record_batch(kinesis_client, stream_name, records)
        time.sleep(random.randint(1,7))

if __name__ == '__main__':
    args = parse_cmd_line()
    aws_region = args.region_name
    kinesis_stream = args.stream_name
    batch_size = args.batch_size or DEFAULT_BATCH_SIZE
    application_id = args.application_id
    
    print('===========================================')
    print('CONFIGURATION PARAMETERS:')
    print('- KINESIS_STREAM: ' + kinesis_stream)
    print('- AWS_REGION: ' + aws_region)
    print('- APPLICATION_ID: ' + application_id)
    SERVERS = getUUIDs('servers', 3)
    print('===========================================\n')
    
    session = boto3.Session()
    client = session.client('kinesis', region_name=aws_region)
    
    send_events_infinite(client, kinesis_stream, batch_size, application_id)