#!/usr/bin/env python3
#
# get-cat-facts.py
# An example Python-based Fluvio connector

from fluvio import Fluvio
import requests
import time

WAIT_SECONDS = 10
CAT_FACTS_API = 'https://catfact.ninja/fact'
CAT_FACTS_TOPIC = 'cat-facts-random'

if __name__ == '__main__':
    # Before entering event loop
    # Connect to cluster and create a producer before we enter loop
    fluvio = Fluvio.connect()
    producer = fluvio.topic_producer(CAT_FACTS_TOPIC)

    # Event loop
    while True:
        # Get random cat fact
        catfact = requests.get(CAT_FACTS_API)

        # Save fact
        producer.send_string(catfact.text)

        # Print fact to container logs
        print(catfact.text)

        # Be polite and control the rate we send requests to external API
        time.sleep(WAIT_SECONDS)
