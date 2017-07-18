# -*- coding: utf-8 -*-
import os
import time
import logging
from decimal import Decimal
from collections import deque
from datetime import datetime, timedelta

import boto3
from elasticsearch import Elasticsearch

base_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(base_dir, 'storm_detect.log')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s => %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

last_notification_sent = datetime.utcnow() + timedelta(hours=-1)


def search_by_doctype(elastic_search_connection, doc_type, time_hours):
    date_now = datetime.utcnow()
    date = date_now + timedelta(hours=-int(time_hours))
    date = date.strftime("%Y-%m-%d %T")

    logger.info("Initial: {}".format(date))
    logger.info("End: {}".format(date_now))
    query = {
        "sort": [
            {"state.reported.timestamp": {"order": "desc"}},
        ],
        "query": {
            "range": {
                "state.reported.timestamp": {
                    "gte": "{}".format(date),
                    "lte": "now",
                    "format": "yyyy-MM-dd HH:mm:ss"
                }
            }
        }
    }
    data = elastic_search_connection.search(
        index="iot",
        body=query,
        doc_type=doc_type,
        size=500
    )
    return data


def is_decaying(values):
    if len(values) in (0, 1):
        return False
    old_value = None
    for value in reversed(values):
        if old_value and old_value > value:
            return False
        old_value = value
    return True


def is_raising(values):
    if len(values) in (0, 1):
        return False
    old_value = None
    for value in values:
        if old_value and old_value > value:
            return False
        old_value = value
    return True


def send_sns_notification():
    global last_notification_sent

    seconds_ago = (datetime.utcnow() - last_notification_sent).seconds
    if seconds_ago > 600:
        sns_client = boto3.client(
            'sns',
            region_name='us-east-1',
            aws_access_key_id='MY-ACCESS-KEY',  # change to your access key
            aws_secret_access_key='secret',  # change to your secret key
        )
        sns_client.publish(
            TopicArn='arn:aws:sns:us-east-1:ACCOUNT:StormDetected',  # change to your topic ARN
            Message=('Atenção, foi detectado alto aumento de temperatura e baixa '
                     'luminosidade, grande chance de tempestade nos '
                     'próximos minutos.'),
            MessageStructure='string',
            Subject='Alerta de tempestade')

        last_notification_sent = datetime.utcnow()
    else:
        logger.info('Notification not sended: seconds_ago={}'.format(seconds_ago))


def get_average(elastic_search_connection, doc_type, time_hours):
    hits = search_by_doctype(elastic_search_connection, doc_type=doc_type, time_hours=time_hours)
    hits, total = hits['hits'], hits['hits']['total']
    sum_values = sum([hit["_source"]["state"]["reported"][doc_type]
                      for hit in hits['hits']])
    average = Decimal(sum_values) / Decimal(total)
    return average


if __name__ == '__main__':
    while True:
        es = Elasticsearch(
            ['http://hostname-xpto.us-east-1.es.amazonaws.com:80/'],
            verify_certs=True
        )

        import argparse
        parser = argparse.ArgumentParser(description='Run IoT publisher.')
        parser.add_argument('--maxlen', dest='maxlen', action='store',
                            help='maxlen of deque', type=int, metavar='<1|2|..|X>', required=True)
        parser.add_argument('--time-hours', dest='time_hours', action='store',
                            help='time of comparison', type=int, metavar='<24|36|..|X>', required=True)
        args = parser.parse_args()

        maxlen = args.maxlen
        time_hours = args.time_hours
        temperature_values = deque(maxlen=maxlen)
        light_values = deque(maxlen=maxlen)
        while True:
            try:
                average_temperature = get_average(es, 'temperature', time_hours)
                temperature_values.append(average_temperature)

                logger.info("Average temp: {}".format(average_temperature))
                logger.info("Values temp: {}".format(temperature_values))
                logger.info("len: {}".format(len(temperature_values)))

                average_light = get_average(es, 'light', time_hours)
                light_values.append(average_light)

                logger.info("Average light: {}".format(average_light))
                logger.info("Values light: {}".format(light_values))
                logger.info("len: {}".format(len(light_values)))

                if is_decaying(temperature_values) and len(temperature_values) >= maxlen:
                    logger.warning('Temperature is high!')

                if is_raising(light_values) and len(light_values) >= maxlen:
                    logger.warning('Light is down!')

                storm_detected = all([
                    is_decaying(temperature_values),
                    len(temperature_values) >= maxlen,
                    is_raising(light_values),
                    len(light_values) >= maxlen,
                ])

                if storm_detected:
                    msg = ("High darkness and high temperature: "
                           "average_temperature={} temperature_values={!r} "
                           "average_light={} light_values={!r} maxlen={}")
                    logger.critical(msg.format(average_temperature, temperature_values,
                                    average_light, light_values, maxlen))
                    send_sns_notification()

                time.sleep(10)
            except Exception:
                logger.exception('storm_detect exception')
                break
