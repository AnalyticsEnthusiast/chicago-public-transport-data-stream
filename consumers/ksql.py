"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"


KSQL_STATEMENT = """
CREATE TABLE turnstyle (
    station_id varchar,
    station_name varchar,
    line varchar
) WITH (
    KAFKA_TOPIC='org.chicago.cta.station.arrivals.turnstyles',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

CREATE TABLE turnstyle_summary 
WITH (
        VALUE_FORMAT='JSON'
    ) AS 
        SELECT 
        station_id,
        count(station_id) as count
    FROM turnstyle
    GROUP BY station_id
    ;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("turnstyle_summary") is True:
        return

    try:
        logging.debug("executing ksql statement...")
        
        resp = requests.post(
            f"{KSQL_URL}/ksql",
            headers={
                "Content-Type": "application/vnd.ksql.v1+json",
                "Accept": "application/vnd.ksql.v1+json"
            },
            data=json.dumps(
                {
                    "ksql": KSQL_STATEMENT,
                    "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
                }
            )
        )
        return
        print(resp)
        # Ensure that a 2XX status code was returned
        resp.raise_for_status()
        
    except Exception as e:
        print(e)
        raise

if __name__ == "__main__":
    execute_statement()
