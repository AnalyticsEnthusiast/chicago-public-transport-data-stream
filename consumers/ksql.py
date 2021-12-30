"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

KSQL_STATEMENT = """
CREATE TABLE turnstyle (
    station_id varchar,
    station_name varchar,
    line varchar
) WITH (
    KAFKA_TOPIC='com.udacity.turnstyles',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

CREATE TABLE turnstyle_summary 
        AS 
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
