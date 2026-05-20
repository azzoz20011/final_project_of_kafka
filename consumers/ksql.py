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

KSQL_STATEMENTS = [
    """
CREATE STREAM IF NOT EXISTS turnstile (
    station_id INTEGER,
    station_name STRING,
    line STRING
) WITH (
    KAFKA_TOPIC='cta.turnstile.red.wilson',
    KEY_FORMAT='KAFKA',
    PARTITIONS=1,
    REPLICAS=1,
    VALUE_FORMAT='AVRO'
);
""",
    """
CREATE TABLE IF NOT EXISTS turnstile_summary
WITH (
    KAFKA_TOPIC='TURNSTILE_SUMMARY',
    PARTITIONS=1,
    REPLICAS=1,
    VALUE_FORMAT='JSON'
) AS
SELECT station_id, CAST(COUNT(station_id) AS INTEGER) AS count
FROM turnstile
GROUP BY station_id;
""",
]


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    for statement in KSQL_STATEMENTS:
        resp = requests.post(
            f"{KSQL_URL}/ksql",
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            data=json.dumps(
                {
                    "ksql": statement,
                    "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
                }
            ),
        )

        # Ensure that a 2XX status code was returned
        if resp.status_code >= 400:
            print(resp.text)
        resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
