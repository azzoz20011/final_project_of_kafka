"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

# Kafka Connect writes the stations table to cta.stations.
topic = app.topic("cta.stations", value_type=Station)

# Keep the requested output topic name.
out_topic = app.topic(
    "cta.output_stations",
    partitions=1,
    value_type=TransformedStation,
)

table = app.Table(
    "cta.stations",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def transformstations(stations):
    async for station in stations:
        lines = []
        if station.red:
            lines.append("red")
        if station.blue:
            lines.append("blue")
        if station.green:
            lines.append("green")

        for line in lines:
            transformed = TransformedStation(
                station_id=station.station_id,
                station_name=station.station_name,
                order=station.order,
                line=line,
            )
            table[f"{line}.{station.station_id}"] = transformed
            await out_topic.send(value=transformed)


if __name__ == "__main__":
    app.main()
