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


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("org.chicago.cta.stations", value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic("org.chicago.cta.transformed_station", partitions=1, value_type=TransformedStation)
# TODO: Define a Faust Table
table = app.Table(
    "org.chicago.cta.transformed_station",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)

"""
def get_line_type(station):
    line=""
    if station.red:
        line = "red"
    elif station.blue:
        line = "blue"
    elif station.green:
        line = "green"
    return TransformedStation(station_id=str(station.station_id), 
                              station_name=str(station.station_name), 
                              order=str(station.order),
                              line=line)
"""

@app.agent(topic)
async def station(stations):
    
    async for station in stations:
        line = "red" if station.red else "blue" if station.blue else "green" if station.green else ""
            
        transformed_station = TransformedStation(station_id=str(station.station_id), 
                                                 station_name=str(station.station_name), 
                                                 order=str(station.order),
                                                 line=line)
        
        table[station.station_id] = transformed_station

"""
#await out_topic.send(key=str(s.station_id),
        #                     value=s)
@app.agent(out_topic)
async def transformed_station(transformed_stations):
    
    async for transformed_station in transformed_stations.group_by(TransformedStation.station_id):
        table[transformed_stations.station_id] = transformed_station
"""


if __name__ == "__main__":
    app.main()
