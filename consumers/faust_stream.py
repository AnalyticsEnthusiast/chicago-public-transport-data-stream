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
    line: str = ""


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("org.chicago.cta.stations", value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic("org.chicago.cta.transformed_station", partitions=1, value_type=TransformedStation)
# TODO: Define a Faust Table
table = app.Table(
    "stations_tbl",
    default=str,
    partitions=1,
    changelog_topic=out_topic,
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#

def get_line_type(station):
    if station.red:
        station.line = "red"
    elif station.blue:
        station.line = "blue"
    elif station.green:
        station.line = "green"
    return station
    

@app.agent(topic)
async def station(stations):
    
    stations.add_processor(get_line_type)
    
    async for s in stations:
        await out_topic.send(key=str(s.station_id), 
                             value=TransformedStation(station_id=str(s.station_id), 
                                                      station_name=str(s.station_name), 
                                                      order=str(s.order),
                                                      line=str(s.line)))
"""        
@app.agent(out_topic)
async def transformed_station(transformed_stations):
    
    async for transformed_station in transformed_stations.group_by():
"""     

if __name__ == "__main__":
    app.main()
