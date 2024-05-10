# Import the dependencies.
import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import pandas as pd
import datetime as dt

from flask import Flask, jsonify
from datetime import datetime

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii_3.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
measurement  = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################
@app.route("/")
def landing():
    return(
        f"Welcome to the Hawaiian Weather API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start>/end/<end><br/>"
        f"/api/v1.0/<start><br/>"
    )

@app.route("/api/v1.0/precipitation")
def last_year_of_rain():
    # Starting from the most recent data point in the database. 
    most_recent = (dt.date(2017,8,23))
    # Calculate the date one year from the last date in data set.
    year_ago = most_recent - dt.timedelta(days=365)
    #print(year_ago)

    # Perform a query to retrieve the data and precipitation scores
    rain_data = session.query(measurement.date,measurement.prcp)\
        .filter(measurement.date > year_ago)\
        .filter(measurement.date < most_recent).all()

    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    rain_frame = pd.DataFrame(rain_data, columns=['date','rainfall'])

    #convert the rain_frame dataframe into a dictionary
    rain_dictionary = rain_frame.set_index('date')['rainfall'].to_dict()

    #return the jsonified version of the dictionary
    return jsonify(rain_dictionary)
    

@app.route("/api/v1.0/stations")
def station_list():
    #query retrievies the information from the station table
    station_results = session.query(station.station,station.name,station.latitude,station.longitude,station.elevation).all()
    
    #convert list of tuples into normal list (do we need this??)
    all_stations = list(np.ravel(station_results))

    #return the jsonified version of the list
    return jsonify(all_stations)
    

@app.route("/api/v1.0/tobs")
def observed_temps():
    #most recently observed date
    most_recent = (dt.date(2017,8,23))

    #finds the date one year before the most recent date
    year_ago = most_recent - dt.timedelta(days=365)

    # Design a query to find the most active stations (i.e. which stations have the most rows?)
    # List the stations and their counts in descending order.
    station_record_count = session.query(measurement.station,func.count(measurement.date)).\
        group_by(measurement.station).\
        order_by(func.count(measurement.date).desc()).all()

    #variable stores a dynamic reference to the most active station
    most_active_station = station_record_count[0][0]

    #this query filters for the temperature data of the last year for the most active station
    most_active_temp = session.query(measurement.date,measurement.tobs).\
    filter(measurement.station == most_active_station).\
    filter(measurement.date > year_ago).\
    filter(measurement.date < most_recent).all()

    #convert list of tuples into normal list (do we need this??)
    last_year_temps = list(np.ravel(most_active_temp))

    #return the jsonified version of the list
    return jsonify(last_year_temps)
   



@app.route("/api/v1.0/<start>")
def temp_from_start(start):
    sel = [
    func.min(measurement.tobs),
    func.max(measurement.tobs),
    func.avg(measurement.tobs)
    ]
    start_date = start

    rain_date_data = session.query(*sel)\
    .filter(measurement.date >= start_date).all()
    
    data_for_dates = list(np.ravel(rain_date_data))

    return jsonify(data_for_dates)

@app.route("/api/v1.0/<start>/end/<end>")
def temp_from_start_to_end(start,end):
    sel = [
    func.min(measurement.tobs),
    func.max(measurement.tobs),
    func.avg(measurement.tobs)
    ]
    start_date = start

    if end != '':
        end_date = end
        print(end_date)
        rain_date_data = session.query(*sel)\
        .filter(measurement.date >= start_date)\
        .filter(measurement.date <= end_date).all()
    else:
        rain_date_data = session.query(*sel)\
        .filter(measurement.date >= start_date).all()
    
    data_for_dates = list(np.ravel(rain_date_data))

    return jsonify(data_for_dates)

if __name__=="__main__":
    print("Run the application")
    app.run(debug=True)
