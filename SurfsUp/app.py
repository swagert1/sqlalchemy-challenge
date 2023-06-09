# Import the dependencies.
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, distinct

from flask import Flask, jsonify
from datetime import datetime
import datetime as dt
from collections import defaultdict

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB

# The session is opened and closed within each route separately

#################################################
# Flask Setup
#################################################

app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():

    session = Session(engine)

    latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    latest = datetime.strptime(latest_date[0], '%Y-%m-%d')
    dt_year_ago = latest - dt.timedelta(days=365)
    year_ago = datetime.date(dt_year_ago) #finding the date one year ago from the last measurement date


    #Query to find the date after the year ago date
    results = session.query(Measurement.date, Measurement.prcp, Measurement.station)\
            .filter(Measurement.date >= year_ago)\
            .order_by(Measurement.date).all()
    
    session.close

    #expected output
    # {
    # "2016-08-23": {
    #     "USC00519397": 0.0,
    #     "USC00513117": 0.15,
    #     "USC00514830": 0.05,
    #     ...
    # },

    rain = defaultdict(dict) #this allows us to generate nested dictionaries

    for date, prcp, station in results:
        rain[date][station] = prcp  #Create a prcp value for each station listed, and for each date

    return jsonify(rain)

@app.route("/api/v1.0/stations")
def stations():

    session = Session(engine)

    results = session.query(distinct(Measurement.station)).all() #Getting the station names

    session.close

    stations = []

    for s in results:
        stations.append(s[0])

    return jsonify(stations)

@app.route("/api/v1.0/tobs")
def temperatures():

    session = Session(engine)

    latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    latest = datetime.strptime(latest_date[0], '%Y-%m-%d')
    dt_year_ago = latest - dt.timedelta(days=365)
    year_ago = datetime.date(dt_year_ago) #Same as above

    active = session.query(Measurement.station, func.count(Measurement.station))\
        .group_by(Measurement.station)\
        .order_by(func.count(Measurement.station).desc()).first() #Getting the most acvtive station

    results = session.query(Measurement.date, Measurement.tobs)\
        .filter(Measurement.date >= year_ago).filter(Measurement.station == active[0])\
        .order_by(Measurement.date).all() #Resticting the query to the most active station over the last year of temp data

    session.close

    temps = []

    for date, tobs in results:
        temp_dict = {}
        temp_dict['Date'] = date
        temp_dict['Temp'] = tobs
        temps.append(temp_dict)

    return jsonify(temps)

@app.route("/api/v1.0/<start>") #User will specifiy the start date as <start> (yyyy-mm-dd format)
def start(start):

    start_date = datetime.strptime(start, '%Y-%m-%d')
    start_date = datetime.date(start_date)

    session = Session(engine)

    #Query gets the min, max, and avg temp over the period of time between the start date and the end of the dataset
    results = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs),\
              func.avg(Measurement.tobs)).filter(Measurement.date >= start_date).all()
    
    session.close

    agg = []

    for min, max, avg in results:
        agg_dict = {}
        agg_dict['Min Temp'] = min
        agg_dict['Max Temp'] = max
        agg_dict['Avg Temp'] = avg
        agg.append(agg_dict)
        
    return jsonify(agg)

@app.route("/api/v1.0/<start>/<end>") #User will specifiy the start date as <start> and end date as <end> (yyyy-mm-dd format)
def start_end(start,end):

    start_date = datetime.strptime(start, '%Y-%m-%d')
    start_date = datetime.date(start_date)

    end_date = datetime.strptime(end, '%Y-%m-%d')
    end_date = datetime.date(end_date)

    session = Session(engine)

    #Same query as above except end date is included in the filter
    results = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs),\
              func.avg(Measurement.tobs)).filter(Measurement.date >= start_date)\
                .filter(Measurement.date <= end_date).all()
    
    session.close

    agg = []

    for min, max, avg in results:
        agg_dict = {}
        agg_dict['Min Temp'] = min
        agg_dict['Max Temp'] = max
        agg_dict['Avg Temp'] = avg
        agg.append(agg_dict)
        
    return jsonify(agg)

if __name__ == '__main__':
    app.run(debug=True)