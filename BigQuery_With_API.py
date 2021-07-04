# -*- coding: utf-8 -*-
"""
Created on Sun Jun 27 17:14:24 2021

@author: Solomon.Benny
"""

""" Initial Imports"""
from flask import Flask
from flask_restful import Resource, Api
from google.cloud import bigquery


import os
""" Service Account Key for this project in Gloud Cloud Console"""
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'nycdatasolutionsolomon.json'



import numpy as np

"""Functional requirements"""
radiusInKm=3.5
lng=-74.0474287
lat=40.6895436


"""1 degree latitude ~ 111.2 km for latitude"""
lat_change = radiusInKm/111.2;

#lon_change = abs(np.cos(lat*(np.pi/180)))
lon_change=radiusInKm / abs(np.cos(lat*(np.pi/180))*111.32)

lat_min = lat - lat_change
lon_min = lng - lon_change
lat_max = lat + lat_change
lon_max = lng + lon_change


print (lat_min,lat_max,lon_min,lon_max)



""" Initializing the app"""
app = Flask(__name__)
api = Api(app)



"""Defining the function"""

class NYCData(Resource):
   def get(self):
        client = bigquery.Client()
        query = """
SELECT
  passenger_count AS passengerCount,
  COUNT(ROW) AS numberOfTrips,
  SUM(trip_duration) AS totalTimeInMinutes,
  CAST (CEIL(SUM(trip_duration)/((24*60)*366)) AS integer) AS numberOfCabsRequired
FROM (
  SELECT
    *
  FROM (
    SELECT
      ROW_NUMBER() OVER(ORDER BY pickup_datetime ASC) AS ROW,
      pickup_datetime,
      dropoff_datetime,
      passenger_count,
      pickup_longitude,
      pickup_latitude,
      dropoff_latitude,
      dropoff_longitude,
      DATE_DIFF(dropoff_datetime, pickup_datetime, minute) AS trip_duration
    FROM
      bigquery-public-data.new_york.tlc_yellow_trips_2016
    WHERE
      (dropoff_latitude>40.658068779856116
        AND dropoff_latitude<40.721018420143885)
      AND (dropoff_longitude>-74.08889359023512
        AND dropoff_longitude<-74.00596380976488)
      AND (pickup_latitude>-90
        AND pickup_latitude<90)
      AND pickup_datetime!=dropoff_datetime
      AND passenger_count>0
      AND passenger_count<7 )
  WHERE
    trip_duration>0)
GROUP BY
  passenger_count
ORDER BY
  passengerCount ASC
                """
                
        job_config = bigquery.QueryJobConfig(
                query_parameters=[
                        bigquery.ScalarQueryParameter("lat_min", "FLOAT", lat_min),
                        bigquery.ScalarQueryParameter("lat_max", "FLOAT", lat_max),
                        bigquery.ScalarQueryParameter("lon_min", "FLOAT", lon_min),
                        bigquery.ScalarQueryParameter("lon_max", "FLOAT", lon_max)]
                )
                
        query_res = client.query(query, job_config = job_config)
        df = query_res.to_dataframe()
        df = df.to_json(orient='records')
        
        return (df)


api.add_resource(NYCData, '/')


if __name__ == '__main__':
    app.run(debug=True,use_reloader=False, port = 12345)


