# -*- coding: utf-8 -*-

import collections
import csv
import datetime
import cassandra.cluster
import textwrap
import numpy as np
import json
import itertools

limiteur = lambda generator, limit: (data for _, data in zip(range(limit), generator))

Trip = collections.namedtuple(
    "Trip",
    ("id", "starttime", "polyline"),
)

def read_csv(filename):
    """Read the file.

    Parameters
    ----------
    filename : string
               The file to read.
    """
    with open(filename) as f:
        for row in csv.DictReader(f):
            id = row["TRIP_ID"]
            starttime = datetime.datetime.fromtimestamp(int(row["TIMESTAMP"]))
            polyline = json.loads(row["POLYLINE"])
            if row["MISSING_DATA"] == "False" and len(polyline) > 1:
                yield Trip(id, starttime, polyline)

def _insert_query_by_hour(trip):
    """Build the query to insert datas in the DB.

    Parameters
    ----------
    trip : Trip
           The trip to insert in the DB.

    Return
    ------
    query : string
            The query to execute.
    """
    query = textwrap.dedent(
        f"""
        INSERT INTO taxi_trip_by_hour
        (
            trip_id,
            starttime_year,
            starttime_month,
            starttime_day,
            starttime_hour,
            starttime,
            polyline
        )
        VALUES
        (
            '{trip.id}',
            {trip.starttime.year},
            {trip.starttime.month},
            {trip.starttime.day},
            {trip.starttime.hour},
            '{trip.starttime.isoformat()}',
            {[(point[0], point[1]) for point in trip.polyline]}
        );
        """
    )
    return query

INSERTS_Q = (
    _insert_query_by_hour,
)

class TaxiData(object):
    """To access DB which stores taxi trip data"""
    def __init__(self):
        self._cluster = cassandra.cluster.Cluster()
        self._session = self._cluster.connect("paroisem")

    def insert_datastream(self, stream):
        """Insert datas into the DB.

        Parameters
        ----------
        stream : iterable
                 Iterable where values to insert are taken.
        """
        for trip in stream:
            for q in INSERTS_Q:
                query = q(trip)
                self._session.execute(query)

    def get_trip_one_day_hour(self, dt):
        """
        Get the trip of a given hour and day.

        Parameters
        ----------
        dt : object datetime
             Date of the trips to retrieve.
        """
        query = textwrap.dedent(
            f"""
            SELECT
                trip_id,
                starttime,
                polyline
            FROM
                taxi_trip_by_hour
            WHERE
                    starttime_year={dt.year}
                AND
                    starttime_month={dt.month}
                AND
                    starttime_day={dt.day}
                AND
                    starttime_hour={dt.hour}
            ;
            """
        )
        for r in self._session.execute(query):
            yield Trip(
                r.trip_id,
                r.starttime,
                r.polyline,
            )

    def get_hour_trips_between_dates(self, dt1, dt2, hour):
        """
        Get trip of a given hour between two dates.

        Parameters
        ----------
        dt1 : object datetime
              1rst date of the interval.

        dt2 : object datetime
              2nd date of the interval.
              
        hour : integer
               Hour of the trips to retrieve.
        """
        duration = dt2-dt1
        for day in range(duration.days):
            date = dt1 + datetime.timedelta(days=day)
            date = date.replace(hour=hour)
            for trip in self.get_trip_one_day_hour(date):
                yield trip
