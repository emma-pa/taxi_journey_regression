# -*- coding: utf-8 -*-

import functools
import numpy as np
from traitement import TaxiData
import matplotlib.pyplot as plt
import datetime

RT = 6371e3
dist = lambda lon1, lat1, lon2, lat2: (
        2
        * np.pi
        * RT
        * np.sqrt(
            ((lat1 - lat2) / 360) ** 2
            + ((lon1 - lon2) / 360 * np.cos((lat1 + lat2) / 360 * np.pi)) ** 2
        )
)

def T_d_one_trip(trip):
    """
    Calculte the distance and duration of à trip.

    Parameters
    ----------
    trip : tuple Trip
           The trip to use for calculation.

    Return
    ------
    d : float
        Distance calculated whith the trip in m/s.
    T : float
        Duration of the trip in second.
    """
    d = 0
    for n_line in range(len(trip.polyline)-1):
        lon1, lat1 = trip.polyline[n_line]
        lon2, lat2 = trip.polyline[n_line+1]
        d += dist(lon1, lat1, lon2, lat2)
    T = (len(trip.polyline)-1) * 15
    """
    Retourner un tuple ! 
    """
    return np.array([1, d, d**2, T, d*T])

def regression_by_hour(stream):
    """
    Calculte tau and speed for an trips of a precise hour.

    Parameters
    ----------
    stream : iterable
             Iterable where trips are taken.

    Return
    ------
    tau : float
          Estimation of tau for a precise hour, in minutes.

    speed : float
            Estimation of speed for a precise hour, in km/h.
    """
    # Get sums needed for calculation
    sum_1, sum_d, sum_d_2, sum_T, sum_d_T = functools.reduce(lambda x, y: x+y, map(T_d_one_trip, stream))
    d_mean = sum_d/sum_1
    T_mean = sum_T/sum_1
    SdT = sum_d_T/sum_1 - d_mean*T_mean
    Sdd = sum_d_2/sum_1 - d_mean**2
    tau = T_mean - SdT/Sdd*d_mean
    speed = Sdd/SdT
    # Set speed in km/h and tau in minutes
    return (tau/60, speed*36/10)

def regression_between_dates(taxi, d1, d2):
    """
    Calculate estimations of tau and speed for every hour of a day,
    based on trips between two dates.

    Parameters
    ----------
    taxi : object TaxiData
           To conect the DB and get trips to use.

    d1 : object datetime
        Fisrt date of the interval.

    d2 : object datetime
         Last date of the interval.

    Return
    ------
    tau : array-like of shape (1,24)
          Estimations of tau for every hour of a day.

    speed : array-like of shape (1,24)
            Estimations of speed for every hour of a day.
    """
    tau = np.zeros(24)
    speed = np.zeros(24)
    for h in range(24):
        tau[h], speed[h] = regression_by_hour(taxi.get_hour_trips_between_dates(d1, d2, h))
    return (tau, speed)

def view_results(tau, speed):
    """
    Display plots of evolution of tau and speed.

    Parameters
    ----------
    tau : array-like of shape (1,24)
          Estimations of tau for every hour of a day to display in a plot.

    speed : array-like of shape (1,24)
            Estimations of speed for every hour of a day to display in a plot.
    """
    hours = np.arange(24)
    plt.figure(figsize=(20,10))
    plt.subplot(121)
    plt.plot(hours, speed, '-o')
    plt.title('Speed estimations in function of hours of the day')
    plt.subplot(122)
    plt.plot(hours, tau, '-o')
    plt.title('Time of picking up estimations in function of hours of the day')
    plt.savefig('results.png')

taxi = TaxiData()
tau, spee = regression_between_dates(taxi.get_hour_trips_between_dates(datetime.datetime(2013,7,1), datetime.datetime(2013,7,20)))
view_results(tau, speed)
