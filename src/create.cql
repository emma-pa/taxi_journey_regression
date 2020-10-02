USE paroisem;

DROP TABLE IF EXISTS taxi_trip_by_hour;
CREATE TABLE taxi_trip_by_hour
(
    trip_id varchar,
    starttime_year int,
    starttime_month int,
    starttime_day int,
    starttime_hour int,
    starttime timestamp,
    polyline list<frozen<tuple<float,float>>>,
    primary key ((starttime_year, starttime_month, starttime_day, starttime_hour), trip_id)
);

-- Correction : polyline frozen<list<tuple<float,float>>>
-- ATTENTION : FLOAT SEULEMENT SUR 32 BITS ! UTILISER DOUBLE, entraîne des erreurs de précision
