--SQL Queries:

--1) Derive the final dataframe which has station level statistics:

%%bigquery station_final_stats

with annual_trips_total as
(
select count(*) as annual_trips_total_network
from  `bigquery-public-data.new_york_citibike.citibike_trips`
where date(starttime) >= '2016-01-01' and date(starttime) <= '2017-12-31'
),

start_trips_station as
(
select start_station_name, count(*) as annual_ridership_station_start
from  `bigquery-public-data.new_york_citibike.citibike_trips`
where date(starttime) >= '2016-01-01' and date(starttime) <= '2017-12-31'
group by 1
),

end_trips_station as
(
select end_station_name, count(*) as annual_ridership_station_end
from  `bigquery-public-data.new_york_citibike.citibike_trips`
where date(starttime) >= '2016-01-01' and date(starttime) <= '2017-12-31'
group by 1
),

station_info as 

(
select name as station_name, latitude, longitude, 
num_bikes_available + num_bikes_disabled as num_bikes, num_docks_available + num_docks_disabled as num_docks
FROM `bigquery-public-data.new_york_citibike.citibike_stations` 
)

select coalesce(A.start_station_name, B.end_station_name) as station_name_
,annual_ridership_station_start
, annual_ridership_station_end
, annual_ridership_station_start + annual_ridership_station_end as annual_ridership_station
, C.annual_trips_total_network
, (annual_ridership_station_start + annual_ridership_station_end)/ C.annual_trips_total_network * 100 as perc_of_total_by_station,
D.*
from start_trips_station as A
full outer join end_trips_station as B on A.start_station_name = B.end_station_name
left join annual_trips_total as C on 1=1 
left join station_info as D on coalesce(A.start_station_name, B.end_station_name) = D.station_name

--2) Derive the closest station to every station: name, distance, lat longitude:

%%bigquery closet_station_name_distance

with temp1 as
(
select A.station_id as A_station_id, A.name as A_station_name, A.latitude as A_latitude, A.longitude as A_longitude,
B.station_id as B_station_id, B.name as B_station_name, B.latitude as B_latitude, B.longitude as B_longitude,
st_distance(
    st_geogpoint(A.longitude, A.latitude),
    st_geogpoint(B.longitude, B.latitude)
  )/1000 as dist_in_kms

from  `bigquery-public-data.new_york_citibike.citibike_stations` as A left join `bigquery-public-data.new_york_citibike.citibike_stations` as B
on A.name != B.name
),

temp2 as 
(
select *, ROW_NUMBER() over (PARTITION by A_station_id order by dist_in_kms desc) as Row_no_dist from temp1
)

select * from temp2 where row_no_dist = 1

--3) Query to find average trip distance:

%%bigquery trip_distance_df

with temp1 as
(
select *,
st_distance(
    st_geogpoint(start_station_longitude, start_station_latitude),
    st_geogpoint(end_station_longitude, end_station_latitude)
  )/1000 as dist_in_kms

from  `bigquery-public-data.new_york_citibike.citibike_trips`
where date(starttime) >= '2016-01-01' and date(starttime) <= '2017-12-31'
)

with temp1 as
(
select *,
st_distance(
    st_geogpoint(start_station_longitude, start_station_latitude),
    st_geogpoint(end_station_longitude, end_station_latitude)
  )/1000 as dist_in_kms

from  `bigquery-public-data.new_york_citibike.citibike_trips`
where date(starttime) >= '2016-01-01' and date(starttime) <= '2017-12-31'
and st_geogpoint(start_station_longitude, start_station_latitude) != st_geogpoint(end_station_longitude, end_station_latitude)
)

select avg(dist_in_kms) as avg_dist from temp1 
#PERCENTILE_CONT(dist_in_kms, 0.5) OVER(PARTITION BY sex) from temp1

--4) Query to find the most popular 20 start station and end station trips where the start point and end point are not the same/notsame:

%%bigquery
SELECT start_station_name, end_station_name, count(*) FROM `bigquery-public-data.new_york_citibike.citibike_trips`
where date(starttime) >= '2016-01-01' and date(starttime) <= '2017-12-31'
and start_station_name = end_station_name
group by 1,2 order by count(*) desc limit 20

%%bigquery
SELECT start_station_name, end_station_name, count(*) FROM `bigquery-public-data.new_york_citibike.citibike_trips`
where date(starttime) >= '2016-01-01' and date(starttime) <= '2017-12-31'
and start_station_name != end_station_name
group by 1,2 order by count(*) desc limit 20

--5) Query to find the most popular end stations and the total no of trips that took place with those end stations, 
-- this indicated these are popular end point of the trip, also getting how many trips take place with these stations as the starting point:

%%bigquery

with popular_ends as
(
SELECT end_station_name, count(*) as trip_vol_as_end FROM `bigquery-public-data.new_york_citibike.citibike_trips`
where date(starttime) >= '2016-01-01' and date(starttime) <= '2017-12-31'
group by 1 order by count(*) desc limit 20
),

t1 as 
(
select * from `bigquery-public-data.new_york_citibike.citibike_trips`
where date(starttime) >= '2016-01-01' and date(starttime) <= '2017-12-31' and
start_station_name in (select distinct(end_station_name) from popular_ends)
),

t2 as 
(
select start_station_name, count(*) as trip_vol_as_start from t1 group by 1
)

select A.* ,B.* from popular_ends as A left join t2 as B on A.end_station_name = B.start_station_name

--6) Query to find the 20 highest used stations from which trips originated and the no of trips:

%%bigquery
SELECT start_station_name, count(*) FROM `bigquery-public-data.new_york_citibike.citibike_trips`
where date(starttime) >= '2016-01-01' and date(starttime) <= '2017-12-31'
group by 1 order by count(*) desc limit 20

--6) Query to find out the stations which are exclusive end stations only, that is no trip originated from these stations, 
-- trips only ended at these stations

%%bigquery end_stations_trips

with end_stations as 
(
SELECT distinct(end_station_name) FROM `bigquery-public-data.new_york_citibike.citibike_trips`
where date(starttime) >= '2016-01-01' and date(starttime) <= '2017-12-31'
and end_station_name not in (select distinct(start_station_name) as end_station_name 
                             FROM `bigquery-public-data.new_york_citibike.citibike_trips`
                            where date(starttime) >= '2016-01-01' and date(starttime) <= '2017-12-31')    
)
,

temp1 as 
(
select * from  `bigquery-public-data.new_york_citibike.citibike_trips`
where date(starttime) >= '2016-01-01' and date(starttime) <= '2017-12-31' 
and end_station_name in (select end_station_name from end_stations)
)

select end_station_name, count(*) from temp1 group by 1