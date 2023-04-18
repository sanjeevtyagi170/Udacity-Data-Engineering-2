import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table  if exists staging_songs"
songplay_table_drop = "drop table  if exists songplays"
user_table_drop = "drop table  if exists users"
song_table_drop = "drop table  if exists songs"
artist_table_drop = "drop table  if exists artists"
time_table_drop = "drop table  if exists time"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events(
artist varchar,
auth varchar,
firstname varchar(50),
gender char,
iteminsession integer,
lastname varchar(50),
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration float,
sessionid integer,
song varchar,
status integer,
ts bigint,
userAgent varchar,
userid integer
)
""")

staging_songs_table_create = ("""
create table staging_songs(
num_songs int,
artist_id varchar,
artist_latitude float,
artist_longitude float,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration float,
year int
)
""")

songplay_table_create = ("""
create table if not exists songplays(
songplay_id INTEGER IDENTITY (1, 1) PRIMARY KEY,
start_time timestamp,
user_id int,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location varchar,
user_agent varchar
)

""")

user_table_create = ("""
create table if not exists users(
user_id int,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
)

""")

song_table_create = ("""

create table if not exists songs(
song_id varchar,
title varchar,
artist_id varchar,
year int,
duration float

)
""")

artist_table_create = ("""
create table if not exists artists(
artist_id varchar,
name varchar,
location varchar,
latitude float,
longitude float
)
""")

time_table_create = ("""
create table if not exists time(
start_time timestamp,
hour int,
day int,
week int,
month int,
year int,
weekday varchar

)

""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events
    from {0}
    iam_role {1}
    json {2};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
   copy staging_songs
    from {0}
    iam_role {1}
    format as json 'auto';
""").format(SONG_DATA, ARN)


# FINAL TABLES

songplay_table_insert = ("""
insert into songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
select DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
se.userId,
se.level,
ss.song_id,
ss.artist_id,
se.sessionId,
se.location,
se.userAgent
FROM staging_songs ss
INNER JOIN staging_events se
ON (ss.title = se.song AND se.artist = ss.artist_name)
AND se.page = 'NextSong';

""")

user_table_insert = ("""
insert into users(user_id,first_name,last_name,gender,level)
select distinct userid, firstname,lastname,gender,level
from staging_events
where page = 'NextSong' and userId is not null
""")

song_table_insert = ("""
insert into songs(song_id,title,artist_id,year,duration)
select distinct song_id,title,artist_id,year,duration
from staging_songs
where song_id is not null
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude)
select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
from staging_songs
where artist_id is not null
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select distinct timestamp 'epoch' + ts/1000 * INTERVAL '1 second'  AS start_time, 
extract(hour from start_time), 
extract(day from start_time), 
extract(week from start_time), 
extract(month from start_time), 
extract(year from start_time), 
extract(weekday from start_time)
from staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
