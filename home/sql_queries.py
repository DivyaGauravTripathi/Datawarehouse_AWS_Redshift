import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "Drop table if exists staging_events"
staging_songs_table_drop = "Drop table if exists staging_songs"
songplay_table_drop = "Drop table if exists songplay" 
user_table_drop = "Drop table if exists users"
song_table_drop = "Drop table if exists song"
artist_table_drop = "Drop table if exists artist"
time_table_drop = "Drop table if exists time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE staging_events(
    event_id INT IDENTITY(0,1),
    artist_name VARCHAR(255),
    auth VARCHAR(50),
    user_first_name VARCHAR(255),
    user_gender  VARCHAR(1),
    item_in_session	INTEGER,
    user_last_name VARCHAR(255),
    song_length	DOUBLE PRECISION, 
    user_level VARCHAR(50),
    location VARCHAR(255),	
    method VARCHAR(25),
    page VARCHAR(35),	
    registration VARCHAR(50),	
    session_id	BIGINT,
    song_title VARCHAR(255),
    status INTEGER,  -- SHOULD BE ALWAYS A NUMBER, HTTP STATUS	
    ts varchar,
    user_agent TEXT,	
    user_id VARCHAR(100),
    PRIMARY KEY (event_id));
""")

staging_songs_table_create = ("""Create Table if not exists staging_songs(
num_songs int not null,
artist_id varchar not null,
artist_latitude varchar,
artist_longitude varchar,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration decimal(10),
year int

);
""")

songplay_table_create = ("""
Create Table if not exists songplays (
songplay_id integer identity(0,1) not null sortkey,
start_time  timestamp not null,
user_id     varchar not null distkey,
level       varchar not null,
song_id     varchar not null, 
artist_id   varchar not null,
session_id  varchar not null,
location   varchar null,
user_agent  varchar null
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER                 NOT NULL SORTKEY,
                first_name  VARCHAR(50)             NULL,
                last_name   VARCHAR(80)             NULL,
                gender      VARCHAR(10)             NULL,
                level       VARCHAR(10)             NULL
    ) diststyle all;
""")

song_table_create = (""" 
CREATE TABLE IF NOT EXISTS songs (
                song_id     VARCHAR(50)             NOT NULL SORTKEY,
                title       VARCHAR(500)           NOT NULL,
                artist_id   VARCHAR(50)             NOT NULL,
                year        INTEGER                 NOT NULL,
                duration    DECIMAL(9)              NOT NULL
    );
""") 

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
                artist_id   VARCHAR(50)             NOT NULL SORTKEY,
                name        VARCHAR(500)           NULL,
                location    VARCHAR(500)           NULL,
                latitude    DECIMAL(9)              NULL,
                longitude   DECIMAL(9)              NULL
    ) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
                start_time  TIMESTAMP               NOT NULL SORTKEY,
                hour        SMALLINT                NULL,
                day         SMALLINT                NULL,
                week        SMALLINT                NULL,
                month       SMALLINT                NULL,
                year        SMALLINT                NULL,
                weekday     SMALLINT                NULL
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from '{}'
 credentials 'aws_iam_role={}'
 region 'us-west-2' 
 COMPUPDATE OFF STATUPDATE OFF
 JSON '{}'""").format(config.get('S3','LOG_DATA'),
                        config.get('IAM_ROLE', 'ARN'),
                        config.get('S3','LOG_JSONPATH'))


staging_songs_copy = ("""
Copy staging_events from 's3://udacity-dend/song_data'
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2' format as json 'auto'; 
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""Insert into songplays (
start_time,user_id,level,song_id,artist_id,session_id,location,user_agent
    )
Select DISTINCT 
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,se.user_Id,se.user_level,ss.song_id,ss.artist_id,se.session_Id,se.location,se.user_Agent               
    FROM staging_events AS se
    JOIN staging_songs AS ss
        ON (se.artist_name = ss.artist_name) 
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level)
    SELECT  se.user_Id,se.first_Name,se.last_Name,se.user_gender,se.user_level   
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration)
    SELECT  ss.song_id,ss.title,ss.artist_id,ss.year,ss.duration
    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,latitude,longitude)
    SELECT ss.artist_id,ss.artist_name,ss.artist_location,ss.artist_latitude,ss.artist_longitude
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday)
    SELECT  se.ts,
            EXTRACT(hour FROM start_time),
            EXTRACT(day FROM start_time),
            EXTRACT(week FROM start_time),
            EXTRACT(month FROM start_time),
            EXTRACT(year FROM start_time),
            EXTRACT(week FROM start_time)
    FROM    staging_events AS se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy] 
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
