import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
    CREATE TABLE staging_events (
        event_id        INTEGER IDENTITY(0,1)   PRIMARY KEY,        
        artist          VARCHAR(500)            NULL,
        auth            VARCHAR(20)             NULL,
        first_name      VARCHAR(500)            NULL,
        gender          CHAR(1)                 NULL,
        item_in_session INTEGER                 NULL,
        last_name       VARCHAR(500)            NULL,
        length          FLOAT8                  NULL,
        level           VARCHAR(50)             NULL,
        location        VARCHAR(500)            NULL,
        method          VARCHAR(10)             NULL,
        page            VARCHAR(100)            NULL,
        registration    FLOAT8                  NULL,
        session_id      INTEGER                 NULL,
        song            VARCHAR(500)            NULL DISTKEY,
        status          INTEGER                 NULL,
        ts              DOUBLE PRECISION        NULL,
        user_agent      VARCHAR(500)            NULL,
        user_id         INTEGER                 NULL
        )
"""

staging_songs_table_create = """
    CREATE TABLE staging_songs (
        num_songs           INTEGER         NOT NULL,
        artist_id           VARCHAR(20)     NOT NULL,
        artist_latitude     FLOAT8          NULL,
        artist_longitude    FLOAT8          NULL,
        artist_location     VARCHAR(500)    NULL,
        artist_name         VARCHAR(500)    NOT NULL,
        song_id             VARCHAR(20)     PRIMARY KEY,
        title               VARCHAR(500)    NOT NULL DISTKEY,
        duration            FLOAT8          NULL,
        year                INTEGER         NULL
    )
"""

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id                 INTEGER                 PRIMARY KEY DISTKEY,
    first_name              VARCHAR(500)            NOT NULL,
    last_name               VARCHAR(500)            NOT NULL,
    gender                  CHAR(1)                 NULL,
    level                   varchar(50)             NOT NULL
    )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id                 VARCHAR(20)             PRIMARY KEY,
    title                   VARCHAR(500)            NOT NULL,
    artist_id               VARCHAR(20)             NOT NULL,
    year                    INTEGER                 NULL,
    duration float NOT NULL
    ) DISTSTYLE AUTO
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id               VARCHAR(20)             PRIMARY KEY,
    name                    VARCHAR(500)            NOT NULL,
    location                VARCHAR(500)            NULL,
    latitude                FLOAT8                  NULL,
    longitude               FLOAT8                  NULL
    ) DISTSTYLE ALL
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time              TIMESTAMP               PRIMARY KEY,
    hour                    INTEGER                 NOT NULL,
    day                     INTEGER                 NOT NULL,
    week                    INTEGER                 NOT NULL,
    month                   INTEGER                 NOT NULL,
    year                    INTEGER                 NOT NULL,
    weekday                 INTEGER                 NOT NULL
    ) DISTSTYLE ALL
""")


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id             INTEGER IDENTITY(0,1)           PRIMARY KEY,
    start_time              TIMESTAMP                       NOT NULL REFERENCES time (start_time),
    user_id                 INTEGER                         NOT NULL REFERENCES users (user_id) DISTKEY,
    level                   VARCHAR(50)                     NOT NULL,
    song_id                 VARCHAR(20)                     REFERENCES songs (song_id),
    artist_id               VARCHAR(20)                     REFERENCES artists (artist_id),
    session_id              INTEGER                         NOT NULL,
    location                VARCHAR(200)                    NULL,
    user_agent              VARCHAR(500)                    NOT NULL
    )
""")

# LOAD DATA TO STAGING TABLES

staging_events_copy = (
    """
    COPY staging_events FROM '{}/'
    iam_role '{}'
    json '{}'
    region 'us-west-2';
"""
).format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = (
    """
    COPY staging_songs FROM '{}/'
    iam_role '{}'
    json 'auto'
    region 'us-west-2';
"""
).format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

user_table_insert = ("""
INSERT INTO users (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
    ) 
    SELECT DISTINCT staging_events.user_id  as user_id,
        staging_events.first_name           as first_name,
        staging_events.last_name            as last_name,
        staging_events.gender               as gender,
        staging_events.level                as level
    FROM staging_events
    WHERE staging_events.page= 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration) 
    SELECT DISTINCT staging_songs.song_id   as song_id,
        staging_songs.title                 as title,
        staging_songs.artist_id             as artist_id,
        staging_songs.year                  as year,
        staging_songs.duration              as duration
    FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
    ) 
    SELECT DISTINCT staging_songs.artist_id as artist_id,
        staging_songs.artist_name           as name,
        staging_songs.artist_location       as location,
        staging_songs.artist_latitude       as latitude,
        staging_songs.artist_longitude      as longitude
    FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday) 
    SELECT DISTINCT TIMESTAMP 'epoch' + staging_events.ts/1000 *INTERVAL '1 second' as start_time,
        EXTRACT(HOUR FROM start_time)               as hour,
        EXTRACT(DAY FROM start_time)                as day,
        EXTRACT(WEEK FROM start_time)               as week,
        EXTRACT(MONTH FROM start_time)              as month,
        EXTRACT(YEAR FROM start_time)               as year,
        EXTRACT(DOW FROM start_time)                as weekday
    FROM staging_events
    WHERE staging_events.page= 'NextSong';
""")

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
    ) 
    SELECT TIMESTAMP 'epoch' + staging_events.ts/1000 *INTERVAL '1 second' as start_time,
        staging_events.user_id          as user_id,
        staging_events.level            as level,
        staging_songs.song_id           as song_id,
        staging_songs.artist_id         as artist_id,
        staging_events.session_id       as session_id,
        staging_events.location         as location,
        staging_events.user_agent       as user_agent
    FROM staging_events JOIN staging_songs ON (staging_events.song=staging_songs.title and staging_events.artist=staging_songs.artist_name)
    WHERE staging_events.page='NextSong';
        
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
