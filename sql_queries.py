import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE stg_events (
        artist VARCHAR(256),
        auth VARCHAR(16) NOT NULL,
        firstName VARCHAR(64),
        gender VARCHAR(1),
        itemInSession INT NOT NULL,
        lastName VARCHAR(64),
        length NUMERIC(10, 5),
        level VARCHAR(8) NOT NULL,
        location VARCHAR(512),
        method VARCHAR(4) NOT NULL,
        page VARCHAR(64) NOT NULL,
        registration BIGINT,
        sessionId BIGINT NOT NULL,
        song VARCHAR(256),
        status INT NOT NULL,
        ts BIGINT NOT NULL,
        userAgent VARCHAR(512),
        userId BIGINT,
        PRIMARY KEY(sessionId, itemInSession)
    ) diststyle even;
""")

staging_songs_table_create = ("""
    CREATE TABLE stg_songs (
        num_songs INT NOT NULL,
        artist_id VARCHAR(32) NOT NULL,
        artist_latitude NUMERIC(11, 8),
        artist_longitude NUMERIC(11, 8),
        artist_location VARCHAR(512),
        artist_name VARCHAR(256) NOT NULL,
        song_id VARCHAR(32) NOT NULL,
        title VARCHAR(256) NOT NULL,
        duration NUMERIC(10, 5) NOT NULL,
        year INT  NOT NULL,
        PRIMARY KEY(song_id)
    ) diststyle even;
""")

songplay_table_create = ("""
    CREATE TABLE fact_songplay (
        songplay_id VARCHAR PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id VARCHAR NOT NULL,
        level VARCHAR NOT NULL,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id VARCHAR,
        location VARCHAR,
        user_agent VARCHAR
    ) diststyle even;
""")

user_table_create = ("""
    CREATE TABLE dim_user (
        user_id VARCHAR PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    ) diststyle even;
""")

song_table_create = ("""
    CREATE TABLE dim_song (
        song_id VARCHAR(32) PRIMARY KEY,
        title VARCHAR(256) NOT NULL,
        artist_id VARCHAR(32) NOT NULL,
        year INT NOT NULL,
        duration NUMERIC(10, 5) NOT NULL
    ) diststyle even;
""")

artist_table_create = ("""
    CREATE TABLE dim_artist (
        artist_id VARCHAR(32) PRIMARY KEY,
        name VARCHAR(256) NOT NULL,
        location VARCHAR(512),
        latitude NUMERIC(11, 8),
        longitude NUMERIC(11, 8)
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE dim_time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY stg_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}' 
    FORMAT AS JSON {}
    REGION 'us-west-2';
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
)

staging_songs_copy = ("""
    COPY stg_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}' 
    FORMAT AS JSON 'auto'
    REGION 'us-west-2';
""").format(
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN']
)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
INSERT INTO dim_song (
    song_id,
    title,
    artist_id,
    year,
    duration
) SELECT 
  	ss.song_id,
    ss.title,
    ss.artist_id,
    ss.year,
    ss.duration
FROM stg_songs ss
LEFT JOIN dim_song ds ON ds.song_id = ss.song_id
WHERE ds.song_id IS NULL;
""")

artist_table_insert = ("""
INSERT INTO dim_artist (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT
	ranked_stg_songs.artist_id,
    ranked_stg_songs.artist_name,
    ranked_stg_songs.artist_location,
    ranked_stg_songs.artist_latitude,
    ranked_stg_songs.artist_longitude
FROM ( 
  SELECT 
      artist_id,
      artist_name,
      artist_location,
      artist_latitude,
      artist_longitude,
      RANK() OVER(PARTITION BY artist_id ORDER BY year DESC) RANK
  FROM stg_songs
) AS ranked_stg_songs
LEFT JOIN dim_artist da ON da.artist_id = ranked_stg_songs.artist_id
WHERE ranked_stg_songs.rank = 1 AND da.artist_id IS NULL;
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]







# # INSERT RECORDS

# songplay_table_insert = ("""
# INSERT INTO songplays (
#     songplay_id,
#     start_time,
#     user_id,
#     level,
#     song_id,
#     artist_id,
#     session_id,
#     location,
#     user_agent
# ) VALUES (
#     %s,
#     %s,
#     %s,
#     %s,
#     %s,
#     %s,
#     %s,
#     %s,
#     %s
# );
# """)

# user_table_insert = ("""
# INSERT INTO users (
#     user_id,
#     first_name,
#     last_name,
#     gender,
#     level
# ) VALUES (
#     %s,
#     %s,
#     %s,
#     %s,
#     %s
# ) ON CONFLICT ON CONSTRAINT users_pkey DO 
#     UPDATE SET level = EXCLUDED.level;
# """)

# song_table_insert = ("""
# INSERT INTO songs (
#     song_id,
#     title,
#     artist_id,
#     year,
#     duration
# ) VALUES (
#     %s,
#     %s,
#     %s,
#     %s,
#     %s    
# ) ON CONFLICT DO NOTHING;;
# """)

# artist_table_insert = ("""
# INSERT INTO artists (
#     artist_id,
#     name,
#     location,
#     latitude,
#     longitude
# ) VALUES (
#     %s,
#     %s,
#     %s,
#     %s,
#     %s
# ) ON CONFLICT DO NOTHING;;
# """)


# time_table_insert = ("""
# INSERT INTO time (
#     start_time,
#     hour,
#     day,
#     week,
#     month,
#     year,
#     weekday
# ) VALUES (
#     %s,
#     %s,
#     %s,
#     %s,
#     %s,
#     %s,
#     %s
# ) ON CONFLICT DO NOTHING;;
# """)

# # FIND SONGS

# song_select = ("""
# SELECT
#     s.song_id,
#     a.artist_id
# FROM songs s
# JOIN artists a ON s.artist_id = a.artist_id
# WHERE
#     s.title = %s AND
#     a.name = %s AND
#     s.duration = %s;
# """)

# # QUERY LISTS

# create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]