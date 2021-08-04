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
        artist VARCHAR(128) NOT NULL,
        auth VARCHAR(16) NOT NULL,
        firstName VARCHAR(32) NOT NULL,
        gender VARCHAR(1) NOT NULL,
        itemInSession INT NOT NULL,
        lastName VARCHAR(32) NOT NULL,
        length NUMERIC(10, 5) NOT NULL,
        level VARCHAR(8) NOT NULL,
        location VARCHAR(64) NOT NULL,
        method VARCHAR(4) NOT NULL,
        page VARCHAR(64) NOT NULL,
        registration BIGINT,
        sessionId BIGINT,
        song VARCHAR(64) NOT NULL,
        status INT NOT NULL,
        ts BIGINT NOT NULL,
        userAgent VARCHAR(64) NOT NULL,
        userId BIGINT,
        PRIMARY KEY(userId, sessionId, itemInSession)
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE stg_songs (
        num_songs INT NOT NULL,
        artist_id VARCHAR(32) NOT NULL,
        artist_latitude NUMERIC(11, 8),
        artist_longitude NUMERIC(11, 8),
        artist_location VARCHAR(64),
        artist_name VARCHAR(64) NOT NULL,
        song_id VARCHAR(32) NOT NULL,
        title VARCHAR(32) NOT NULL,
        duration NUMERIC(10, 5) NOT NULL,
        year INT NOT NULL,
        PRIMARY KEY(song_id)
    );
""")

songplay_table_create = ("""
    CREATE TABLE fact_songplay (
        songplay_id VARCHAR PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id VARCHAR NOT NULL,
        level VARCHAR NOT NULL,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id VARCHAR NOT NULL,
        location VARCHAR NOT NULL,
        user_agent VARCHAR NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE dim_user (
        user_id VARCHAR PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR NOT NULL,
        level VARCHAR NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE dim_song (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INT NOT NULL,
        duration DECIMAL NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE dim_artist (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude DECIMAL,
        longitude DECIMAL
    );
""")

time_table_create = ("""
    CREATE TABLE dim_time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
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