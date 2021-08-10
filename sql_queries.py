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
        songplay_id BIGINT IDENTITY(1, 1),
        start_time TIMESTAMP NOT NULL,
        user_id BIGINT SORTKEY,
        level VARCHAR(8) NOT NULL,
        song_id VARCHAR(32) DISTKEY,
        artist_id VARCHAR(32),
        session_id BIGINT NOT NULL,
        location VARCHAR(512),
        user_agent VARCHAR(512)
    );
""")

user_table_create = ("""
    CREATE TABLE dim_user (
        user_id BIGINT PRIMARY KEY SORTKEY,
        first_name VARCHAR(64),
        last_name VARCHAR(64),
        gender VARCHAR(1),
        level VARCHAR(8) NOT NULL
    ) diststyle even;
""")

song_table_create = ("""
    CREATE TABLE dim_song (
        song_id VARCHAR(32) PRIMARY KEY SORTKEY DISTKEY,
        title VARCHAR(256) NOT NULL,
        artist_id VARCHAR(32) NOT NULL,
        year INT NOT NULL,
        duration NUMERIC(10, 5) NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE dim_artist (
        artist_id VARCHAR(32) PRIMARY KEY SORTKEY,
        name VARCHAR(256) NOT NULL,
        location VARCHAR(512),
        latitude NUMERIC(11, 8),
        longitude NUMERIC(11, 8)
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE dim_time (
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL
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
INSERT INTO fact_songplay (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
) 
SELECT 
	stg_fs.start_time,
    stg_fs.user_id,
    stg_fs.level,
    stg_fs.song_id,
    stg_fs.artist_id,
    stg_fs.session_id,
    stg_fs.location,
    stg_fs.user_agent
FROM (
  SELECT
      timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
      se.userid as user_id,
      se.level,
      ds.song_id,
      da.artist_id,
      se.sessionid as session_id,
      se.location,
      se.useragent as user_agent
  FROM stg_events se
  LEFT JOIN (
      SELECT DISTINCT artist_id, artist_name
      FROM stg_songs
  ) da ON TRIM(da.artist_name) = TRIM(se.artist)
  LEFT JOIN (
      SELECT DISTINCT song_id, title
      FROM stg_songs
  ) ds ON TRIM(ds.title) = TRIM(se.song)
) AS stg_fs
LEFT JOIN fact_songplay fs ON fs.session_id = stg_fs.session_id AND fs.start_time = stg_fs.start_time
WHERE fs.songplay_id IS NULL
""")

user_table_insert = ("""
INSERT INTO dim_user (
    user_id,
    first_name,
    last_name,
    gender,
    level
) 
SELECT 
	u.user_id,
    u.first_name,
    u.last_name,
    u.gender,
    u.level
FROM (
  SELECT
      se.userId AS user_id,
      se.firstname as first_name,
      se.lastname as last_name,
      se.gender,
      se.level,
      RANK() OVER(PARTITION BY userId ORDER BY ts DESC) rank
  FROM stg_events se
  LEFT JOIN dim_user du on du.user_id = se.userId
  WHERE userId IS NOT NULL AND du.user_id IS NULL
) AS u
WHERE u.rank = 1
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
INSERT INTO dim_time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
) SELECT
	t.start_time,
    EXTRACT(hour FROM t.start_time) AS hour,
    EXTRACT(day FROM t.start_time) AS day,
    EXTRACT(week FROM t.start_time) AS week,
    EXTRACT(month FROM t.start_time) AS month,
    EXTRACT(year FROM t.start_time) AS year,
    EXTRACT(dayofweek FROM t.start_time) AS weekday
FROM
(
  SELECT 
      DISTINCT(timestamp 'epoch' + ts/1000 * interval '1 second') AS start_time
  FROM stg_events
) AS t
LEFT JOIN dim_time dt ON dt.start_time = t.start_time
WHERE dt.start_time IS NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
