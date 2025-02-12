class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
              FROM staging_events
              WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time) AS hour, 
               extract(day from start_time) AS day, extract(week from start_time) AS week, 
               extract(month from start_time) AS month, extract(year from start_time) AS year, 
               extract(dayofweek from start_time) AS dayofweek
        FROM songplays
    """)

    # Add a create_table_list attribute for table creation queries
    create_table_list = [
        """
        CREATE TABLE IF NOT EXISTS staging_events (
            artist          VARCHAR,
            auth            VARCHAR,
            firstName       VARCHAR,
            gender          VARCHAR,
            itemInSession   INTEGER,
            lastName        VARCHAR,
            length          FLOAT,
            level           VARCHAR,
            location        VARCHAR,
            method          VARCHAR,
            page            VARCHAR,
            registration    BIGINT,
            sessionId       INTEGER,
            song            VARCHAR,
            status          INTEGER,
            ts              BIGINT,
            userAgent       VARCHAR,
            userId          INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs        INTEGER,
            artist_id        VARCHAR,
            artist_latitude  FLOAT,
            artist_longitude FLOAT,
            artist_location  VARCHAR,
            artist_name      VARCHAR,
            song_id          VARCHAR,
            title            VARCHAR,
            duration         FLOAT,
            year             INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id      VARCHAR PRIMARY KEY,
            start_time       TIMESTAMP NOT NULL,
            user_id          INTEGER NOT NULL,
            level            VARCHAR,
            song_id          VARCHAR,
            artist_id        VARCHAR,
            session_id       INTEGER,
            location         VARCHAR,
            user_agent       VARCHAR
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id          INTEGER PRIMARY KEY,
            first_name       VARCHAR,
            last_name        VARCHAR,
            gender           VARCHAR,
            level            VARCHAR
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS songs (
            song_id          VARCHAR PRIMARY KEY,
            title            VARCHAR NOT NULL,
            artist_id        VARCHAR,
            year             INTEGER,
            duration         FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS artists (
            artist_id        VARCHAR PRIMARY KEY,
            name             VARCHAR NOT NULL,
            location         VARCHAR,
            latitude         FLOAT,
            longitude        FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS time (
            start_time       TIMESTAMP PRIMARY KEY,
            hour             INTEGER,
            day              INTEGER,
            week             INTEGER,
            month            INTEGER,
            year             INTEGER,
            weekday          INTEGER
        );
        """
    ]
