

class SqlQueries:
    # drop table
    drop_staging_events = "drop table if exists staging_events"
    drop_staging_songs = "drop table if exists staging_songs"
    drop_artists = "drop table if exists artists"
    drop_songs = "drop table if exists songs"
    drop_users = "drop table if exists users"
    drop_songplays = "drop table if exists songplays"
    drop_times = "drop table if exists times"
    drop_table_list = [drop_staging_events, drop_staging_songs, drop_artists, drop_songs, drop_users, drop_songplays,
                       drop_times]

    # create table if not exists sql
    create_staging_log_table = """
        CREATE TABLE if not exists staging_events (
            artist varchar(256),
            auth varchar(256),
            firstName varchar(256),
            gender varchar(256),
            itemInSession int4,
            lastName varchar(256),
            length numeric(18,0),
            "level" varchar(256),
            location varchar(256),
            "method" varchar(256),
            page varchar(256),
            registration numeric(18,0),
            sessionId int4,
            song varchar(256),
            status int4,
            ts int8,
            userAgent varchar(256),
            userId int4
        )
    """

    create_staging_song_table = """
    CREATE TABLE if not exists staging_songs (
        num_songs int4,
        artist_id varchar(256),
        artist_name varchar(256),
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0),
        artist_location varchar(256),
        song_id varchar(256),
        title varchar(256),
        duration numeric(18,0),
        "year" int4
    )
    """

    create_artists_table = """
    CREATE TABLE if not exists artists (
        artist_id varchar(256) NOT NULL,
        artist_name varchar(256),
        artist_location varchar(256),
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0)
    )   
    """

    create_songs_table = """
    CREATE TABLE if not exists songs (
        song_id varchar(256) NOT NULL,
        title varchar(256),
        artist_id varchar(256),
        "year" int4,
        duration numeric(18,0),
        CONSTRAINT songs_pkey PRIMARY KEY (song_id)
    )
    """

    create_users_table = """
    CREATE TABLE if not exists users (
        userId int4 NOT NULL,
        firstName varchar(256),
        lastName varchar(256),
        gender varchar(256),
        "level" varchar(256),
        CONSTRAINT users_pkey PRIMARY KEY (userId)
    )
    """

    create_songplays_table = """
    CREATE TABLE if not exists songplays (
        songplay_id varchar(32) NOT NULL,
        start_time timestamp NOT NULL,
        userId int4 NOT NULL,
        "level" varchar(256),
        song_id varchar(256),
        artist_id varchar(256),
        sessionId int4,
        location varchar(256),
        userAgent varchar(256),
        CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
    )
    """

    create_times_table = """
    CREATE TABLE if not exists times (
        start_time timestamp,
        hour numeric(18,0),
        day numeric(18,0),
        week numeric(18,0),
        month numeric(18,0),
        year numeric(18,0),
        dayofweek numeric(18,0)
    )
    """

    create_table_list = [create_staging_log_table, create_staging_song_table, create_artists_table, create_songs_table,
                         create_users_table, create_songplays_table, create_times_table]

    # insert table
    songplay_table_insert = ("""
    insert into songplays
        SELECT
                md5(events.sessionId || events.start_time) songplay_id,
                events.start_time, 
                events.userId, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionId, 
                events.location, 
                events.userAgent
            FROM 
            (   SELECT 
                    TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, 
                    *
                FROM 
                    staging_events
                WHERE 
                    page='NextSong'
            ) events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
    insert into users
        SELECT distinct 
            userId, 
            firstName, 
            lastName, 
            gender, 
            level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
    insert into songs
        SELECT distinct 
            song_id, 
            title, 
            artist_id, 
            year, 
            duration
        FROM 
            staging_songs
    """)

    artist_table_insert = ("""
    insert into artists
        SELECT distinct 
            artist_id, 
            artist_name, 
            artist_location, 
            artist_latitude, 
            artist_longitude
        FROM 
            staging_songs
    """)

    time_table_insert = ("""
    insert into times
        SELECT 
            start_time, 
            extract(hour from start_time), 
            extract(day from start_time), 
            extract(week from start_time), 
            extract(month from start_time), 
            extract(year from start_time), 
            extract(dayofweek from start_time)
        FROM songplays
    """)


