import psycopg2
from config import Config

class SQLQueries:

    def __init__(self):
        self.__config = Config()
        self.__s3_config = self.__config.s3_config        
        self.__iam_config = self.__config.iam_config        

    def copy_tables(self):
        staging_events_copy = ("""
            COPY staging_events FROM {}
            CREDENTIALS 'aws_iam_role={}'
            COMPUPDATE OFF region 'us-west-2'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            FORMAT AS JSON {};
        """).format(self.__s3_config['LOG_DATA'], 
                    self.__iam_config['ARN'], 
                    self.__s3_config['LOG_JSONPATH'])

        staging_songs_copy = ("""
            COPY staging_songs FROM {}
            CREDENTIALS 'aws_iam_role={}'
            COMPUPDATE OFF region 'us-west-2'
            FORMAT AS JSON 'auto' 
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
        """).format(self.__s3_config['SONG_DATA'], 
                    self.__iam_config['ARN'])
        
        return [staging_events_copy, staging_songs_copy]
    
    def insert_tables(self):
        songplay_table_insert = ("""
            INSERT INTO fact_songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
            SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                            se.userId as user_id,
                            se.level as level,
                            ss.song_id as song_id,
                            ss.artist_id as artist_id,
                            se.sessionId as session_id,
                            se.location as location,
                            se.userAgent as user_agent
            FROM staging_events se
            JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name;
        """)

        user_table_insert = ("""
            INSERT INTO dim_user(user_id, first_name, last_name, gender, level)
            SELECT DISTINCT userId as user_id,
                            firstName as first_name,
                            lastName as last_name,
                            gender as gender,
                            level as level
            FROM staging_events
            where userId IS NOT NULL;
        """)

        song_table_insert = ("""
            INSERT INTO dim_song(song_id, title, artist_id, year, duration)
            SELECT DISTINCT song_id as song_id,
                            title as title,
                            artist_id as artist_id,
                            year as year,
                            duration as duration
            FROM staging_songs
            WHERE song_id IS NOT NULL;
        """)

        artist_table_insert = ("""
            INSERT INTO dim_artist(artist_id, name, location, latitude, longitude)
            SELECT DISTINCT artist_id as artist_id,
                            artist_name as name,
                            artist_location as location,
                            artist_latitude as latitude,
                            artist_longitude as longitude
            FROM staging_songs
            where artist_id IS NOT NULL;
        """)

        time_table_insert = ("""
            INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
            SELECT distinct ts,
                            EXTRACT(hour from ts),
                            EXTRACT(day from ts),
                            EXTRACT(week from ts),
                            EXTRACT(month from ts),
                            EXTRACT(year from ts),
                            EXTRACT(weekday from ts)
            FROM staging_events
            WHERE ts IS NOT NULL;
        """)

        return [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

    def create_tables(self):
        staging_events_table_create = ("""
            CREATE TABLE IF NOT EXISTS staging_events
            (
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
            ts              TIMESTAMP,
            userAgent       VARCHAR,
            userId          INTEGER
            );
        """)

        staging_songs_table_create = ("""
            CREATE TABLE IF NOT EXISTS staging_songs
            (
            song_id            VARCHAR,
            num_songs          INTEGER,
            title              VARCHAR,
            artist_name        VARCHAR,
            artist_latitude    FLOAT,
            year               INTEGER,
            duration           FLOAT,
            artist_id          VARCHAR,
            artist_longitude   FLOAT,
            artist_location    VARCHAR
            );
        """)

        songplay_table_create = ("""
            CREATE TABLE IF NOT EXISTS fact_songplay
            (
            songplay_id          INTEGER IDENTITY(0,1) PRIMARY KEY sortkey,
            start_time           TIMESTAMP,
            user_id              INTEGER,
            level                VARCHAR,
            song_id              VARCHAR,
            artist_id            VARCHAR,
            session_id           INTEGER,
            location             VARCHAR,
            user_agent           VARCHAR
            );
        """)

        user_table_create = ("""
            CREATE TABLE IF NOT EXISTS dim_user
            (
            user_id INTEGER PRIMARY KEY distkey,
            first_name      VARCHAR,
            last_name       VARCHAR,
            gender          VARCHAR,
            level           VARCHAR
            );
        """)

        song_table_create = ("""
            CREATE TABLE IF NOT EXISTS dim_song
            (
            song_id     VARCHAR PRIMARY KEY,
            title       VARCHAR,
            artist_id   VARCHAR distkey,
            year        INTEGER,
            duration    FLOAT
            );
        """)

        artist_table_create = ("""
            CREATE TABLE IF NOT EXISTS dim_artist
            (
            artist_id          VARCHAR PRIMARY KEY distkey,
            name               VARCHAR,
            location           VARCHAR,
            latitude           FLOAT,
            longitude          FLOAT
            );
        """)

        time_table_create = ("""
            CREATE TABLE IF NOT EXISTS dim_time
            (
            start_time    TIMESTAMP PRIMARY KEY sortkey distkey,
            hour          INTEGER,
            day           INTEGER,
            week          INTEGER,
            month         INTEGER,
            year          INTEGER,
            weekday       INTEGER
            );
        """)

        return [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

    def drop_tables(self):
        staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
        staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
        songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
        user_table_drop = "DROP TABLE IF EXISTS dim_user"
        song_table_drop = "DROP TABLE IF EXISTS dim_song"
        artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
        time_table_drop = "DROP TABLE IF EXISTS dim_time"
        
        return [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

class ETL: 

    def __init__(self):
        self.__sql_queries = SQLQueries()
        self.__config = Config()
        self.__db_cluster_config = self.__config.cluster_config
        #Database cofig
        self.__db_host = self.__db_cluster_config['HOST']
        self.__db_name = self.__db_cluster_config['DB_NAME']
        self.__db_user = self.__db_cluster_config['DB_USER']
        self.__db_pass = self.__db_cluster_config['DB_PASSWORD']
        self.__db_port = self.__db_cluster_config['DB_PORT']

    def load_staging_tables(self, cur, conn):
        print('Initialize Copy Tables')
        for query in self.__sql_queries.copy_tables():
            cur.execute(query)
            conn.commit()

    def insert_tables(self, cur, conn):
        print('Initialize Insert Tables')
        for query in self.__sql_queries.insert_tables():
            cur.execute(query)
            conn.commit()

    def process(self):
        conn = psycopg2.connect("host={} \
                                dbname={} \
                                user={} \
                                password={} \
                                port={}".format(self.__db_host,
                                                self.__db_name,
                                                self.__db_user,
                                                self.__db_pass,
                                                self.__db_port))
        cur = conn.cursor()
        
        self.load_staging_tables(cur, conn)
        self.insert_tables(cur, conn)

        conn.close()
