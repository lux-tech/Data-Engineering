CREATE TABLE public.artists (
    artistid varchar(256) NOT NULL,
    name varchar(256),
    location varchar(256),
    lattitude numeric(18,0),
    longitude numeric(18,0)
);

CREATE TABLE public.songplays (
    songplay_id varchar(32) NOT NULL,
    start_time timestamp NOT NULL,
    userid int4 NOT NULL,
    "level" varchar(256),
    song_id varchar(256),
    artist_id varchar(256),
    sessionid int4,
    location varchar(256),
    user_agent varchar(256),
    CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
);

CREATE TABLE public.songs (
    songid varchar(256) NOT NULL,
    title varchar(256),
    artistid varchar(256),
    "year" int4,
    duration numeric(18,0),
    CONSTRAINT songs_pkey PRIMARY KEY (songid)
);

CREATE TABLE public.staging_events (
    artist varchar(256),
    auth varchar(256),
    firstname varchar(256),
    gender varchar(256),
    iteminsession int4,
    lastname varchar(256),
    length numeric(18,0),
    "level" varchar(256),
    location varchar(256),
    "method" varchar(256),
    page varchar(256),
    registration numeric(18,0),
    sessionid int4,
    song varchar(256),
    status int4,
    ts int8,
    useragent varchar(256),
    userid int4
);

CREATE TABLE staging_songs (
    song_id TEXT PRIMARY KEY,
    num_songs INT,
    artist_id TEXT,
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location TEXT,
    artist_name TEXT,
    title TEXT,
    duration DOUBLE PRECISION,
    year INT
);

CREATE TABLE public.users (
    userid int4 NOT NULL,
    first_name varchar(256),
    last_name varchar(256),
    gender varchar(256),
    "level" varchar(256),
    CONSTRAINT users_pkey PRIMARY KEY (userid)
);

CREATE TABLE IF NOT EXISTS public.time (
    start_time timestamp, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday int NOT NULL,
    CONSTRAINT time_pkey PRIMARY KEY (start_time)
);





