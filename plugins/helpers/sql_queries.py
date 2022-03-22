class SqlQueries:
    create_table_list = ['''
            CREATE TABLE IF NOT EXISTS bts_toptrack (
                market varchar(5) not null,
                rank integer not null,
                track_id varchar(100) not null,
                track_name varchar(100) not null,
                popularity integer not null,
                duration_ms integer not null,
                track_number integer not null,
                album_id varchar(100) not null,
                album_name varchar(100) not null,
                release_date date not null,
                primary key(market, track_id)
            )
            ''',
            '''
            CREATE TABLE IF NOT EXISTS bts_toptrack_tweet (
                tweet_id bigint not null,
                track_name varchar(100) not null,
                tweet_url varchar(300) not null,
                created_at timestamp not null,
                user_id bigint not null,
                user_name varchar(200) not null,
                user_followers_count varchar(200) not null,
                primary key(tweet_id)
            )
            '''
    ]