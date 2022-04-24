class SqlQueries:
    create_table_list = ['''
            CREATE TABLE IF NOT EXISTS celerbrity (
	            id bigint not null
	          , celerbrity varchar(256)
	          , followers_count integer
	          , url varchar(256)
	          , check_date timestamp not null
	          , primary key (id, check_date)
            )
            ''',
            '''
            CREATE TABLE IF NOT EXISTS celerbrity_tweet (
                twit_id bigint
	          , celebrity varchar(256)
	          , tweet_url varchar(256)
	          , retweet_count integer
	          , favorite_count integer
	          , created_at timestamp
            )
            '''
    ]