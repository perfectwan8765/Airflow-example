from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from helpers import SqlQueries

from datetime import datetime
import pandas as pd
from TwitterAPI import TwitterAPI

twitter_owners = {
    'BTS' : 'bts_twt', 
    'BLACKPINK' : 'ygofficialblink', 
    'JUSTINBIEBER' : 'justinbieber',
    'KATYPERRY' : 'katyperry',
    'RIHANNA' : 'rihanna',
    'TAYLORSWIFT' : 'taylorswift13',
    'LADYGAGA' : 'ladygaga',
    'BRUNOMARS' : 'BrunoMars', 
    'SELENAGOMEZ' : 'selenagomez'
}

def get_twitter_api () :
    tweet_apikey = Variable.get('tweet_apikey', deserialize_json=True)
    api = TwitterAPI(tweet_apikey['api_key'], tweet_apikey['api_secret_key'], tweet_apikey['access_token'], tweet_apikey['access_token_secret'])

    return api

def get_redshift_con () :
    redshift_conn_id = 'redshift_conn'
    sql_alchemy_engine = RedshiftSQLHook(redshift_conn_id=redshift_conn_id).get_sqlalchemy_engine()

    return sql_alchemy_engine

def insert_table_dict (table_name, dict_info) :
    df = pd.DataFrame(dict_info, columns = dict_info.keys())
    df.to_sql(name=table_name, con=get_redshift_con(), index=False, if_exists='replace')

def insert_popular_tweet() : 
    api = get_twitter_api()

    celebrity_list = []
    tweet_ids = []
    tweet_urls = []
    retweet_counts = []
    favorite_counts = []
    tweet_created_at_list = []

    for name, screen_name in twitter_owners.items() :
        # GET popular tweet 
        tweet_response = api.request('search/tweets', {
            'q' : '%40{screen_name}'.format(screen_name=screen_name),
            'lang' : 'en', 
            'result_type' : 'popular'
        })

        for tweet in tweet_response :
            celebrity_list.append(name)
            tweet_ids.append(tweet['id'])
            tweet_created_at_list.append(datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
            retweet_counts.append(tweet["retweet_count"])
            favorite_counts.append(tweet["favorite_count"])

            user = tweet['user']
            tweet_urls.append("https://twitter.com/{screen_name}/status/{tweet_id}".format(screen_name=user['screen_name'], tweet_id=user['id']))
    
    celebrity_tweets = {
        "celebrity" : celebrity_list,
        "twit_id" : tweet_ids,
        "tweet_url" : tweet_urls,
        "retweet_count" : retweet_counts,
        "favorite_count" : favorite_counts,
        "created_at" : tweet_created_at_list,
    }

    insert_table_dict('celerbrity_tweet', celebrity_tweets)

def insert_celebrity_twitter() :
    now = datetime.now()
    api = get_twitter_api()

    celebrity_ids = []
    celebrity_names = []
    celebrity_followers_counts = []
    celebrity_urls = []
    check_dates = []

    for name, screen_name in twitter_owners.items() :
        # GET popular tweet
        user_response = api.request('users/show', {
            'screen_name' : screen_name,
        })

        for user_info in user_response :
            celebrity_ids.append(user_info['id'])
            celebrity_names.append(user_info['name'])
            celebrity_followers_counts.append(user_info['followers_count'])
            celebrity_urls.append("https://twitter.com/{screen_name}".format(screen_name=user_info['screen_name']))
            check_dates.append(now)

    celebritys = {
        "id" : celebrity_ids,
        "celerbrity" : celebrity_names,
        "followers_count" : celebrity_followers_counts,
        "url" : celebrity_urls,
        "check_date" : check_dates
    }

    insert_table_dict('celerbrity', celebritys)


with DAG('bts_tweet', schedule_interval='@daily', 
        default_args={
            'start_date' : datetime(2022, 1, 1)
        }, 
        catchup=False) as dag :

    create_table = RedshiftSQLOperator(
        task_id = 'create_table',
        redshift_conn_id = 'redshift_conn',
        sql = SqlQueries.create_table_list
    )

    insert_popular_tweet = PythonOperator(
        task_id = 'insert_popular_tweet',
        python_callable = insert_popular_tweet
    )

    insert_celebrity_twitter = PythonOperator(
        task_id = 'insert_celebrity_twitter',
        python_callable = insert_celebrity_twitter
    )

    chain(
        create_table,
        [insert_popular_tweet, insert_celebrity_twitter],
    )    