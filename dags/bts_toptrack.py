from operator import index
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.hooks.S3_hook import S3Hook
from helpers import SqlQueries

from datetime import datetime
import base64
import requests
import json
import pandas as pd
from TwitterAPI import TwitterAPI

default_args = {
    'start_date' : datetime(2022, 1, 1)
}

def _authenticate_spotify() :
    token_url = 'https://accounts.spotify.com/api/token'
    spotify_client_id = Variable.get('spotify_apikey_id')
    spotify_client_secret = Variable.get('spotify_secret')
    client_creds = f"{spotify_client_id}:{spotify_client_secret}"
    client_creds_bs64 = base64.b64encode(client_creds.encode())

    data = {
        "grant_type": "client_credentials"
    }
    headers = {
        "Authorization" : f"Basic {client_creds_bs64.decode()}"
    }

    auth_response = requests.post(token_url, data=data, headers=headers)

    # convert the response to JSON
    auth_response_data = auth_response.json()

    # save the access token
    access_token = auth_response_data['access_token']

    return access_token

def _make_spotify_csv(ti) :
    tracks = ti.xcom_pull(task_ids=['get_bts_toptracks'])

    if not len(tracks) or "nonexistent tracks" in tracks :
        raise ValueError('tracks is empty')
    
    tracks = tracks[0]
    now = datetime.now()
    market = "US"
    ranks = []
    track_ids = []
    track_names = []
    popularity_list = []
    duration_ms_list = []
    track_numbers = []
    album_ids = []
    album_names = []
    release_date_list = []

    for rank, track in enumerate(tracks["tracks"]) :
        ranks.append(rank+1)
        track_ids.append(track["id"])
        track_names.append(track["name"])
        popularity_list.append(track["popularity"])
        duration_ms_list.append(track["duration_ms"])
        track_numbers.append(track["track_number"])

        album = track["album"]
        album_ids.append(album["id"])
        album_names.append(album["name"])
        release_date_list.append(album["release_date"])

    top_tracks = {
        "market": [market]*len(ranks),
        "rank" : ranks,
        "track_id" : track_ids,
        "track_name" : track_names,
        "popularity" : popularity_list,
        "duration_ms" : duration_ms_list,
        "track_number" : track_numbers,
        "album_id" : album_ids,
        "album_name" : album_names,
        "release_date" : release_date_list
    }
    
    top_tracks_df = pd.DataFrame(top_tracks, columns = top_tracks.keys())
    top_tracks_df.to_csv("/tmp/BTS_TopTrack_{year}_{month}_{day}.csv".format(year=now.year, month=now.month, day=now.day), encoding='UTF-8', index=False, header=False)

def _make_tweet_csv() :
    tweet_apikey = Variable.get('tweet_apikey', deserialize_json=True)
    now = datetime.now()
    df = pd.read_csv('/tmp/BTS_TopTrack_{year}_{month}_{day}.csv'.format(year=now.year, month=now.month, day=now.day), encoding='UTF-8', header=None)
    
    track_list = [track.replace(' ','') for track in df[3].tolist()]
    api = TwitterAPI(tweet_apikey['api_key'], tweet_apikey['api_secret_key'], tweet_apikey['access_token'], tweet_apikey['access_token_secret'])

    tracks = []
    tweet_ids = []
    tweet_urls = []
    tweet_created_at_list = []
    user_ids = []
    user_names = []
    user_followers_count_list = []

    for track in track_list :
        if "(" in track :
            track = track.split('(')[0]

        response = api.request('search/tweets', {'q':'%23{track}'.format(track=track), 'lang':'en', 'result_type':'recent', 'count':10})
        for tweet in response:
            tracks.append(track)
            tweet_ids.append(tweet['id'])
            tweet_created_at_list.append(datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))

            user = tweet['user']
            user_ids.append(user['id'])
            user_names.append(user['screen_name'])
            user_followers_count_list.append(user['followers_count'])

            tweet_urls.append("https://twitter.com/{screen_name}/status/{tweet_id}".format(screen_name=user['screen_name'], tweet_id=tweet['id']))

    track_twits = {
        "tweet_id" : tweet_ids,
        "track_name": tracks,
        "tweet_url" : tweet_urls,
        "created_at" : tweet_created_at_list,
        "user_id" : user_ids,
        "user_name" : user_names,
        "user_followers_count" : user_followers_count_list
    }
    
    track_twits_df = pd.DataFrame(track_twits, columns = track_twits.keys())
    track_twits_df.to_csv("/tmp/BTS_TrackTweet_{year}_{month}_{day}.csv".format(year=now.year, month=now.month, day=now.day), encoding='UTF-8', index=False, header=False)

def _upload_csv_s3 () :
    now = datetime.now()
    bucket_name = 'btstweetbucket'
    bucket = S3Hook(bucket_name)

    bucket.load_file(filename="/tmp/BTS_TopTrack_{year}_{month}_{day}.csv".format(year=now.year, month=now.month, day=now.day),
                    key="/track/BTS_TopTrack_{year}_{month}_{day}.csv".format(year=now.year, month=now.month, day=now.day),
                    bucket_name=bucket_name,
                    replace=True
    )

    bucket.load_file(filename="/tmp/BTS_TrackTweet_{year}_{month}_{day}.csv".format(year=now.year, month=now.month, day=now.day),
                    key="/tweet/BTS_TrackTweet_{year}_{month}_{day}.csv".format(year=now.year, month=now.month, day=now.day),
                    bucket_name=bucket_name,
                    replace=True
    )

with DAG('bts_toptrack', schedule_interval='@daily', 
        default_args=default_args, 
        catchup=False) as dag :
    
    now = datetime.now()

    creating_table = RedshiftSQLOperator(
        task_id = 'creating_table',
        redshift_conn_id = 'redshift_conn',
        sql = SqlQueries.create_table_list
    )

    authenticating_spotify = PythonOperator(
        task_id = 'authenticating_spotify',
        python_callable = _authenticate_spotify
    )

    get_bts_toptracks = SimpleHttpOperator(
        task_id = 'get_bts_toptracks',
        http_conn_id = 'spotify_api',
        endpoint = "v1/artists/{{ var.value.get('bts_id') }}/top-tracks?market=US",
        method = 'GET',
        headers = {
            "Accept" : "application/json",
            "Content-Type" : "application/json",
            "Authorization" : "Bearer {{ task_instance.xcom_pull('authenticating_spotify') }}"
        },
        response_filter = lambda response: json.loads(response.text),
        log_response = True
    )

    making_spotify_csv = PythonOperator(
        task_id = 'making_spotify_csv',
        python_callable = _make_spotify_csv
    )

    making_tweet_csv = PythonOperator(
        task_id = 'making_tweet_csv',
        python_callable = _make_tweet_csv
    )

    uploading_csv_s3 = PythonOperator(
        task_id = 'uploading_csv_s3',
        python_callable = _upload_csv_s3
    )

    copying_toptrack_s3_to_redshift = S3ToRedshiftTransfer(
        task_id = 'copying_toptrack_s3_to_redshift',
        schema = 'public',
        table = 'bts_toptrack',
        s3_bucket = 'btstweetbucket',
        s3_key = '/track/BTS_TopTrack_{year}_{month}_{day}.csv'.format(year=now.year, month=now.month, day=now.day),
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 'my_conn_s3',
        copy_options = ['FORMAT AS CSV']
    )

    copying_tweet_s3_to_redshift = S3ToRedshiftTransfer(
        task_id = 'copying_tweet_s3_to_redshift',
        schema = 'public',
        table = 'bts_toptrack_tweet',
        s3_bucket = 'btstweetbucket',
        s3_key = '/tweet/BTS_TrackTweet_{year}_{month}_{day}.csv'.format(year=now.year, month=now.month, day=now.day),
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 'my_conn_s3',
        copy_options = ['FORMAT AS CSV']
    )

    cleaning_csv = BashOperator(
        task_id = 'cleaning_csv',
        bash_command = 'rm -f /tmp/BTS*.csv'
    )

    chain(
        [creating_table, authenticating_spotify],
        get_bts_toptracks,
        making_spotify_csv,
        making_tweet_csv,
        uploading_csv_s3,
        [copying_toptrack_s3_to_redshift, copying_tweet_s3_to_redshift],
        cleaning_csv
    )    