# Airflow Projects 
> Airflow로 구성한 DAG Code 모음
      
## Tech Stack
![Airflow](https://img.shields.io/badge/Airflow-017CEE.svg?&style=for-the-badge&logo=ApacheAirflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB.svg?&style=for-the-badge&logo=Python&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-FF9900.svg?&style=for-the-badge&logo=AmazonAWS&logoColor=white)
      
## Airflow 설치 (in AWS EC2)
1. Install Using PyPl
* Create Python Virtual enviorment
```bash
python3 -m venv sandbox
source /sandbox/bin/activate
```
* Install Airflow using Pypl
```bash
pip install wheel
pip install "apache-airflow==2.2.4" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.4/constraints-3.6.txt"
```
2. SQLite Upgrade   
Airflow 2.0 이상 Version에서 SQLite의 Version은 3.15 이상되어야 한다.([참고링크](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html#setting-up-a-sqlite-database))   
AWS EC2에서 SQLite의 Version은 낮은 버전(3.7)이라 Upgrade가 필요하다.
* [SQLite Site](https://www.sqlite.org/download.html)에서 최신 Version Download   
```bash
wget https://www.sqlite.org/2022/sqlite-autoconf-3380100.tar.gz
tar -xzf sqlite-autoconf-3380100.tar.gz
```
* SQLite 최신 Version Upgrade를 위한 Packge Install
```bash
yum groupinstall "Development Tools"
```
* SQLite Build and Install
```bash
cd sqlite-autoconf-3380100
./configure
make
sudo make install
```
* 추가 설정
```bash
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH # or 해당 command .bash_profile에 추가
cp ./sqlite-autoconf-3380100/sqlite3 /usr/local/bin/sqlite3
#clean
cd .. && rm -rf sqlite-autoconf-3380100
```
* SQLite Version Check
```bash
sqlite

#SQLite version 3.38.1 2022-03-12 13:37:29
#Enter ".help" for usage hints.
#Connected to a transient in-memory database.
#Use ".open FILENAME" to reopen on a persistent database.
```

3. Airflow 시작
```bash
# airflow Metastore 초기화, 처음 Airflow 실행전 command 실행 필요
airflow db -init
# airflow webserver 시작 -D=Daemon
airflow webserver -D
# airflow scheduler 시작 -D=Daemon
airflow scheduler -D
```
      
## Projects Overview
### 1. BTS는 정말 미국에서 인기가 많을까?
> Spotify에서 BTS TopTrack Data와 해당 Track을 해시태그한 최신 Twitter Data를 AWS Redshift에 적재하는 Pipeline를 Apach Airflow로 구성
   
![BTS_TopTrack_DAG](/images/bts_toptrack_dag.png)
![BTS_TopTrack_Diagram](/images/bts_toptrack_diagram.png)

* 구성   
  * Spotify와 Twitter API 활용
  * 미국 내 BTS TopTrack Data, TopTrack 해시태그 Tweet Data 활용
  * Data를 CSV로 생성하여 AWS S3에 Upload
  * S3의 CSV Data를 AWS Redshift에 적재

* Tasks  
  1. authenticating_spotify > Spotify API Token 생성
  2. creating_table > Redshift Table 생성
  3. get_bts_top_tracks > Spotify API TopTrack Data 요청
  4. making_spotify_csv > TopTrack CSV 생성
  5. making_tweet_csv > BTS TopTrack Tweet CSV 생성
  6. uploading_csv_s3 > CSV AWS S3 Upload
  7. copying_toptrack_s3_to_redshift, copying_tweet_s3_to_redshift > AWS S3 CSV를 Redshift Table에 적재
   
* Redshift Table 구성
1. BTS Spotify TopTrack Table
   1. 테이블 명 : bts_toptrack
   2. 컬럼 List  

 |순서|컬럼명|데이터타입|길이|Nullable|PK여부|
 |:---:|------|-------|:----:|----|:--:|
 |1|market|varchar|5|NotNull|Y|
 |2|rank|integer||NotNull||
 |3|track_id|varchar|100|NotNull|Y|
 |4|track_name|varchar|100|NotNull||
 |5|popularity|integer||NotNull||
 |6|duration_ms|integer||NotNull||
 |7|track_number|integer||NotNull||
 |8|album_id|varchar(100)||NotNull||
 |9|album_name|varchar(100)||NotNull||
 |10|release_data|date||NotNull||
  

2. BTS TopTrack 해시태그 Tweet Table
   1. 테이블 명 : bts_toptrack_tweet
   2. 컬럼 List

 |순서|컬럼명|데이터타입|길이|Nullable|PK여부|
 |:---:|------|-------|:----:|----|:--:|
 |1|tweet_id|bigint||NotNull|Y|
 |2|track_name|varchar|100|NotNull||
 |3|tweet_url|varchar|300|NotNull||
 |4|created_at|timestamp||NotNull||
 |5|user_id|bigint||NotNull||
 |6|user_name|varchar|200|NotNull||
 |7|user_followers_count|varchar|200|NotNull||
