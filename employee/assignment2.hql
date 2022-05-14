create database assign2;
create external table assign2.songs_tab_test (artist_id string, artist_latitude string, artist_location string, artist_longitude string, duration string, num_songs string, song_id string, title string) partitioned by (artist_name string,  year string) row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' location 'hdfs://namenode:8020/nayieraassign2';
!hadoop fs -put songs.csv /nayieraassign2;
select * from assign2.songs_tab_test;
alter table assign2.songs_tab_test add partition (artist_name = 'afnan', year = '2020') location 'hdfs://namenode:8020/nayieraassign2/nayra';
!hadoop fs -put songs.csv /nayieraassign2/nayra;
create table assign2.staging2 (artist_id string, artist_latitude string, artist_location string, artist_longitude string, artist_name string, duration string, num_songs string, song_id string, title string, year string) row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';
load data local inpath 'songs.csv' into table assign2.staging2;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table assign2.songs_tab_test partition (artist_name, year) select artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year from assign2.staging2; 
drop table assign2.songs_tab_test;
create external table assign2.songs_tab_test (artist_id string, artist_latitude string, artist_location string, artist_longitude string, duration string, num_songs string, song_id string, title string) partitioned by (artist_name string,  year string) row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' location 'hdfs://namenode:8020/nayieraassign2';
insert into table assign2.songs_tab_test partition (artist_name, year) select artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year from assign2.staging2; 
drop table assign2.songs_tab_test;
create table assign2.songs_tab_test (artist_id string, artist_latitude string, artist_location string, artist_longitude string, duration string, num_songs string, song_id string, title string) partitioned by (year string, artist_name string) row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';
insert into table assign2.songs_tab_test partition (year = '2020', artist_name) select artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title from assign2.staging2 where year = '2020';
create table avro_xx like assign2.staging2 stored as avro; 
create table parquet_xx like assign2.staging2 stored as parquet; 

