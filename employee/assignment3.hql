create table event_tab(artist string, auth string, firstName string, gender string, itemInSession string, lastName string, length string, level string, location string, method string, page string, registration string, sessionId string, song string, status string, ts string, userAgent string,userId string) row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';
load data local inpath 'events.csv' into table event_tab;
select userId, sessionId, first_value(song)over(partition by sessionId ORDER BY song ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), last_value(song)over(partition by sessionId ORDER BY song ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from event_tab limit 50;
SELECT userId, count(distinct song), RANK() OVER (Order BY COUNT(distinct song) DESC) FROM event_tab group by userId;
SELECT userId,count(distinct song), Row_number() OVER  (Order BY COUNT(distinct song) DESC) FROM event_tab where page = 'NextSong' group by userId;
SELECT COUNT(song) FROM event_tab GROUP BY location, artist GROUPING SETS ((location,artist),location,());
SELECT COUNT(song) FROM event_tab GROUP BY location, artist GROUPING SETS ((location,artist),location, artist, ());
SELECT userId, song, LEAD(song, 1, 0) OVER (PARTITION BY userId ORDER BY song) next_song FROM event_tab; 
SELECT userId, song , ts FROM event_tab ORDER BY userId, song, ts;
SELECT userId, song, ts FROM event_tab CLUSTER BY userId, song, ts;