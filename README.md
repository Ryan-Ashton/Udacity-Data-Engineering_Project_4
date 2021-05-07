# Summary
 
A fictious company "Sparkify" is a music streaming start-up which has grown their user base and song database to the point where a cloud solution would better suit their needs. I have been tasked (as their Data Engineer) to build an ETL pipeline that extracts their data from an S3 bucket, processes them using Spark and then transforms the data into a set of dimensional tables for their analytics team.

### Project Files:

- `dwh.cfg` sets up the configurations for AWS
- `etl.py` retrieves the JSON data from an S3 bucket then transforms the data into Parquets creating a star schema with fact and dimension tables
- `test.ipynb` is to test whether various scripts are working

### Example Song Data

```
{
  "artist_id": "ARD7TVE1187B99BFB1",
  "artist_latitude": NaN,
  "artist_longitude": NaN,
  "artist_location": "California - LA",
  "artist_name": Casual,
  "duration": 218.93179,
  "num_songs": 1,
  "song_id": "SOMZWCG12A8C13C480",
  "title": "I Didn't Mean To",
  "year": 2010
  
}
```


### Example Log Data

```
{
  "artist": "Des'ree",
  "auth": "Logged In",
  "firstName": "Kaylee",
  "gender": "F",
  "itemInSession": 1,
  "lastName": "Summers",
  "length": 246.30812,
  "level": "free",
  "location": "Phoenix-Mesa-Scottsdale, AZ",
  "method": "PUT",
  "page": "NextSong",  
  "registration": 1540344794796,
  "sessionId": 139,
  "song": "You Gotta Be",
  "status": 200  
  "ts": 1541106106796,
  "userAgent": "Mozilla/5.0 (Windows NT 6.1; WOW64)",
  "userId": 8,
}
```



# How to Run

1. Run `etl.py` to retieve data from S3 and perform etl back to S3


