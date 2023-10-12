create or replace table tblAlbum
(
   album_id varchar,
   name string,
   release_date date,
   total_tracks int,
   URL varchar
)


create or replace table tblArtist
(
   artist_id varchar,
   artist_name string,
   external_url varchar
)

create or replace table tblSongs
(
    song_id varchar,
    song_name string,
    duration_ms number(38,0),
    URL varchar,
    popularity int,
    song_added TIMESTAMP_TZ,
    album_id varchar,
    artist_id varchar
    
)


select * from tblAlbum
select * from tblArtist----60
select * from tblSongs



CREATE STORAGE INTEGRATION spotify_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::783059372696:role/spotifysnowflakeproject'
  STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-sarath/transformed_data/', 's3://spotify-etl-sarath/transformed_data/')


  DESC INTEGRATION spotify_integration;

  CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;
  

CREATE STAGE my_s3_stage
  STORAGE_INTEGRATION = spotify_integration
  URL = 's3://spotify-etl-sarath/transformed_data/'
  FILE_FORMAT = my_csv_format;

Desc stage my_s3_stage

LIST @my_s3_stage

copy into tblAlbum from '@my_s3_stage/album_data/album_transformed_2023-10-10 16:45:54.012652.csv';

copy into tblArtist from '@my_s3_stage/artist_data/artist_transformed_2023-10-10 16:45:54.080046.csv';

copy into tblSongs from '@my_s3_stage/songs_data/songs_transformed_2023-10-10 16:45:53.740233.csv';

DESC pipe SPOTIFY_ETL_PROJECT.PUBLIC.ALBUM_PIPE;

select system$pipe_status('ALBUM_PIPE');

create pipe Artist_Pipe
auto_ingest=true 
as
copy into tblArtist
from '@my_s3_stage/artist_transformed_2023-10-10 16_45_54.080046666.csv'
file_format = my_csv_format;

desc pipe Artist_Pipe

create pipe Songs_Pipe
auto_ingest=true 
as
copy into tblSongs
from @my_s3_stage
file_format = my_csv_format;

desc pipe Songs_Pipe

create or replace pipe SPOTIFY_ETL_PROJECT.PUBLIC.ALBUM_PIPE
auto_ingest=true
as 
copy into tblAlbum
from @my_s3_stage
