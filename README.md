# ApacheAirflow-ChessAPI-

**Description:**

This project aims to extract data from chess.com APIs with a python script. The data is then sent to a Data Lake in the form of three json files. 
As soon as the data is loaded into the Data Lake, it is ingested into an SQS Queue and then sent to a Snowflake Data Warehouse via a Snowpipe Pipeline. 
Finally, the data is analysed with SQL queries in the Snowflake tables. The execution of the script is orchestrated by Airflow to operate every day midnight.

**Purpose:**

Building an end-to-end Data pipeline to collect useful information about streamers on chess.com, storing this data in S3 and modelling it on Snowflake.


**Setup:**

**Python script:**

The python script first sends a request to the "streamers" API to get a list of all streamers on the platform. 
Then for each streamer the script sends a request for its general information and the statistics of these parts

The three api's which are used in this project are:
https://api.chess.com/pub/streamers -> This api provides information Chess.com streamers.
https://api.chess.com/pub/player/{username} -> This api provides  additional details about a player in a game.
https://api.chess.com/pub/player/{username}/stats  -> This api provides information about  ratings, win/loss, and other stats about a player's game play,etc


**Airflow:**

The Airflow python script allowed me to launch the API python script every day in order to update the list of streamers who are currently live
as well as the statistics of their games. Due to the fact that Data Lake data is automatically sent to the Data Warehouse, only the Airflow scheduler role is used.

**Snowflake:**
In the snowflake side I have created six tables that are:

1.streamers_raw_tbl

2.streamers

3.players_info

4.players_info_raw

5.players_stats

6.player_stats_raw



![Project architecture](https://github.com/naziya-shaik/ApacheAirflow-ChessAPI-/assets/111407441/12c8e3ef-7d0b-4e94-b768-73f2f7971d09)
