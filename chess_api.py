import requests
import s3fs
import json
import boto3
from datetime import datetime,timedelta
# Imported neccessary modules 

def chess_etl_program():
    
    # Connecting to S3 by providing credentials.
    s3 = boto3.resource(
        service_name = 's3',
        region_name = '<region_name>',
        aws_access_key_id = '<access_key>',
        aws_secret_access_key = '<secret_access_key>'
    )
    
    
    bucket = s3.Bucket('chess-streamers-data')
    
    # Getting streamers data from chess.com
    streamersResponse = requests.get('https://api.chess.com/pub/streamers').json()
    data_streamers = []
    for idx,value in enumerate(streamersResponse['streamers']):
        tempdictStreamers = {}
        tempdictStreamers['username'] = value['username']
        tempdictStreamers['isLive'] = value['is_live']
        data_streamers.append(tempdictStreamers)


    now = datetime.now()
    date_time = now.strftime("%m%d%Y%H%M%S")


    fileName_local = './streamers'+date_time+'.json'
    with open(fileName_local, 'w') as file:
        json.dump(data_streamers, file)


    cloudFileName = 'streamers'+date_time+'.json'

    # Saving the json file to the S3 bucket named chess-streamers-data.
    bucket.upload_file(Key=cloudFileName,Filename=fileName_local)

    bucket = s3.Bucket('chess-playersinfo-data')

    dictPlayers = []

    # Pulling further information about each streamers by passing their username.
    for idx,values in enumerate(data_streamers):
        playersInfo = requests.get(f"https://api.chess.com/pub/player/{values['username']}").json()
        tempPlayersInfo = {}
        tempPlayersInfo['username'] = playersInfo.get('username','N/A')
        tempPlayersInfo['followers'] = playersInfo.get('followers',0)
        tempPlayersInfo['country'] = playersInfo.get('country','N/A')
        tempPlayersInfo['joined'] = playersInfo.get('joined',0)
        tempPlayersInfo['location'] = playersInfo.get('location','N/A')
        tempPlayersInfo['name'] = playersInfo.get('name','N/A')
        tempPlayersInfo['player_id'] = playersInfo.get('player_id',0)
        tempPlayersInfo['status'] = playersInfo.get('status','N/A')
        tempPlayersInfo['title'] = playersInfo.get('title','N/A')
        # tempPlayersInfo['status'] = playersInfo['status']
        dictPlayers.append(tempPlayersInfo)


    now = datetime.now()
    date_time = now.strftime("%m%d%Y%H%M%S")


    fileName_local = './playerInfo'+date_time+'.json'
    with open(fileName_local, 'w') as file:
        json.dump(dictPlayers, file)

    cloudFileName = 'PlayerInfo'+date_time+'.json'

    # Uploading the json file which contains further information about each streamers to the bucket named chess-playersinfo-data
    bucket.upload_file(Key=cloudFileName,Filename=fileName_local)

    bucket = s3.Bucket('chess-playersstats-data')

    dictStats = []

    # Getting stats about each user by passing their username
    for idx, value in enumerate(dictPlayers):
        tempStatsDict = {}
        playersStats = requests.get(f"https://api.chess.com/pub/player/{value['username']}/stats").json()
        chess_blitz = playersStats.get('chess_blitz','')
        if chess_blitz == '':
            tempStatsDict['last_blitz'] = 0
            tempStatsDict['draw_blitz'] = 0
            tempStatsDict['loss_blitz'] = 0
            tempStatsDict['win_blitz'] = 0

        else:
            tempStatsDict['last_blitz'] = chess_blitz['last']['rating']

            tempStatsDict['draw_blitz'] = chess_blitz['record']['draw']
            tempStatsDict['loss_blitz'] = chess_blitz['record']['loss']
            tempStatsDict['win_blitz'] = chess_blitz['record']['win']

        chess_bullet = playersStats.get('chess_bullet','')

        if chess_bullet == '':
            tempStatsDict['last_bullet'] = 0
            tempStatsDict['draw_bullet'] = 0
            tempStatsDict['loss_bullet'] = 0
            tempStatsDict['win_bullet'] = 0

        else:
            tempStatsDict['last_bullet'] = chess_bullet['last']['rating']

            tempStatsDict['draw_bullet'] = chess_bullet['record']['draw']
            tempStatsDict['loss_bullet'] = chess_bullet['record']['loss']
            tempStatsDict['win_bullet'] = chess_bullet['record']['win']

        chess_rapid = playersStats.get('chess_rapid')
        if chess_rapid == " " or chess_rapid == None:
            tempStatsDict['last_rapid'] = 0
            tempStatsDict['draw_rapid'] = 0
            tempStatsDict['loss_rapid'] = 0
            tempStatsDict['win_rapid'] = 0
        else:
            tempStatsDict['last_rapid'] = chess_rapid['last']['rating']
            tempStatsDict['draw_rapid'] = chess_rapid['record']['draw']
            tempStatsDict['loss_rapid'] = chess_rapid['record']['loss']
            tempStatsDict['win_rapid'] = chess_rapid['record']['win']

        fide = playersStats.get('fide',0)
        tempStatsDict['FIDE'] = fide

        dictStats.append(tempStatsDict)

    now = datetime.now()
    date_time = now.strftime("%m%d%Y%H%M%S")

    fileName_local = './playerStats'+date_time+'.json'
    with open(fileName_local, 'w') as file:
        json.dump(dictStats, file)

    cloudFileName = 'PlayerStats'+date_time+'.json'

    # Uploading the json file which account stats about each player to the bucket named chess-playersstats-data
    bucket.upload_file(Key=cloudFileName,Filename=fileName_local)











    


    





