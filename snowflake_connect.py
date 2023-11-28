import snowflake.connector


def snowflake_connection():

    # Making a connection with snowflake by providing the credentials
    connect = snowflake.connector.connect(
        user= '**********',
        password= '**********',
        account = '**************',
        database = '*************',
        warehouse = '**************'
    )

    myCursor = connect.cursor()

    # Loading the streamers data into final table(streamers) from streamers_raw_tbl by applying neccessary transformation as the data is
    # present in json format  

    insert_statement_streamers = '''insert into streamers(is_live,username)
                                        select streamers_flat.value['isLive'] as is_live, streamers_flat.value['username']::string as username
                                        from 
                                        (select streamers_json as json_data 
                                        from streamers_raw_tbl) streamers,
                                            lateral flatten(input => streamers.json_data) streamers_flat'''
    
    myCursor.execute(insert_statement_streamers)




#    Loading the data into the players_info (final table) from the players_info_raw after applying neccessary transformation


    insert_statement_playersInfo = '''insert into players_info(username,followers,country,joined,location,name,player_id,status,title)
    select players_flat.value['username']:: string as username, players_flat.value['followers'] as followers, substring(players_flat.value['country']::string,35,2) 
                                                                                                                        as country,
        players_flat.value['joined'] as joined, players_flat.value['location']::string as location, players_flat.value['name']::string as name,
        players_flat.value['player_id'] as player_id, players_flat.value['status']::string as status, players_flat.value['title']::string as title
        from
                (select players_json as json_data 
                from players_info_raw) playersinfo,
                lateral flatten(input => playersinfo.json_data) players_flat'''

    myCursor.execute(insert_statement_playersInfo)

    # Loading the data into the players_stats (final table) from the player_stats_json after applying neccessary transformation

    insert_statement_playersStats = '''insert into players_stats(last_blitz,draw_blitz,loss_blitz,win_blitz,last_bullet,draw_bullet,loss_bullet,win_bullet,last_rapid,draw_rapid,loss_rapid,
                        win_rapid,fide)
                        select players_stats_flat.value['last_blitz'] as last_blitz, players_stats_flat.value['draw_blitz'] as draw_blitz, 
                               players_stats_flat.value['loss_blitz'] as loss_blitz, players_stats_flat.value['win_blitz'] as win_blitz,
                               players_stats_flat.value['last_bullet'] as last_bullet, players_stats_flat.value['draw_bullet'] as draw_bullet,
                               players_stats_flat.value['loss_bullet'] as loss_bullet, players_stats_flat.value['win_bullet'] as win_bullet,
                               players_stats_flat.value['last_rapid'] as last_rapid, players_stats_flat.value['draw_rapid'] as draw_rapid,
                               players_stats_flat.value['loss_rapid'] as loss_rapid, players_stats_flat.value['win_rapid'] as win_rapid,
                               players_stats_flat.value['FIDE'] as fide
                               from (
                                    select player_stats_json as json_data 
                                     from player_stats_raw) playersstats,
                                     lateral flatten(input => playersstats.json_data) players_stats_flat'''
    
    myCursor.execute(insert_statement_playersStats)

    
    
    
    # Finally closing the connection.

    connect.close()