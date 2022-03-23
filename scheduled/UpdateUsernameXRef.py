import datetime as dt
import json
from urllib import request, error
import pyodbc as sql
import pandas as pd

# TODO: Add argument parsing to allow for command-line options, thinking primarily of refreshing a specific user vs. entire dataset
# Could do more like only those active within last month, only one site, etc

def ChessComUserUpdate():
    conn = sql.connect('Driver={ODBC Driver 17 for SQL Server};Server=HUNT-PC1;Database=ChessAnalysis;Trusted_Connection=yes;')    
    qry_text = "SELECT PlayerID, Username FROM UsernameXRef WHERE Source = 'Chess.com'"
    #qry_text = "SELECT PlayerID, Username FROM UsernameXRef WHERE Source = 'Chess.com' AND EEHFlag = 0 AND DownloadFlag = 1"
    users = pd.read_sql(qry_text, conn).values.tolist()
    rec_ct = len(users)
    if rec_ct == 0:
        conn.close()
        print('No users found!')
        quit()

    csr = conn.cursor()
    ctr = 1
    for i in users:
        print('Chess.com ' + str(ctr) + '/' + str(rec_ct) + ': ' + i[1])
        sql_cmd = ''
        url = 'https://api.chess.com/pub/player/' + i[1]
        try:
            # last active datetime and status
            json_data = request.urlopen(url).read()
            json_loaded = json.loads(json_data)
            last_online = json_loaded['last_online']
            sql_date = str(dt.datetime.fromtimestamp(last_online))
            if json_loaded['status'][0:6] == 'closed':
                user_status = 'Closed'
            else:
                user_status = 'Open'

            # ratings and game counts
            url_ratings = 'https://api.chess.com/pub/player/' + i[1] + '/stats'
            json_rating_data = request.urlopen(url_ratings).read() # since user already is confirmed to exist, don't need error handling
            json_rating_loaded = json.loads(json_rating_data)

            bullet_rating = 'NULL'
            blitz_rating = 'NULL'
            rapid_rating = 'NULL'
            daily_rating = 'NULL'  
            bullet_games = '0'
            blitz_games = '0'
            rapid_games = '0'
            daily_games = '0'
            if 'chess_bullet' in json_rating_loaded:
                bullet_rating = json_rating_loaded['chess_bullet']['last']['rating']
                bullet_games = json_rating_loaded['chess_bullet']['record']['win'] + json_rating_loaded['chess_bullet']['record']['loss'] + json_rating_loaded['chess_bullet']['record']['draw']
            if 'chess_blitz' in json_rating_loaded:
                blitz_rating = json_rating_loaded['chess_blitz']['last']['rating']
                blitz_games = json_rating_loaded['chess_blitz']['record']['win'] + json_rating_loaded['chess_blitz']['record']['loss'] + json_rating_loaded['chess_blitz']['record']['draw']
            if 'chess_rapid' in json_rating_loaded:
                rapid_rating = json_rating_loaded['chess_rapid']['last']['rating']
                rapid_games = json_rating_loaded['chess_rapid']['record']['win'] + json_rating_loaded['chess_rapid']['record']['loss'] + json_rating_loaded['chess_rapid']['record']['draw']
            if 'chess_daily' in json_rating_loaded:
                daily_rating = json_rating_loaded['chess_daily']['last']['rating']
                daily_games = json_rating_loaded['chess_daily']['record']['win'] + json_rating_loaded['chess_daily']['record']['loss'] + json_rating_loaded['chess_daily']['record']['draw']

            # set SQL command
            sql_cmd = "UPDATE UsernameXRef SET LastActiveOnline = '" + sql_date + "'"
            sql_cmd = sql_cmd + ", UserStatus = '" + user_status + "'"
            sql_cmd = sql_cmd + ", BulletRating = " + str(bullet_rating)
            sql_cmd = sql_cmd + ", BlitzRating = " + str(blitz_rating)
            sql_cmd = sql_cmd + ", RapidRating = " + str(rapid_rating)
            sql_cmd = sql_cmd + ", DailyRating = " + str(daily_rating)
            sql_cmd = sql_cmd + ", BulletGames = " + str(bullet_games)
            sql_cmd = sql_cmd + ", BlitzGames = " + str(blitz_games)
            sql_cmd = sql_cmd + ", RapidGames = " + str(rapid_games)
            sql_cmd = sql_cmd + ", DailyGames = " + str(daily_games)
            sql_cmd = sql_cmd + ' WHERE PlayerID = ' + str(i[0])
        except error.HTTPError as e:
            if e.getcode() == 404:
                sql_cmd = "UPDATE UsernameXRef SET UserStatus = 'DNE' WHERE PlayerID = " + str(i[0])
            else:
                # could handle these differently, but should happen very rarely if ever
                sql_cmd = "UPDATE UsernameXRef SET UserStatus = NULL WHERE PlayerID = " + str(i[0])
        if sql_cmd != '':
            #print(sql_cmd)
            csr.execute(sql_cmd)
            conn.commit()
        ctr = ctr + 1
    conn.close()

def LichessUserUpdate():
    conn = sql.connect('Driver={ODBC Driver 17 for SQL Server};Server=HUNT-PC1;Database=ChessAnalysis;Trusted_Connection=yes;')    
    qry_text = "SELECT PlayerID, Username FROM UsernameXRef WHERE Source = 'Lichess'"
    #qry_text = "SELECT PlayerID, Username FROM UsernameXRef WHERE Source = 'Lichess' AND EEHFlag = 0 AND DownloadFlag = 1"
    users = pd.read_sql(qry_text, conn).values.tolist()
    rec_ct = len(users)
    if rec_ct == 0:
        conn.close()
        print('No users found!')
        quit()

    csr = conn.cursor()
    ctr = 1
    for i in users:
        print('Lichess ' + str(ctr) + '/' + str(rec_ct) + ': ' + i[1])
        sql_cmd = ''
        url = 'https://lichess.org/api/user/' + i[1]
        try:
            json_data = request.urlopen(url).read()
            json_loaded = json.loads(json_data)
            try:
                # last active datetime
                last_online = json_loaded['seenAt']//1000
                sql_date = str(dt.datetime.fromtimestamp(last_online))

                # ratings and game counts
                bullet_rating = 'NULL'
                blitz_rating = 'NULL'
                rapid_rating = 'NULL'
                daily_rating = 'NULL'
                bullet_games = '0'
                blitz_games = '0'
                rapid_games = '0'
                daily_games = '0'
                if 'bullet' in json_loaded['perfs']:
                    bullet_rating = json_loaded['perfs']['bullet']['rating']
                    bullet_games = json_loaded['perfs']['bullet']['games']
                if 'blitz' in json_loaded['perfs']:
                    blitz_rating = json_loaded['perfs']['blitz']['rating']
                    blitz_games = json_loaded['perfs']['blitz']['games']
                if 'rapid' in json_loaded['perfs']:
                    rapid_rating = json_loaded['perfs']['rapid']['rating']
                    rapid_games = json_loaded['perfs']['rapid']['games']
                if 'correspondence' in json_loaded['perfs']:
                    daily_rating = json_loaded['perfs']['correspondence']['rating']
                    daily_games = json_loaded['perfs']['correspondence']['games']

                # set SQL command
                sql_cmd = "UPDATE UsernameXRef SET LastActiveOnline = '" + sql_date + "'"
                sql_cmd = sql_cmd + ", UserStatus = 'Open'"
                sql_cmd = sql_cmd + ", BulletRating = " + str(bullet_rating)
                sql_cmd = sql_cmd + ", BlitzRating = " + str(blitz_rating)
                sql_cmd = sql_cmd + ", RapidRating = " + str(rapid_rating)
                sql_cmd = sql_cmd + ", DailyRating = " + str(daily_rating)
                sql_cmd = sql_cmd + ", BulletGames = " + str(bullet_games)
                sql_cmd = sql_cmd + ", BlitzGames = " + str(blitz_games)
                sql_cmd = sql_cmd + ", RapidGames = " + str(rapid_games)
                sql_cmd = sql_cmd + ", DailyGames = " + str(daily_games)
                sql_cmd = sql_cmd + ' WHERE PlayerID = ' + str(i[0])
            except:
                if 'closed' or 'disabled' in json_loaded:
                    sql_cmd = "UPDATE UsernameXRef SET UserStatus = 'Closed' WHERE PlayerID = " + str(i[0])
                else:
                    sql_cmd = "UPDATE UsernameXRef SET LastActiveOnline = NULL WHERE PlayerID = " + str(i[0])
                
        except error.HTTPError as e:
            if e.getcode() == 404:
                sql_cmd = "UPDATE UsernameXRef SET UserStatus = 'DNE' WHERE PlayerID = " + str(i[0])
            else:
                # could handle these differently, but should happen very rarely if ever
                sql_cmd = "UPDATE UsernameXRef SET UserStatus = NULL WHERE PlayerID = " + str(i[0])
        if sql_cmd != '':
            #print(sql_cmd)
            csr.execute(sql_cmd)
            conn.commit()
        ctr = ctr + 1
    conn.close()

def main():
    ChessComUserUpdate()
    LichessUserUpdate()


if __name__ == '__main__':
    main()