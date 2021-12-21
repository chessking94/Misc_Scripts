import pyodbc as sql
import pandas as pd
import datetime as dt
from urllib import request
import os

def main():
    my_games_flag = 1 # 0/1 value; if 0 use below user, if 1 use file

    conn = sql.connect('Driver={ODBC Driver 17 for SQL Server};Server=HUNT-PC1;Database=ChessAnalysis;Trusted_Connection=yes;')   
    if my_games_flag == 1:     
        qry_text = "SELECT ISNULL(LastName, '') + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE EEHFlag = 1 AND Source = 'Chess.com'"
    else:
        qry_text = "SELECT ISNULL(LastName, '') + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE EEHFlag = 0 AND Source = 'Chess.com' AND DownloadFlag = 1"
    users = pd.read_sql(qry_text, conn).values.tolist()
    rec_ct = len(users)
    if rec_ct == 0:
        conn.close()
        print('No users selected to download!')
        quit()
    conn.close()

    today = dt.date.today()
    first = today.replace(day=1)
    lastMonth = first - dt.timedelta(days=1)
    yyyy = lastMonth.strftime('%Y')
    mm = lastMonth.strftime('%m')

    dload_path = r'C:\Users\eehunt\Documents\Chess\Scripts\ChessCom'
    for i in users:
        url = 'https://api.chess.com/pub/player' + '/' + i[1] + '/games/' + str(yyyy) + '/' + str(mm) + '/pgn'
        dload_name = i[1] + '_' + str(yyyy) + str(mm) + '.pgn'
        dload_file = os.path.join(dload_path, dload_name)
        request.urlretrieve(url, dload_file)

    file_list = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
    if len(file_list) > 0:
        # merged and clean pgn
        if rec_ct == 1:
            merge_name = dload_name
            clean_name = 'ChessCom_' + users[0][1] + '_' + str(yyyy) + str(mm) + '.pgn'
        else:
            merge_name = 'ChessComMerged_Multiple_' + str(yyyy) + str(mm) + '.pgn'
            clean_name = 'ChessCom_Multiple_' + str(yyyy) + str(mm) + '.pgn'
            cmd_text = 'copy /B *.pgn ' + merge_name # /B will avoid the random extra char at the end
            if os.getcwd != dload_path:
                os.chdir(dload_path)
            os.system('cmd /C ' + cmd_text)
        cmd_text = 'pgn-extract -C -N -V -D -pl2 --quiet --nosetuptags --output ' + clean_name + ' ' + merge_name
        if os.getcwd != dload_path:
            os.chdir(dload_path)
        os.system('cmd /C ' + cmd_text)

        # delete old files
        dir_files = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
        for filename in dir_files:
            if filename != clean_name:
                fname_relpath = os.path.join(dload_path, filename)
                os.remove(fname_relpath)


if __name__ == '__main__':
    main()