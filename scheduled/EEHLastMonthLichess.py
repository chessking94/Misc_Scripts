import pyodbc as sql
import pandas as pd
import datetime as dt
import requests
import os
import json

def main():  
    my_games_flag = 1

    conn = sql.connect('Driver={ODBC Driver 17 for SQL Server};Server=HUNT-PC1;Database=ChessAnalysis;Trusted_Connection=yes;')   
    if my_games_flag == 1:     
        qry_text = "SELECT ISNULL(LastName, '') + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE EEHFlag = 1 AND Source = 'Lichess'"
    else:
        qry_text = "SELECT ISNULL(LastName, '') + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE EEHFlag = 0 AND Source = 'Lichess' AND DownloadFlag = 1"
    users = pd.read_sql(qry_text, conn).values.tolist()
    rec_ct = len(users)
    if rec_ct == 0:
        conn.close()
        print('No users selected to download!')
        quit()
    conn.close()
    
    today = dt.date.today()
    first = today.replace(day=1)
    lastmonth = first - dt.timedelta(days=1)
    start_dte = dt.datetime(year=lastmonth.year, month=lastmonth.month, day=1, hour=0, minute=0, second=0)
    end_dte = dt.datetime(year=today.year, month=today.month, day=1, hour=0, minute=0, second=0)
    # because I'm lazy I'll hard-code the milli/micro/nanoseconds
    utc_start = str(int(start_dte.replace(tzinfo=dt.timezone.utc).timestamp())) + '000'
    utc_end = str(int(end_dte.replace(tzinfo=dt.timezone.utc).timestamp())) + '000'
    yyyy = lastmonth.strftime('%Y')
    mm = lastmonth.strftime('%m')

    dload_path = r'C:\Users\eehunt\Documents\Chess\Scripts\Lichess'
    for i in users:
        # get auth token
        fpath = r'C:\Users\eehunt\Repository'
        fname = 'keys.json'
        with open(os.path.join(fpath, fname), 'r') as f:
            json_data = json.load(f)
        token_value = json_data.get('LichessAPIToken')

        dload_url = 'https://lichess.org/api/games/user/' + i[1] + '?since=' + utc_start + '&until=' + utc_end
        dload_name = i[1] + '_' + str(yyyy) + str(mm) + '.pgn'
        dload_file = os.path.join(dload_path, dload_name)
        hdr = {'Authorization': 'Bearer ' + token_value}
        # add error handling if url is bad
        with requests.get(dload_url, headers=hdr, stream=True) as resp:
            with open(dload_file, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8196):
                    f.write(chunk)

    # verify files exist to continue processing
    file_list = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
    if len(file_list) > 0:
        # merge and clean pgn
        if rec_ct == 1:
            merge_name = dload_name
            clean_name = 'Lichess_' + users[0][1] + '_' + str(yyyy) + str(mm) + '.pgn'
        else:
            merge_name = 'LichessMerged_Multiple_' + str(yyyy) + str(mm) + '.pgn'
            clean_name = 'Lichess_Multiple_' + str(yyyy) + str(mm) + '.pgn'
            cmd_text = 'copy /B *.pgn ' + merge_name
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