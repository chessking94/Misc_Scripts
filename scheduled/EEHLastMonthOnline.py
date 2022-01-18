import pyodbc as sql
import pandas as pd
import datetime as dt
import requests
import os
import json
import shutil as sh
import fileinput
import chess
import chess.pgn

def archiveold():
    output_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output'
    archive_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output\old'

    file_list = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]
    if len(file_list) > 0:
        for file in file_list:
            old_name = os.path.join(output_path, file)
            new_name = os.path.join(archive_path, file)
            sh.move(old_name, new_name)

def chesscomgames():
    conn = sql.connect('Driver={ODBC Driver 17 for SQL Server};Server=HUNT-PC1;Database=ChessAnalysis;Trusted_Connection=yes;')   
    qry_text = "SELECT ISNULL(LastName, '') + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE EEHFlag = 1 AND Source = 'Chess.com'"
    users = pd.read_sql(qry_text, conn).values.tolist()
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
        with requests.get(url, stream=True) as resp:
            if resp.status_code != 200:
                print('Unable to complete request! Request returned code ' + resp.status_code)
            else:
                with open(dload_file, 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=8196):
                        f.write(chunk)

    file_list = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
    if len(file_list) > 0:
        merge_name = dload_name
        clean_name = 'ChessCom_' + users[0][1] + '_' + str(yyyy) + str(mm) + '.pgn'
        cmd_text = 'pgn-extract -C -N -V -D --quiet --output ' + clean_name + ' ' + merge_name
        if os.getcwd != dload_path:
            os.chdir(dload_path)
        os.system('cmd /C ' + cmd_text)

        dir_files = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
        for filename in dir_files:
            if filename != clean_name:
                fname_relpath = os.path.join(dload_path, filename)
                os.remove(fname_relpath)

        output_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output'
        old_loc = os.path.join(dload_path, clean_name)
        new_loc = os.path.join(output_path, clean_name)
        os.rename(old_loc, new_loc)

def lichessgames():  
    conn = sql.connect('Driver={ODBC Driver 17 for SQL Server};Server=HUNT-PC1;Database=ChessAnalysis;Trusted_Connection=yes;')   
    qry_text = "SELECT ISNULL(LastName, '') + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE EEHFlag = 1 AND Source = 'Lichess'"
    users = pd.read_sql(qry_text, conn).values.tolist()
    conn.close()
    
    today = dt.date.today()
    first = today.replace(day=1)
    lastmonth = first - dt.timedelta(days=1)
    start_dte = dt.datetime(year=lastmonth.year, month=lastmonth.month, day=1, hour=0, minute=0, second=0)
    end_dte = dt.datetime(year=today.year, month=today.month, day=1, hour=0, minute=0, second=0)
    utc_start = str(int(start_dte.replace(tzinfo=dt.timezone.utc).timestamp())) + '000'
    utc_end = str(int(end_dte.replace(tzinfo=dt.timezone.utc).timestamp())) + '000'
    yyyy = lastmonth.strftime('%Y')
    mm = lastmonth.strftime('%m')

    dload_path = r'C:\Users\eehunt\Documents\Chess\Scripts\Lichess'
    for i in users:
        fpath = r'C:\Users\eehunt\Repository'
        fname = 'keys.json'
        with open(os.path.join(fpath, fname), 'r') as f:
            json_data = json.load(f)
        token_value = json_data.get('LichessAPIToken')

        dload_url = 'https://lichess.org/api/games/user/' + i[1] + '?since=' + utc_start + '&until=' + utc_end
        dload_name = i[1] + '_' + str(yyyy) + str(mm) + '.pgn'
        dload_file = os.path.join(dload_path, dload_name)
        hdr = {'Authorization': 'Bearer ' + token_value}
        with requests.get(dload_url, headers=hdr, stream=True) as resp:
            with open(dload_file, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8196):
                    f.write(chunk)

    file_list = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
    if len(file_list) > 0:
        merge_name = dload_name
        clean_name = 'Lichess_' + users[0][1] + '_' + str(yyyy) + str(mm) + '.pgn'
        cmd_text = 'pgn-extract -C -N -V -D --quiet --output ' + clean_name + ' ' + merge_name
        if os.getcwd != dload_path:
            os.chdir(dload_path)
        os.system('cmd /C ' + cmd_text)

        dir_files = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
        for filename in dir_files:
            if filename != clean_name:
                fname_relpath = os.path.join(dload_path, filename)
                os.remove(fname_relpath)
        
        output_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output'
        old_loc = os.path.join(dload_path, clean_name)
        new_loc = os.path.join(output_path, clean_name)
        os.rename(old_loc, new_loc)

def processfiles():
    dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')
    output_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output'

    today = dt.date.today()
    first = today.replace(day=1)
    lastmonth = first - dt.timedelta(days=1)
    yyyy = lastmonth.strftime('%Y')
    mm = lastmonth.strftime('%m')
    
    merge_name = 'EEH_Online_All_' + yyyy + mm + 'Merged.pgn'
    
    cmd_text = 'copy /B *.pgn ' + merge_name
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)
    
    # delete original files
    dir_files = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]
    for filename in dir_files:
        if filename != merge_name:
            fname_relpath = os.path.join(output_path, filename)
            os.remove(fname_relpath)

    # update correspondence game TimeControl tag; missing from Lichess games
    updated_tc_name = os.path.splitext(merge_name)[0] + '_TimeControlFixed' + os.path.splitext(merge_name)[1]
    ofile = os.path.join(output_path, merge_name)
    nfile = os.path.join(output_path, updated_tc_name)
    searchExp = '[TimeControl "-"]'
    replaceExp = '[TimeControl "1/86400"]'
    wfile = open(nfile, 'w')
    for line in fileinput.input(ofile):
        if searchExp in line:
            line = line.replace(searchExp, replaceExp)
        wfile.write(line)
    wfile.close()

    # sort game file
    pgn = open(os.path.join(output_path, updated_tc_name), mode='r', encoding='utf-8', errors='ignore')

    idx = []
    game_date = []
    game_text = []
    gm_idx = 0
    gm_txt = chess.pgn.read_game(pgn)
    while gm_txt is not None:
        idx.append(gm_idx)
        game_date.append(gm_txt.headers['Date'])
        game_text.append(gm_txt)
        gm_txt = chess.pgn.read_game(pgn)
        gm_idx = gm_idx + 1
    
    sort_name = os.path.splitext(updated_tc_name)[0] + '_Sorted' + os.path.splitext(updated_tc_name)[1]
    sort_file = open(os.path.join(output_path, sort_name), 'w')
    idx_sort = [x for _, x in sorted(zip(game_date, idx))]
    for i in idx_sort:
        txt = str(game_text[i]).encode(encoding='utf-8', errors='replace')
        sort_file.write(str(txt) + '\n\n')
    sort_file.close()  
    pgn.close()

    # need to rerun a dummy pgn-extract basically to reformat file from bytes to standard pgn
    clean_name = 'EEH_Online_All_' + yyyy + mm + os.path.splitext(sort_name)[1]
    cmd_text = 'pgn-extract -C -N -V -D --quiet --output ' + clean_name + ' ' + sort_name
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)
    
    # clean up
    os.remove(os.path.join(output_path, merge_name))
    os.remove(os.path.join(output_path, updated_tc_name))
    os.remove(os.path.join(output_path, sort_name))

def main():
    archiveold()
    chesscomgames()
    lichessgames()
    processfiles()


if __name__ == '__main__':
    main()