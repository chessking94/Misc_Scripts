import datetime as dt
import fileinput
import json
import os
import shutil as sh

import chess
import chess.pgn
import pandas as pd
import pyodbc as sql
import requests


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
    conn_str = get_connstr()
    conn = sql.connect(conn_str)
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
        cmd_text = 'pgn-extract -N -V -D --quiet --output ' + clean_name + ' ' + merge_name
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

def get_connstr():
    # get SQL Server connection string from private file
    fpath = r'C:\Users\eehunt\Repository'
    fname = 'confidential.json'
    with open(os.path.join(fpath, fname), 'r') as t:
        key_data = json.load(t)
    conn_str = key_data.get('SqlServerConnectionStringTrusted')
    return conn_str

def get_lichesstoken():
    # get Lichess API token from private file
    fpath = r'C:\Users\eehunt\Repository'
    fname = 'confidential.json'
    with open(os.path.join(fpath, fname), 'r') as t:
        key_data = json.load(t)
    token_value = key_data.get('LichessAPIToken')
    return token_value

def lichessgames():
    conn_str = get_connstr()
    conn = sql.connect(conn_str) 
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
    token_value = get_lichesstoken()
    for i in users:
        dload_url = 'https://lichess.org/api/games/user/' + i[1] + '?clocks=true&since=' + utc_start + '&until=' + utc_end
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
        cmd_text = 'pgn-extract -N -V -D --quiet --output ' + clean_name + ' ' + merge_name
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
    pgn = open(os.path.join(output_path, updated_tc_name), mode='r', encoding='utf-8', errors='replace')

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
        sort_file.write(str(game_text[i]) + '\n\n')
    sort_file.close()  
    pgn.close()
    
    # create White and Black files
    white_tag = 'WhiteTag.txt'
    white_tag_full = os.path.join(output_path, white_tag)
    with open(white_tag_full, 'w') as wt:
        wt.write('White "NefariousNebula"\n')
        wt.write('White "AimlessAlgebraist"')
    
    black_tag = 'BlackTag.txt'
    black_tag_full = os.path.join(output_path, black_tag)
    with open(black_tag_full, 'w') as bl:
        bl.write('Black "NefariousNebula"\n')
        bl.write('Black "AimlessAlgebraist"')
    
    white_all = 'White_All_' + yyyy + mm + os.path.splitext(sort_name)[1]
    black_all = 'Black_All_' + yyyy + mm + os.path.splitext(sort_name)[1]

    cmd_text = 'pgn-extract --quiet -t' + white_tag + ' --output ' + white_all + ' ' + sort_name
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    cmd_text = 'pgn-extract --quiet -t' + black_tag + ' --output ' + black_all + ' ' + sort_name
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    # create CC and Live color-specific files
    cc_tag = 'CCTag.txt'
    cc_tag_full = os.path.join(output_path, cc_tag)
    with open(cc_tag_full, 'w') as cc:
        cc.write('TimeControl >= "86400"')
    
    live_tag = 'LiveTag.txt'
    live_tag_full = os.path.join(output_path, live_tag)
    with open(live_tag_full, 'w') as lv:
        lv.write('TimeControl <= "86399"')
    
    white_cc = 'EEH_Online_CC_White_' + yyyy + mm + os.path.splitext(white_all)[1]
    black_cc = 'EEH_Online_CC_Black_' + yyyy + mm + os.path.splitext(black_all)[1]
    white_live = 'EEH_Online_Live_White_' + yyyy + mm + os.path.splitext(white_all)[1]
    black_live = 'EEH_Online_Live_Black_' + yyyy + mm + os.path.splitext(black_all)[1]

    cmd_text = 'pgn-extract --quiet -t' + cc_tag + ' --output ' + white_cc + ' ' + white_all
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    cmd_text = 'pgn-extract --quiet -t' + cc_tag + ' --output ' + black_cc + ' ' + black_all
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    cmd_text = 'pgn-extract --quiet -t' + live_tag + ' --output ' + white_live + ' ' + white_all
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    cmd_text = 'pgn-extract --quiet -t' + live_tag + ' --output ' + black_live + ' ' + black_all
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    # backtrack and create complete CC and Live files
    cc_all = 'EEH_Online_CC_All_' + yyyy + mm + os.path.splitext(sort_name)[1]
    live_all = 'EEH_Online_Live_All_' + yyyy + mm + os.path.splitext(sort_name)[1]
    cmd_text = 'pgn-extract --quiet -t' + cc_tag + ' --output ' + cc_all + ' ' + sort_name
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    cmd_text = 'pgn-extract --quiet -t' + live_tag + ' --output ' + live_all + ' ' + sort_name
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    # clean up
    os.remove(os.path.join(output_path, merge_name))
    os.remove(os.path.join(output_path, updated_tc_name))
    os.remove(os.path.join(output_path, white_all))
    os.remove(os.path.join(output_path, black_all))
    os.remove(white_tag_full)
    os.remove(black_tag_full)
    os.remove(cc_tag_full)
    os.remove(live_tag_full)
    
    old_name = os.path.join(output_path, sort_name)
    new_name = os.path.join(output_path, 'EEH_Online_All_' + yyyy + mm + os.path.splitext(sort_name)[1])
    os.rename(old_name, new_name)

    # delete empty files, if no games are in a file
    dir_files = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]
    for f in dir_files:
        if os.path.getsize(os.path.join(output_path, f)) == 0:
            os.remove(os.path.join(output_path, f))


def main():
    archiveold()
    chesscomgames()
    lichessgames()
    processfiles()


if __name__ == '__main__':
    main()
