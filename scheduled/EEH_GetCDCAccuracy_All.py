import datetime as dt
import requests
import os
import json

def main():
    yrs = ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021']
    mths = ['01', '02', '03', '04', '05' , '06', '07', '08', '09', '10', '11', '12']

    today = dt.date.today()
    first = today.replace(day=1)
    lastMonth = first - dt.timedelta(days=1)
    yyyy = lastMonth.strftime('%Y')
    mm = lastMonth.strftime('%m')

    usr = 'NefariousNebula'

    #imp_path = r'C:\FileProcessing\Import'
    imp_path = r'C:\Users\eehunt\Repository\zMisc_Scripts\scheduled'
    imp_name = usr + '_ChessCom_Accuracy_' + yyyy + mm + '.txt'
    imp_full = os.path.join(imp_path, imp_name)

    DELIM = '\t'
    NEWLINE = '\n'
    with open(imp_full, 'w') as imp_write:
        imp_write.write('SourceID' + DELIM + 'WhiteAccuracy' + DELIM + 'BlackAccuracy' + NEWLINE)
        for yr in yrs:
            for mth in mths:
                print('Currently reviewing games from ' + yr + mth)
                url = 'https://api.chess.com/pub/player/' + usr + '/games/' + yr + '/' + mth
                resp = requests.get(url)
                if resp.status_code == 404:
                    print('Page not found! Request returned code ' + resp.status_code + ': Yr=' + yr + ', Mo=' + mth)
                elif resp.status_code == 429:
                    print('Rate limited! Request returned code ' + resp.status_code + ': Yr=' + yr + ', Mo=' + mth)
                elif resp.status_code != 200:
                    print('Other error! Request returned code ' + resp.status_code + ': Yr=' + yr + ', Mo=' + mth)
                else:
                    json_data = json.loads(resp.content)
                    gms = json_data.get('games')
                    for g in gms:
                        src_id = g.get('url').split('/')[-1]
                        tmp_acc = g.get('accuracies')
                        acc_w = 'NULL'
                        acc_b = 'NULL'
                        if tmp_acc is not None:
                            acc_w = tmp_acc.get('white') if tmp_acc.get('white') is not None else 'NULL'
                            acc_b = tmp_acc.get('black') if tmp_acc.get('black') is not None else 'NULL'
                        imp_write.write(src_id + DELIM + str(acc_w) + DELIM + str(acc_b) + NEWLINE)


if __name__ == '__main__':
    main()