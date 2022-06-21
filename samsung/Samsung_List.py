import pandas as pd
from datetime import date, datetime, timedelta
import os

from pandas.core.frame import DataFrame

now = datetime.now()

'''
effect = []
if now.weekday() == 4: 
    effect.append((now + timedelta(3)).strftime('%Y-%m-%d'))
    effect.append((now + timedelta(4)).strftime('%Y-%m-%d'))
elif now.weekday() == 3: 
    effect.append((now + timedelta(1)).strftime('%Y-%m-%d'))
    effect.append((now + timedelta(4)).strftime('%Y-%m-%d'))
else:
    effect.append((now + timedelta(1)).strftime('%Y-%m-%d'))
    effect.append((now + timedelta(2)).strftime('%Y-%m-%d'))
'''

## 작업스케줄러 오전, 오후
if 'PM' in now.strftime('%p'):
    sysdate = now.strftime('%Y%m%d') 
    if now.weekday() == 4: effect_date = (now + timedelta(3)).strftime('%Y-%m-%d')
    else: effect_date = (now + timedelta(1)).strftime('%Y-%m-%d')
else:
    sysdate = (now - timedelta(1)).strftime('%Y%m%d')
    if now.weekday() == 4: effect_date = (now + timedelta(3)).strftime('%Y-%m-%d')
    elif now.weekday() == 5: effect_date = (now + timedelta(3)).strftime('%Y-%m-%d')
    else: effect_date = (now + timedelta(1)).strftime('%Y-%m-%d')

#sysdate = now.strftime('20220616') # 발행리스트 파일 날짜 YYYYMMDD
#effect_date = now.strftime('2022-06-17') # 발행일 날짜 YYYY-MM-DD

path = r'Y:\(0000)금융공학연구소\402.파생상품팀\파생상품팀 이슈\06. Live\20210517_삼성증권 Live\발행리스트'
file_list = os.listdir(path)

for file_name in file_list:
    if sysdate in file_name: break
    else: file_name = ''
try:
    df = pd.read_excel(fr'{path}\{file_name}', skiprows=[0])

    raw_data = df[['발행일','회차','KRS코드','모집방식','유형']].sort_values(by='회차', ascending=True)
    # 공모 제외
    except_row = raw_data['모집방식'].isin(['공모', '공모(기관)'])
    data = raw_data[~except_row].copy()
    # 발행일 필터링
    data['발행일'] = pd.to_datetime(data['발행일'], format='%Y%m%d')
    f_data = data[data['발행일'] == effect_date]
    # OTC
    #otc_data = f_data[f_data['유형'] == 'OTC']
    #otc_list = dict(zip(otc_data['회차'],otc_data['KRS코드']))
    # OTC 아닌거
    #notc_data = f_data[f_data['유형'] != 'OTC']
    #notc_list = dict(zip(notc_data['회차'],notc_data['KRS코드']))

    dict_list = dict(zip(f_data['회차'],f_data['KRS코드']))
    print(dict_list)
except Exception as e:
    print(f'발행리스트 오류, {e}')

