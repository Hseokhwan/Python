# %%
from reprlib import recursive_repr
from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import json
from pprint import pprint
import requests
import pandas as pd
import html5lib

def Cut_Sheet(soup,start_text='모집\s?또는\s?매출의\s?개요$',end_text='투자위험요소$'):
    file_to_save=[]
    for tag_s,tag_e in zip(soup.find_all(string=re.compile(start_text)),soup.find_all(string=re.compile(end_text))):
        limits = len(tag_s.parent.find_all_next(['p','table'],recursive=False)) - len(tag_e.parent.find_all_next(['p','table'],recursive=False))
        before_file = tag_s.parent.find_all_next(['p','table'],recursive=False,limit=limits)
        before_file.insert(0,tag_s.find_parent('p'))
        file_to_save.append(BeautifulSoup(''.join(map(str, before_file)),'html.parser'))
    return file_to_save

API_KEY = "894dc5368ce9574789c64978581694a54daa4a3e"
args = {
    'bgn_de' : '20220119', # 검색 시작일
    'end_de' : '20220119', # 검색 종료일
    'pblntf_ty' : 'C', # 공시유형
    'corp_code' : '016360', # 회사 코드  016360 (미래), 
    'last_reprt_at' : 'Y', # 최종 보고서
    'sort_mth' : 'asc' # 정렬
    #'corp_cls' : 'Y', # 법인 구분(Y=유가)
}
args_str = ''
for k, v in args.items():
    args_str += '&%s=%s' % (k, v)

url = 'https://opendart.fss.or.kr/api/list.json?crtfc_key=%s%s' % (API_KEY, args_str)

response = urlopen(url).read() # requests.get(url)
readjson = json.loads(response) #json()
#pprint(readjson)

get_rceptno=[]
for i in range(1,readjson['total_page']+1):
    
    response = urlopen(url + f'&page_no={i}').read()
    readjson = json.loads(response)
      
    for j in  range(0,len(readjson['list'])):
        if '투자설명서' in readjson['list'][j]['report_nm']:
            get_rceptno.append(readjson['list'][j]['rcept_no'])
#print(get_rceptno)

# %%
rcept_no = '20220119000156'
dcmno = '8356005' # 삼성
rcept_no = '20220111000376'
dcmno = '8348472' # 교보

url_to_crawl = f"http://dart.fss.or.kr/report/viewer.do?rcpNo={rcept_no}&dcmNo={dcmno}&eleId=1&offset=0&length=0&dtd=dart3.xsd"
print(url_to_crawl)

soup = BeautifulSoup(urlopen(url_to_crawl).read(),'html.parser')

sheet = Cut_Sheet(soup)[0] # 반복

def Info_Sheet(sheet):
    """
    [종목명 / 기초자산 / 발행가액 / 청약시작일 / 청약종료일 / 발행일 / 만기일]
    * sheet = Cut_Sheet
    """
    def extract_text(index):
        return df[df_col.str.contains(re.compile(index))].iat[0,1]
    def extract_date(index):
        date = df[df_col.str.contains(re.compile(index))].iat[0,1]
        try: return re.search('\d+년[\s\d]+월[\s\d]+일', date).group()
        except: return date
    sheet_tables = sheet.select('table') 

    """ [상품 기초 정보]
    항목 Column 없는 경우, Column 생성 ex) 키움증권 """
    df = pd.read_html(str(sheet_tables), match='청약', header=0)[0]
    
    if len(df.columns) >= 3:
        df = df.drop(df.columns[0], axis=1)
    if re.search('항.*?목', df.columns[0]) == None:
        df = df.transpose().reset_index().transpose()
    df_col = df[df.columns[0]]

    name = df.iat[0,1]
    effect_price = extract_text('발\s*행\s*가\s*액')
    sub_start_date = extract_date('청\s*약\s*시\s*작')
    sub_end_date = extract_date('청\s*약\s*종\s*료')
    effect_date = extract_date('발\s*행\s*일')
    if '삼성' not in name: 
        exp_date = None
        exp_pay_date = extract_date('만\s*기\s*일')
    elif '삼성' in name:
        exp_date = extract_date('만\s*기\s*일')
        exp_pay_date = extract_date('지\s*급\s*일')

    """ [기초자산]
    변동성 table 사용 """
    df = pd.read_html(str(sheet_tables), match='변동성', header=0)[0]
    df_under = df.iat[0,1]
    if '-' in df_under: underlying = re.findall('-[^%]*:[^%]*%', df_under)
    else: underlying = re.findall('\S[^%]*:[^%]*%', df_under)
    
    return {
        '종목명' : name,
        '기초자산' : underlying,
        '발행가액' : effect_price,
        '청약시작일' : sub_start_date,
        '청약종료일' : sub_end_date,
        '발행일' : effect_date,
        '만기평가일' : exp_date,
        '만기지급일' : exp_pay_date
    }

Info_Sheet(sheet)

# %%
dict_test = {
    '20220118000317' : '8355184', # 유안타
    '20220125000512' : '8363010', # 유진
    '20220118000284' : '8355055', # KB
    '20220120000129' : '8357635', # 키움
    '20220119000331' : '8356867', # 하나
    '20220121000362' : '8359923', # 하이
    '20220119000120' : '8356021', # 한국
    '20220119000251' : '8356521', # 한화
    '20220112000103' : '8349177', # 현대차
    '20220119000320' : '8356808', # 메리츠
    '20220125000299' : '8362377', # 미래
    '20220119000156' : '8356005', # 삼성
    '20220121000213' : '8359509', # BNK
    '20220112000258' : '8349659', # DB
    '20220112000170' : '8349381', # NH
    '20191209000163' : '6988200', # SK
    '20220112000279' : '8349685', # 교보
    '20220119000113' : '8355993', # 대신
    '20220119000218' : '8356378', # 신영
    '20220118000146' : '8354561', # 신한
    '20220119000134' : '8356060', # IBK
}

for k, v in dict_test.items():
    url_to_crawl = f"http://dart.fss.or.kr/report/viewer.do?rcpNo={k}&dcmNo={v}&eleId=1&offset=0&length=0&dtd=dart3.xsd"
    #print(url_to_crawl)
    soup = BeautifulSoup(urlopen(url_to_crawl).read(),'html.parser')

    sheet = Cut_Sheet(soup)[0] # 반복

    try: print(Info_Sheet(sheet))
    except Exception as e: print(e)

# %%
