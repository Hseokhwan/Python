# %%
from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import json
from pprint import pprint
import requests
import pandas as pd
import html5lib
import numpy as np
from datetime import datetime
from collections import namedtuple, Counter
from parse import *
#from Crawling_Method import *

# %%

"""
[함수 목록]
Cut_Sheet : 투자설명서 내에서 각 종목별로 필요한 부분만 가져와서 List로 저장
Df_find_val_iloc : Dataframe 에서 특정 행의 열 값 취득
Get_date : 상환 평가일 Parsing 
Get_range : 상환 조건 Parsing
Get_payoff : 상환 수익률 Parsing
"""
###################################### [Cut_Sheet] ####################################################################################################################
def Cut_Sheet(soup,start_text='모집\s?또는\s?매출의\s?개요$',end_text='기타사항$'):
    file_to_save=[]
    for tag_s,tag_e in zip(soup.find_all(string=re.compile(start_text)), soup.find_all(string=re.compile(end_text))):
        limits = len(tag_s.parent.find_all_next(['p','table'], recursive=False)) - len(tag_e.parent.find_all_next(['p','table'], recursive=False))
        before_file = tag_s.parent.find_all_next(['p','table'], recursive=False, limit=limits)
        before_file.insert(0, tag_s.find_parent('p'))
        file_to_save.append(BeautifulSoup(''.join(map(str, before_file)), 'html.parser'))
    return file_to_save

###################################### [Df_find_val_iloc] ####################################################################################################################
def Df_find_val_iloc(table, text):
    """ table의 첫번째 열에서 regex text를 찾고 전체 테이블에서 일치하는 행,열 값을 추린 후에 마지막 열 값 취득 """
    val = list(table[table.iloc[:,0].str.contains(text)].iloc[:,-1])
    if len(val) == 1: return val[0]
    else: return val
###################################### [Get_date] ####################################################################################################################
def Get_date(df, No, exp_date):
    """
    [Parameters]
    * df : 상환조건 부분 Dataframe (df_all)
    * No : 상환차수 (Range_Barrier_df 에서 'NO' 컬럼 값)
    * exp_date : 발행정보 테이블에서 가져온 만기평가일 (exp_date)
    [Variable]
    * redem_date_row : df_all 에서 조기상환일 값
    * exp_date_row : df_all 에서 만기평가일 값
    * exp_no : No 에서 가져온 만기상환차수 갯수
    * exp_date_avg : 평가일 종가 평균일때 평가일자 리스트
    [Process]
    1. 조기상환일 parsing 후 date_tuple_list 에 추가
    2. 만기평가일 1개인 경우, parsing 후 date_tuple_list 에 추가 (exp_no 갯수만큼)
    3. 만기평가일 여러개인 경우, parsing 후 date_tuple_list 에 추가 (exp_no 갯수만큼)
    """
    def date_findall(index): return re.findall('\d+[가-힣]\d+[가-힣]\d+', index)
    def date_sub(index): return re.sub('[^\d]', '-', index)
    def date_tuple(Start_date='', End_date=''):
        """ 상환평가일 파싱 후 namedtuple로 저장 """
        d_tuple = namedtuple('d_tuple', 'Start_date, End_date')
        return d_tuple(Start_date, End_date)
    
    redem_date_row = Df_find_val_iloc(df, '중간기준가격\s*결정일\s*\(예정\)').replace(' ', '')
    exp_date_row = Df_find_val_iloc(df, '최종기준가격\s*결정일\s*\(예정\)').replace(' ', '')
    exp_no = No.count(No[-1])

    date_tuple_list = []
    try:
        for d in date_findall(redem_date_row):
            date_tuple_list.append( date_tuple(Start_date=date_sub(d), End_date=date_sub(d)) )
        if re.search('최종기준가격\s*결정일', exp_date_row) == None and '만기일' in exp_date_row:
            for i in range(exp_no):
                date_tuple_list.append( date_tuple(Start_date=exp_date, End_date=exp_date) )
        elif re.search('최종기준가격\s*결정일', exp_date_row) != None:
            exp_date_avg = [date_sub(d) for d in date_findall(exp_date_row)]
            for i in range(exp_no):
                date_tuple_list.append( date_tuple(Start_date=exp_date_avg[0], End_date=exp_date) )
        else: 
            for i in range(exp_no): date_tuple_list.append( date_tuple(Start_date='만기평가일 신규 유형', End_date='만기평가일 신규 유형') )
    except:
        for i in range(exp_no): date_tuple_list.append( date_tuple(Start_date='만기평가일 Parsing Error', End_date='만기평가일 Parsing Error') ) 

    return pd.DataFrame([list(t) for t in date_tuple_list], columns=['Start_date', 'End_date'])
###################################### [Get_range] ####################################################################################################################
def Get_range(df):
    def barrier_touch(index):
        """ Barrier 인 경우, No Touch/Touch 판단 """
        try:
            if re.search('적이(한번도)?없는', index) != None: return '(N)' ## No Touch 조건
            elif re.search('적이(한번도)?있는', index) != None: return '(T)' ## Touch 조건       
            else: return 'Touch 신규 유형' #### 오류
        except: return 'Touch parsing Error'
    def range_parsing(index): return re.sub('[^\d.%]', '', re.search('[\d\.\[\]\(\)]+%', index).group())
    def get_inequality(index, condition=None, reverse=False):
        """
        - Upper R/B, Lower R/B 각각 판단 후, 유형에 따라 Return
        [Parameters]
        * index : 조기상환 조건 or parsing 된 상환조건 값(이 경우, reverse=True)
        * conditon : Barrier 또는 Range condition 값, reverse=True 인 경우 값 필요없음
        * reverse : 만기상환 Range 없는 경우(reverse=True), 이전 Range 반대로 부호 변경 후 Return
        [Process]
        I1. x% ~ y% 사이에 있는 경우
        I2. x% 이상 or 초과
        I3. x% 이하 or 미만 """
        try:
            ineq_up_re = re.compile('크거나같|크(게|고)')      
            ineq_down_re = re.compile('작(게|고)|작거나같')      
            ineq_up = {'크거나같' : '>=', '크게' : '>', '크고' : '>'}
            ineq_down = {'작게' : '>', '작고' : '>','작거나같' : '>='}
            def ineq_search(index, regex): return regex.search(index)
 
            if reverse == False:
                if ineq_search(index, ineq_up_re) != None and ineq_search(index, ineq_down_re) != None and index.count('%') == 2: # @ I1 @
                    condition = [range_parsing(i) for i in condition]
                    condition = sorted(condition, key=lambda x:int(x[:x.find('%')]))
                    return '%s %s x %s %s' % ( condition[-1], ineq_down.get(ineq_search(index, ineq_down_re).group()), ineq_up.get(ineq_search(index, ineq_up_re).group()), condition[0] )
                elif ineq_search(index, ineq_up_re) != None: # @ I1 @
                    #print(index)
                    ineq = ineq_up.get(ineq_search(index, ineq_up_re).group())
                    return 'x %s %s' % (ineq, condition)
                elif ineq_search(index, ineq_down_re) != None: # @ I2 @
                    ineq = ineq_down.get(ineq_search(index, ineq_down_re).group())
                    return '%s %s x' % (condition, ineq)
                else: return 'Inequality 신규 유형'
            elif reverse == True: 
                ineq_reverse = {'>=':'>', '>':'>='}
                if parse('x {} {}', index) != None:
                    reverse_parse = parse('x {} {}', index)
                    return '%s %s x' % ( reverse_parse[1], ineq_reverse.get(reverse_parse[0]) )
                elif parse('{} {} x {} {}', index) != None:
                    reverse_parse = parse('{} {} x {} {}', index)
                    return '%s %s x' % ( reverse_parse[3], ineq_reverse.get(reverse_parse[2]) )
                else: return index
        except: return 'Inequality parsing Error'
    def get_strike_level(index):
        """
        [행사가격 중복 제거]
        삼성증권의 경우, 기초자산별로 행사가격 표시되어 있으므로 중복제거 하여 하나의 행사가격으로 표기
        행사가격이 다를 경우, 확인 메세지 """
        strike = list(set(re.findall('[\d\.]+%', index)))
        if len(strike) == 1: return ''.join(strike)
        else: return 'Strike level 확인'
    def get_redem_no(index):
        """ [조기상환 차수] 상환 조건에서 조기상환 차수 parsing """
        return re.sub('[^\d\.]', '', re.search('\d+차중간기준가격결정일행사|\d+차중간기준가격결정일불?포함이전', index).group())
    def range_tuple(Range='', Barrier='', Custom=''):
        """ Range/Barrier 파싱 후 namedtuple로 저장 """
        range_tuple = namedtuple('range_tuple', 'Range, Barrier, Custom')
        return range_tuple(Range, Barrier, Custom)
    """ 
    [조기상환 행사가격/조건]
    redem_strike: Dataframe에서 행사가격 가져온 후, 차수별로 나눔
    redem_range: Dataframe에서 조건 가져온 후, 차수별로 나눔
    """
    redem_strike_row = Df_find_val_iloc(df_all, '중간기준가격\s*결정일\s*행사가격') ## Dataframe 행사가격 값
    redem_strike = list(filter(None, re.split('\d+차', redem_strike_row))) ## 행사가격을 차수별로 나눔 | 1차 80% 80% 80%, 2차 80% 80% 80% ...

    redem_range_row = Df_find_val_iloc(df_all, '조기상환조건') ## Dataframe 조기상환조건 값
    redem_range = [i for i in re.sub('[(\xa0)\s]', '', redem_range_row).split('로합니다') if '차중간기준' in i] ## ## 행사가격을 차수별로 구분 | 1차 ~~, 2차 ~~ ...
    """
    [조기상환 Range, Barrier parsing]
    * no: 조기상환 차수
    * no_list: 조기상환 차수 리스트
    * range_tuple_list: namedtuple range 리스트로 저장
    """
    range_tuple_list, redem_no_list = [], []
    for i in redem_range:
        no = get_redem_no(i)
        if len(redem_no_list) == 0 or no != redem_no_list[-1]: ## R1 ## 이전 차수랑 현재 차수 비교
            """ [Barrier 없는 경우]
            - 조기상환 차수가 첫번째이거나, 이전 조기상환 차수(no_list[-1])와 다른 경우, Range 값 range_tuple_list 에 저장
            * range_cond: Range 행사가격
            * range_parse: Range, 부등호 """
            try: 
                range_cond = get_strike_level(redem_strike[int(no)-1])
                range_parse = get_inequality(i, range_cond) 
                range_tuple_list.append( range_tuple(Range=range_parse) )
            except: range_tuple_list.append( range_tuple(Range='Parsing Error') ) 
        else: ## R2 ##
            """ [Barrier 있는 경우 - 리자드]
            이전 조기상환 차수(no_list[-1])와 현재 조기상환 차수(no)가 같은 경우 -> barrier가 있음
            * barrier_strike: Dataframe에서 가져온 리자드 행사가격
            * barrier_cond: 베리어 행사가격
            * barrier_parse: barrier, 부등호, 터치 유무
            1. Barrier parsing
            (1) Dataframe에서 barrier 행사가격 가져온 후, list인지 체크 (barrier 행사가격이 2개 이상이면 Df_find_val_iloc 가 list로 return 됨) 
            (2) barrier가 1개인 경우, range_tuple_list에 barrier 추가
            (3) barrier가 n개인 경우, barrier_cond 리스트에서 현재 조기상환 차수(no)-1 번째 값 barrier 추가
            2. range_tuple_list 에서 마지막 Range 값 가져온 후, 반대 부호로 변경하여 range_tuple_lsit에 추가 """ 
            try:
                barrier_strike = Df_find_val_iloc(df_all, '리자드')
                if type(barrier_strike) != list: barrier_cond = get_strike_level(barrier_strike)
                else:
                    barrier_no = re.sub('[^\d+]', '', re.search('\d+차리자드행사가격', i).group())
                    barrier_cond = [get_strike_level(k) for k in barrier_strike][int(barrier_no)-1]
                barrier_parse = '{} {}'.format(get_inequality(i, barrier_cond), barrier_touch(i))

                if re.search('발생하지않고', i) != None:
                    prev_range = range_tuple_list[-1][0] if range_tuple_list[-1][0].count('%') == 1 else range_tuple_list[-1][0][range_tuple_list[-1][0].find('x'):] ## if 이전차수 Range가 1개인 경우, else 2개인 경우
                    range_cond = get_inequality(prev_range, reverse=True) 
                else: range_cond = ' Range 신규 유형' 
                
                range_tuple_list.append( range_tuple(Range = range_cond, Barrier = barrier_parse) )
            except: range_tuple_list.append( range_tuple(Barrier='Parsing Error') ) 
            
        redem_no_list.append(no)    

    """  [만기상환 행사가격/조건]
    * exp_strike: Dataframe에서 행사가격 가져옴
    * exp_range: Dataframe에서 조건 가져온 후, 차수별로 나눔
    - 문장에 '만기상환' 단어 포함, '지급합니다' 로 split 한 후 '발행회사' 이전까지의 문자열을 list로
    * no_list: 조기상환차수 리스트 + (조기상환 마지막 차수 + 1) x 만기상환 조건 갯수  """
    exp_strike = Df_find_val_iloc(df, '만기\s*행사가격') ## Dataframe 행사가격 값

    exp_range_row = Df_find_val_iloc(df, '만기상환금액$') ## Dataframe 만기상환조건 값
    exp_range = [i[:i.find('발행회사')] for i in re.sub('[(\xa0)\s]', '', exp_range_row).split('지급합니다') if '만기상환' in i] ## ## 행사가격을 차수별로 구분 

    No_list = redem_no_list + [str(int(redem_no_list[-1])+1)] * len(exp_range)

    """ [만기상환 Range, Barrier parsing]
    * range_tuple_list: namedtuple range 리스트로 저장 """
    for i in exp_range:
        if '만기행사가격' in i:
            """ [Barrier 없는 경우]
            - 조기상환 차수가 첫번째이거나, 이전 조기상환 차수(no_list[-1])와 다른 경우, Range 값 range_tuple_list 에 저장
            * range_cond: Range 행사가격
            * range_parse: Range, 부등호
            1. 행사가격이 2개 이상인 경우, exp_list = list type
            1-1. 각각의 행사가격을 행사조건에 대입하여 숫자%로 치환 후 get_inequality -> range_tuple 추가
            2. 행사가격이 1개인 경우, get_inequality -> range_tuple 추가 """
            try:
                if type(exp_strike) == list:
                    exp_strike = [get_strike_level(x) for x in exp_strike]
                    for n, r in enumerate(exp_strike, 1):i = i.replace(f'{n}차만기행사가격', r)
                    r_list = re.findall('[\d\.]+%[가-힣]{,7}', i)
                    if len(r_list) == 1: r_list = range_parsing(r_list[0])
                    range_parse = get_inequality(i, r_list)
                else: 
                    range_cond = get_strike_level(exp_strike)
                    range_parse = get_inequality(i, range_cond)

                range_tuple_list.append( range_tuple(Range=range_parse) )
            except: range_tuple_list.append( range_tuple(Range='Parsing Error') ) 
        else:
            """ [Barrier 있는 경우 - 하락한계]
            * prev_range: 이전 차수 조기상환 Range 및 부호
            * range_cond: 이전 차수 부호 뒤집음
            * barrier_strike: Dataframe에서 배리어 찾기
            * barrier_cond: 베리어 행사가격
            * barrier_parse: barrier strike, 부등호, 터치 유무 
            range_tuple_list에 range_cond, barrier_parse 추가 """ 
            prev_range = range_tuple_list[-1][0] if range_tuple_list[-1][0].count('%') == 1 else range_tuple_list[-1][0][range_tuple_list[-1][0].find('x'):] ## if 이전차수 Range가 1개인 경우, else 2개인 경우
            range_cond = get_inequality(prev_range, reverse=True) 
            try:
                if '하락한계' in i:
                    barrier_strike = Df_find_val_iloc(df_all, '하락한계')
                    barrier_cond = get_strike_level(barrier_strike)
                    barrier_parse = '{} {}'.format(get_inequality(i, barrier_cond), barrier_touch(i))

                    range_tuple_list.append( range_tuple(Range = range_cond, Barrier = barrier_parse) )
                else: range_tuple_list.append( range_tuple(Range = range_cond) )   
            except: range_tuple_list.append( range_tuple(Barrier='Parsing Error') )

    df_expiry = pd.DataFrame([list(t) for t in range_tuple_list], columns=['Range', 'Barrier (N/T)', 'Custom'])
    df_expiry.insert(0, 'No', No_list)

    return df_expiry


##################################################################################################################################################################################################################
##################################################################################################################################################################################################################

rcept_no = '20220217000441' # NH

url = "http://dart.fss.or.kr/dsaf001/main.do?rcpNo="+rcept_no
find_dcmno_re = re.compile('dcmNo\'\]\s?\=\s?\"(.*?)\"')
dcmno = find_dcmno_re.findall(urlopen(url).read().decode('utf-8'))[0]
url_to_crawl = f"http://dart.fss.or.kr/report/viewer.do?rcpNo={rcept_no}&dcmNo={dcmno}&eleId=1&offset=0&length=0&dtd=dart3.xsd"
print(url_to_crawl)

soup = BeautifulSoup(urlopen(url_to_crawl).read(),'html.parser')

try: sheet = Cut_Sheet(soup)[-1] # 반복 / file_to_save0
except IndexError as e: print(f'전체 Cut_Sheet list number 오류 : {e}')
except Exception as e: print(f'전체 Cut_Sheet 오류 : {e}')

try: date_sheet = Cut_Sheet(sheet, '상환금액의\s?지급에\s?관한\s?사항$','기타사항$')[0]
except Exception as e: print(f'상환 내용 Cut_Sheet 오류 : {e}')

date_sheet_text = re.sub('[\xa0\n\s]', '',date_sheet.get_text())

#if '조기상환조건' in date_sheet_text:
"""
[Dataframe] 조기상환조건, 조기상환일, 만기상환조건 테이블
"""
df_range = pd.read_html(str(date_sheet), match='최초기준가격', header=0)[0] ## 자동조기상환 발생조건  
df_expiry = pd.read_html(str(date_sheet), match='최종기준가격', header=0)[0] ## 만기상환
df_all = pd.concat([df_range, df_expiry])
""" 
[조기상환/만기상환 행사가격]
"""
redem_range_row = Df_find_val_iloc(df_all, '중간기준가격\s*결정일\s*행사가격')
redem_range = list(filter(None, re.split('\d+차', redem_range_row)))
exp_range = Df_find_val_iloc(df_all, '만기행사가격')

Range_Barrier_df = Get_range(df_all)
"""
[Date_df] 최초기준일, 조기상환평가일, 만기평가일        
1. 최초기준일, 만기평가일 parsing - base_date / redem_date / exp_date 
2. Get_date에 만기평가일 넣어서 조기상환평가일, 만기평가일 parsing 후 dataframe 으로 return
"""
base_date_row = Df_find_val_iloc(df_all, '최초기준가격\s*결정일\s*\(예정\)').replace(' ', '')
base_date = re.sub('[^\d]', '-', re.search('\d+[가-힣]\d+[가-힣]\d+', base_date_row).group())

exp_date_row = Df_find_val_iloc(pd.read_html(str(sheet.select('table')), match='청약', header=0)[0], '만\s*기\s*일').replace(' ', '')
exp_date = re.sub('[^\d]', '-', re.search('\d+[가-힣]\d+[가-힣]\d+', exp_date_row).group())

Date_df = Get_date(df_all, list(Range_Barrier_df['No']), exp_date)

""" 차수, 심볼, 페이오프 """








