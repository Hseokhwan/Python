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

# %%
"""
[함수 목록]
Cut_Sheet : 투자설명서 내에서 각 종목별로 필요한 부분만 가져와서 List로 저장
Df_find_val_iat : Dataframe 에서 특정 행의 열 값 취득
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
###################################### [Df_find_val_iat] ####################################################################################################################
def Df_find_val_iat(table, text):
    """ table의 첫번째 열에서 regex text를 찾고 전체 테이블에서 일치하는 행의 열 값을 취득 """
    return table[table.iloc[:,0].str.contains(text)].iat[0,-1].replace(' ', '')
###################################### [Get_observe_under_coupon] ####################################################################################################################
def Get_observe_under_coupon(df):
    """ 쿠폰 지급 관찰 기초자산 """
    coupon_row = Df_find_val_iat(df, '월수익\s*금액\s*및\s*지급\s*조건')
    observe_pattern_1 = re.compile('각기초자산의해당월수익중간기준가격이모두')

    if observe_pattern_1.search(coupon_row) != None: return 'ALL'
    else: return '관찰 기초자산 신규 유형'
###################################### [Get_date] ####################################################################################################################
def Get_coupon_date(df, exp_date):
    """
    [Parameters]
    * df : 상환조건 부분 Dataframe (df_all)
    * exp_date : 발행정보 테이블에서 가져온 만기평가일 (exp_date)
    [Variable]
    * coupon_date_row : df_all 에서 쿠폰평가일 값
    * exp_date_row : df_all 에서 만기평가일 값
    * exp_date_avg : 평가일 종가 평균일때 평가일자 리스트
    [Process]
    1. 쿠폰평가일 parsing 후 date_tuple_list 에 추가
    2. 만기평가일 1개인 경우, parsing 후 date_tuple_list 에 추가 (exp_no 갯수만큼)
    3. 만기평가일 여러개인 경우, parsing 후 date_tuple_list 에 추가 (exp_no 갯수만큼)
    """
    def date_findall(index): return re.findall('\d+[가-힣]\d+[가-힣]\d+', index)
    def date_sub(index): return re.sub('[^\d]', '-', index)
    def date_tuple(Start_date='', End_date=''):
        """ 상환평가일 파싱 후 namedtuple로 저장 """
        d_tuple = namedtuple('d_tuple', 'Start_date, End_date')
        return d_tuple(Start_date, End_date)
    
    coupon_date_row = Df_find_val_iat(df, '월수익\s*중간기준가격\s*결정일\s*\(예정\)')
    exp_date_row = Df_find_val_iat(df, '최종기준가격\s*결정일\s*\(예정\)')

    coupon_tuple_list = []
    try:
        for d in date_findall(coupon_date_row): coupon_tuple_list.append( date_tuple(Start_date=date_sub(d), End_date=date_sub(d)) )
        ## 쿠폰 만기평가일 ##
        if re.search('최종기준가격\s*결정일', exp_date_row) == None and '만기일' in exp_date_row:
            coupon_tuple_list.append( date_tuple(Start_date=exp_date, End_date=exp_date) )
        elif re.search('최종기준가격\s*결정일', exp_date_row) != None:
            exp_date_avg = [date_sub(d) for d in date_findall(exp_date_row)]
            coupon_tuple_list.append( date_tuple(Start_date=exp_date_avg[0], End_date=exp_date) )
        else: coupon_tuple_list.append( date_tuple(Start_date='평가일 신규 유형', End_date='평가일 신규 유형') )
    
    except: coupon_tuple_list.append( date_tuple(Start_date='평가일 Parsing Error', End_date='평가일 Parsing Error') )

    return pd.DataFrame([list(t) for t in coupon_tuple_list], columns=['Start_date', 'End_date'])
###################################### [Get_date] ####################################################################################################################
def Get_redem_date(df, coupon_df):
    """
    [Parameters]
    * df : 상환조건 부분 Dataframe (df_all)
    * coupon_df : 쿠폰평가일 Coupon_date_df
    [Variable]
    * redem_date_row : df_all 에서 조기상환일 값
    * redem_date_re : redem_date_row 상환 차수별로 split
    * redem_date_list : 조기상환 차수에 해당하는 coupon_df 값을 찾아서 list로 저장
    [Process]
    조기상환 차수에 해당하는 coupon_df의 날짜를 찾아서 redem_date_list에 넣고 Dataframe return
    """
    def date_tuple(Start_date='', End_date=''):
        """ 상환평가일 파싱 후 namedtuple로 저장 """
        d_tuple = namedtuple('d_tuple', 'Start_date, End_date')
        return d_tuple(Start_date, End_date)
    
    redem_date_row = Df_find_val_iat(df, '\d+\)\s*중간기준가격\s*결정일\(예정\)')
    redem_date_re = re.findall(':\d+[가-힣]+', redem_date_row)
    
    redem_date_list = []
    for i in redem_date_re:
        n = re.sub('[^\d]', '', i)
        redem_date_list.append(list(coupon_df.iloc[int(n)-1]))
    redem_date_list.append(list(coupon_df.iloc[-1]))

    return pd.DataFrame(redem_date_list, columns=['Start_date', 'End_date'])
###################################### [Get_range] ####################################################################################################################
def Get_range(df, len_coupon_date):
    """
    [Parameters]
    * df : 상환조건 부분 Dataframe (df_all)
    * len_coupon_date : 쿠폰평가일 갯수
    [Variable]
    * _strike_ : df_all 에서 행사가격 값
    * _range_ : df_all 에서 Range 조건 값
    [Process]
    1. 조기/만기상환 행사가격 parsing -> 상환 Range/Barrier parsing -> 조기/만기상환 조건 Dataframe 생성 후 return
    2. 쿠폰 행사가격 parsing -> 쿠폰 Range parsing -> 쿠폰 지급 조건 Dataframe 생성 후 return
    """
    def barrier_touch(index):
        """ Barrier의 No Touch/Touch 판단 """
        try:
            if re.search('적이(한번도)?없는', index) != None: return '(N)' ## No Touch 조건
            elif re.search('적이(한번도)?있는', index) != None: return '(T)' ## Touch 조건       
            else: return 'Touch 신규 유형' #### 오류
        except: return 'Touch parsing Error'
    def get_strike_level(index):
        """
        [행사가격 중복 제거]
        삼성증권의 경우, 기초자산별로 행사가격 표시되어 있으므로 중복제거 하여 하나의 행사가격으로 표기
        행사가격이 다를 경우, 확인 메세지 """
        strike = list(set(re.findall('[\d\.]+%', index)))
        if len(strike) == 1: return ''.join(strike)
        else: return 'Strike level 확인'
    def range_tuple(Range='', Barrier='', Custom=''):
        """ Range/Barrier 파싱 후 namedtuple로 저장 """
        range_tuple = namedtuple('range_tuple', 'Range, Barrier, Custom')
        return range_tuple(Range, Barrier, Custom)
    def get_inequality(index, condition=None, reverse=False):
        """
        [Parameters]
        * index : 조기상환 조건 or parsing 된 상환조건 값(이 경우, reverse=True)
        * conditon : Barrier 또는 Range condition 값, reverse=True 인 경우 값 필요없음
        * reverse : 만기상환 Range 없는 경우(reverse=True), 이전 Range 반대로 부호 변경 후 Return
        [Process]
        - Upper R/B, Lower R/B 각각 판단 후, 유형에 따라 Return
        I1. x% 이상 or 초과
        I2. x% 이하 or 미만 """
        try:
            ineq_up_re = re.compile('크거나같|크(게|고)')      
            ineq_down_re = re.compile('작(게|고)|작거나같')      
            ineq_up = {'크거나같' : '>=', '크게' : '>', '크고' : '>'}
            ineq_down = {'작게' : '>', '작고' : '>','작거나같' : '>='}
            def ineq_search(index, regex): return regex.search(index)
 
            if reverse == False:
                if ineq_search(index, ineq_up_re) != None: # @ I1 @
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
    """ 
    [상환 행사가격/조건/상환 차수]
    redem_strike: Dataframe에서 행사가격 가져온 후, 차수별로 나눔
    redem_range: Dataframe에서 조건 가져온 후, 차수별로 나눔 """
    redem_strike_row = Df_find_val_iat(df, '중간기준가격\s*결정일\s*행사가격') ## Dataframe 행사가격 값
    redem_strike = list(filter(None, re.split('\d+차', redem_strike_row))) ## 행사가격을 차수별로 나눔 | 1차 80% 80% 80%, 2차 80% 80% 80% ...
    exp_strike = Df_find_val_iat(df, '만기행사가격') ## Dataframe 행사가격 값

    redem_range_row = Df_find_val_iat(df, '조기상환조건') ## Dataframe 조기상환조건 값    
    exp_range_row = Df_find_val_iat(df, '만기상환금액') ## Dataframe 만기상환조건 값
    exp_range = [i[:i.find('발행회사')] for i in re.sub('[(\xa0)\s]', '', exp_range_row).split('지급합니다') if '최종기준가격이' in i] ## ## 행사가격을 차수별로 구분 

    no_list = list(range(1, len(redem_strike)+1))
    no_list += [no_list[-1]+1] * len(exp_range)

    range_tuple_list = []
    """ [조기상환 Range parsing]
        * range_cond: Range 행사가격
        * range_parse: Range, 부등호"""
    try:
        for i in redem_strike:
            if re.search('중간기준가격이모두해당중간기준가격결정일행사가격보다[가-힣]+되는경우,', redem_range_row) != None:
                range_cond = get_strike_level(i)
                range_parse = get_inequality(redem_range_row, range_cond)
                range_tuple_list.append( range_tuple(Range=range_parse) )

            else: range_tuple_list.append( range_tuple(Range='Range 조건 신규 유형') )
    except: range_tuple_list.append( range_tuple(Range='Parsing Error') )

    """ [만기상환 Range, Barrier parsing]"""
    for i in exp_range:
        if '만기행사가격' in i:
            """ [Barrier 없는 경우] """
            try:
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
            prev_range = range_tuple_list[-1][0] ## if 이전차수 Range가 1개인 경우, else 2개인 경우
            range_cond = get_inequality(prev_range, reverse=True) 
            try:
                if '하락한계' in i:
                    barrier_strike = Df_find_val_iat(df_all, '하락한계')
                    barrier_cond = get_strike_level(barrier_strike)
                    barrier_parse = '{} {}'.format(get_inequality(i, barrier_cond), barrier_touch(i))
                    range_tuple_list.append( range_tuple(Range = range_cond, Barrier = barrier_parse) )

                else: range_tuple_list.append( range_tuple(Range = range_cond) )   
            except: range_tuple_list.append( range_tuple(Barrier='Parsing Error') )
    
    redem_range_df = pd.DataFrame(list(range_tuple_list), columns=['Range', 'Barrier (N/T)', 'Custom'])
    redem_range_df.insert(0, 'No', no_list)

    """ [조기상환, 만기상환 Coupon 행사가격] """
    redem_coupon_strike =  get_strike_level(Df_find_val_iat(df, '월수익\s*행사가격'))
    exp_coupon_strike =  get_strike_level(Df_find_val_iat(df, '만기행사가격'))

    """ [조기상환, 만기상환 Coupon 조건] """
    redem_coupon_range = Df_find_val_iat(df, '월수익\s*금액\s*및\s*지급\s*조건')
    exp_coupon_range = Df_find_val_iat(df, '만기\s*월수익\s*금액\s*및\s*지급\s*조건')

    coupon_range_list = []
    if re.search('중간기준가격이모두해당월수익행사가격보다[가-힣]+되는경우', redem_coupon_range) != None:
        range_parse = get_inequality(redem_coupon_range, redem_coupon_strike)
        coupon_range_list.append( range_parse )
        coupon_range_list = coupon_range_list * (len_coupon_date-1)
    else: coupon_range_list.append( 'Range 조건 신규 유형')

    if re.search('최종기준가격이모두해당월수익행사가격보다[가-힣]+되는경우', exp_coupon_range) != None:
        range_parse = get_inequality(exp_coupon_range, exp_coupon_strike)
        coupon_range_list.append( range_parse )
    else: coupon_range_list.append( 'Range 조건 신규 유형' )    

    coupon_range_df = pd.DataFrame(coupon_range_list, columns=['Range'])

    return redem_range_df, coupon_range_df
###################################### [Get_payoff] ####################################################################################################################
def Get_payoff(df, len_redem):
    """
    [Parameters]
    * df : 상환조건 부분 Dataframe (df_all)
    * len_redem : 조기상환 평가일 갯수
    [Variable]
    * _payoff_ : df_all 에서 수익지급 값
    [Process]
    1. 조기/만기상환 payoff parsing -> 조기/만기상환 조건 Dataframe 생성 후 return
    2. 조기/만기상환 symbol parsing -> symbol_list return
    """
    def payoff_tuple(Fixed='', Gear='', Bonus='', Cap='', Floor=''):
        """ Payoff 파싱 후 namedtuple로 저장 """
        pay_tuple = namedtuple('pay_tuple', 'Fixed, Gear, Bonus, Cap, Floor')
        return pay_tuple(Fixed, Gear, Bonus, Cap, Floor)

    redem_payoff = Df_find_val_iat(df, '조기상환조건')
    exp_payoff_row = Df_find_val_iat(df, '만기상환금액')
    exp_payoff = [i for i in re.sub('[(\xa0)\s]', '', exp_payoff_row).split('지급합니다') if '최종기준가격이' in i] ## ## 행사가격을 차수별로 구분 
    
    """ [조기/만기상환 payoff parsing] """
    payoff_tuple_list = []
    if re.search('상환금액은\[?액면가액\+해당월수익금액\]?으로합니다', redem_payoff) != None:
        payoff_tuple_list.append( payoff_tuple(Fixed='0') )
    else: payoff_tuple_list.append( payoff_tuple(Fixed='Payoff 신규 유형') )
    payoff_tuple_list = payoff_tuple_list * len_redem
    
    for i in exp_payoff:
        try:
            if re.search('액면가액\+해당만기월수익금액', i) != None:
                payoff_tuple_list.append( payoff_tuple(Fixed='0') )
            elif re.search('액면가액xWorst가격변동률\+1\W?\+해당만기월수익금액', i) != None:
                payoff_tuple_list.append( payoff_tuple(Gear='1') )      
            else: payoff_tuple_list.append( payoff_tuple(Fixed='Payoff 신규 유형') )      
        except: payoff_tuple_list.append( payoff_tuple(Fixed='Payoff parsing Error') )

    """ [조기/만기상환 symbol parsing] """
    symbol_list = []
    if re.search('각중간기준가격결정일에각기초자산의해당중간기준가격이모두', redem_payoff) != None:
        symbol_list.append('ALL')
    else: symbol_list.append('Symbol 확인')
    symbol_list = symbol_list * len_redem

    for i in exp_payoff:
        if re.search('각기초자산의최종기준가격이모두각각의만기행사가격보다', i) != None:
            symbol_list.append('ALL')
        elif re.search('하나의기초자산이라도최종기준가격이해당만기행사가격보다', i) != None:
            symbol_list.append('ALL')
        else: symbol_list.append('Symbol 확인')          

    payoff_df = pd.DataFrame(list(payoff_tuple_list), columns=['Fixed', 'Gear', 'Bonus', 'Cap', 'Floor'])

    return payoff_df, symbol_list
##################################################################################################################################################################################################################
##################################################################################################################################################################################################################
"""
[Samsung Mtly] 
- 필요 데이터
최초기준일
상환 평가일/지급조건/수익률
쿠폰 평가일/지급조건/수익률
상환 지급일 lag
"""
rcept_no = '20220114000323' # NH

url = "http://dart.fss.or.kr/dsaf001/main.do?rcpNo="+rcept_no
find_dcmno_re = re.compile('dcmNo\'\]\s?\=\s?\"(.*?)\"')
dcmno = find_dcmno_re.findall(urlopen(url).read().decode('utf-8'))[0]
url_to_crawl = f"http://dart.fss.or.kr/report/viewer.do?rcpNo={rcept_no}&dcmNo={dcmno}&eleId=1&offset=0&length=0&dtd=dart3.xsd"
print(url_to_crawl)

soup = BeautifulSoup(urlopen(url_to_crawl).read(),'html.parser')

try: sheet = Cut_Sheet(soup)[1] # 반복 / file_to_save0
except IndexError as e: print(f'전체 Cut_Sheet list number 오류 : {e}')
except Exception as e: print(f'전체 Cut_Sheet 오류 : {e}')

try: date_sheet = Cut_Sheet(sheet, '상환금액의\s?지급에\s?관한\s?사항$','기타사항$')[0]
except Exception as e: print(f'상환 내용 Cut_Sheet 오류 : {e}')

date_sheet_text = re.sub('[\xa0\n\s]', '',date_sheet.get_text())
#if re.search('월\s*수익', str(date_sheet)) != None:
"""
[Dataframe] 조기상환조건, 조기상환일, 만기상환조건 테이블
"""
df_range = pd.read_html(str(date_sheet), match='최초기준가격', header=0)[0] ## 자동조기상환 발생조건  
df_expiry = pd.read_html(str(date_sheet), match='최종기준가격', header=0)[0] ## 만기상환
df_all = pd.concat([df_range, df_expiry])

""" [지급일 lag] coupon_lag / redem_lag / exp_lag """
coupon_lag = re.sub('[^\d]', '', Df_find_val_iat(df_all, '월수익\s*금액\s*지급일'))
redem_lag = re.sub('[^\d]', '', Df_find_val_iat(df_all, '자동조기상환\s*금액\s*지급일'))
exp_lag = re.sub('[^\d]', '', Df_find_val_iat(df_all, '만기상환\s*금액\s*지급일'))

""" [쿠폰평가일, 상환평가일] Coupon_date_df / Redem_date_df """ 
exp_table = pd.read_html(str(date_sheet), match='최종\s*기준\s*가격')[0].astype(str)
exp_date_row = Df_find_val_iat(pd.read_html(str(sheet.select('table')), match='청약', header=0)[0], '만\s*기\s*일')
exp_date = re.sub('[^\d]', '-', re.search('\d+[가-힣]\d+[가-힣]\d+', exp_date_row).group())

Coupon_date_df = Get_coupon_date(df_all, exp_date) #  ['1', '2', '3', '4', '5', '6', '6']
Redem_date_df = Get_redem_date(df_all, Coupon_date_df)

""" [쿠폰 수익] coupon_payoff 
새로운 pay 조건 생길 시, raw_payoff 조건 추가 """
coupon_date_num = len(list(Coupon_date_df.iloc[:,0]))

payoff_row = Df_find_val_iat(df_all, '월수익\s*금액\s*및\s*지급\s*조건')
coupon_payoff = '{:g}'.format(float(re.search('[\d\.]+%', payoff_row).group().replace('%', '')) / 100)
Coupon_payoff_df = pd.DataFrame([coupon_payoff] * coupon_date_num, columns=['Fixed'])

""" [쿠폰 관찰 기초자산] """
Observe_under_coupon = pd.DataFrame([Get_observe_under_coupon(df_all)] * coupon_date_num, columns=['Symbol'])

""" [상환 Range, Barrier / 쿠폰 Range] """  
Range_Barrier_df, Coupon_range_df = Get_range(df_all, coupon_date_num)

""" [상환 Payoff, 조기상환 관찰 기초자산] """
redem_num_list = list(Range_Barrier_df['No'])
redem_no = len(redem_num_list) - redem_num_list.count(redem_num_list[-1])

Payoff_df, Observe_under_redem = Get_payoff(df_all, redem_no)

"""
[관찰기초자산, 상환일, 상환조건 Datafram 병합]
1. Redem_date_df 에서 만기상환일을 상환차수 만큼 추가함
2. Range_Barrier_df 에 관찰기초자산, 상환일을 insert """
exp_no = redem_num_list.count(redem_num_list[-1])
for x in range(1, exp_no): Redem_date_df = pd.concat([Redem_date_df, Redem_date_df.iloc[[-1]]])
Range_Barrier_df.insert(1, 'Symbol', Observe_under_redem )
Range_Barrier_df.insert(2, 'Start_date', list(Redem_date_df['Start_date']) )
Range_Barrier_df.insert(3, 'End_date', list(Redem_date_df['End_date']) )

""" [먼슬리 Dataframe] 차수, 관찰기초자산, 쿠폰평가일, 상환조건, 수익률  """
No_coupon = pd.DataFrame(list(range(1, coupon_date_num+1)), columns=['No'])
Df_mtly = pd.concat([No_coupon, Observe_under_coupon, Coupon_date_df, Coupon_range_df, Coupon_payoff_df], axis=1)
Df_mtly.index = list(range(1, coupon_date_num+1))

""" [조기상환 Df_final] """
Df_final = pd.concat([Range_Barrier_df, Payoff_df], axis=1)
Df_final.index = list(range(1, len(redem_num_list) + 1))

Df_final, Df_mtly
