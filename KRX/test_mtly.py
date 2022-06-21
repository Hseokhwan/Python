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
from collections import namedtuple
from parse import *

###################################### [Cut_Sheet] ####################################################################################################################
def Cut_Sheet(soup,start_text='모집\s?또는\s?매출의\s?개요$',end_text='중요사항$'):
    file_to_save=[]
    for tag_s,tag_e in zip(soup.find_all(string=re.compile(start_text)), soup.find_all(string=re.compile(end_text))):
        limits = len(tag_s.parent.find_all_next(['p','table'], recursive=False)) - len(tag_e.parent.find_all_next(['p','table'], recursive=False))
        before_file = tag_s.parent.find_all_next(['p','table'], recursive=False, limit=limits)
        before_file.insert(0, tag_s.find_parent('p'))
        file_to_save.append(BeautifulSoup(''.join(map(str, before_file)), 'html.parser'))
    return file_to_save
###################################### [Get_observe_under_coupon] ####################################################################################################################
def Get_observe_under_coupon(sheet_text):
    """ 쿠폰 지급 관찰 기초자산 """
    observe_pattern_1 = re.compile('(모든|각)?기초자산의?(가격|종가)')
    observe_pattern_2 = re.compile('기초자산의?(종가가|가격이)모두')
    observe_pattern_3 = re.compile('(지급조건|상관)없이.+(쿠폰|수익)지급')   ## 트루 14906회 / rcpNo=20220315000866 , NH 21523회 / rcpNo=20220121000481

    if observe_pattern_1.search(sheet_text) != None: return 'ALL'
    elif observe_pattern_2.search(sheet_text) != None: return 'ALL'
    elif observe_pattern_3.search(sheet_text) != None: return 'ALL'
    #elif observe_pattern_4.search(sheet_text) != None: return 'ALL'
    else: return '관찰 기초자산 신규 유형'
###################################### [Get_date] ####################################################################################################################    
def Get_coupon_date(coupon_sheet):
    """
    [Parameters]
    * coupon_sheet : 월수익 지급 부분만 Cut_Sheet
    [Variable]
    * date_pattern_ : 평가일 패턴 2개
    * coupon_date_list : Start_date 순으로 오름차순 정렬
    [Process]
    1. 평가일 패턴으로 구분하여 진행 (1번 패턴 오류시 2번으로) 프로세스는 같음
    2. 평가일 Dataframe에서 날짜 컬럼 찾기 (월수익 지급의 경우 평가일 table 양식이 정형화되어 있지 않음) 
    3. 정규표현식으로 평가일 찾은 후, 각 차수에서 평가일이 1개인지 2개인지 파악 후, 리스트에 저장
    """
    def date_sub(index): return re.sub('[^\d]', '-', index)
    def date_tuple(Start_date='', End_date=''):
        """ 상환평가일 파싱 후 namedtuple로 저장 """
        d_tuple = namedtuple('d_tuple', 'Start_date, End_date')
        return d_tuple(Start_date, End_date)

    date_pattern_1 = re.compile('\d[\d\s]+[가-힣]+[\d\s]+[가-힣]+[\d\s]+')  ## YYYY년MM월DD일
    date_pattern_2 = re.compile('\d[\d\s]+-[\d\s]+-[\d\s]+') ## YYYY-MM-DD
    
    raw_date= []
    coupon_tuple_list = []
    try: ## date_pattern_1 
        date_df = pd.read_html(str(coupon_sheet), match='[\d\s]+[가-힣][\d\s]+[가-힣][\d\s]+[가-힣]', header=None)[0].astype(str)
        for i in list(date_df.columns): ## 날짜 컬럼 찾기
            raw_date.extend(list(date_df[i][date_df[i].str.contains(date_pattern_1)])) ## 찾은 날짜 컬럼에서 날짜인 것만 리스트 추가
        for i in raw_date: 
            date_findall = date_pattern_1.findall(i)
            ## 종평 찾기 ##
            if len(date_findall) == 1:
                d = date_sub(date_findall[0].replace(' ', ''))
                coupon_tuple_list.append( date_tuple(Start_date=date_sub(d), End_date=date_sub(d)) )
            elif len(date_findall) >= 2:
                d_list = [d.replace(' ', '') for d in date_findall]
                coupon_tuple_list.append( date_tuple(Start_date=date_sub(d_list[0]), End_date=date_sub(d_list[-1])) )
            else: coupon_tuple_list.append( date_tuple(Start_date='Date 신규 유형', End_date='Date 신규 유형') )
    except: ## date_pattern_2 
        date_df = pd.read_html(str(coupon_sheet), match=date_pattern_2, header=None)[0].astype(str)
        for i in list(date_df.columns): ## 날짜 컬럼 찾기
            raw_date.extend(list(date_df[i][date_df[i].str.contains(date_pattern_2)])) ## 찾은 날짜 컬럼에서 날짜인 것만 리스트 추가
        for i in raw_date:
            date_findall = date_pattern_2.findall(i)
            ## 종평 찾기 ##
            if len(date_findall) == 1:
                d = date_findall[0].replace(' ', '')
                coupon_tuple_list.append( date_tuple(Start_date=date_sub(d), End_date=date_sub(d)) )
            elif len(date_findall) >= 2:
                d_list = [d.replace(' ', '') for d in date_findall]
                coupon_tuple_list.append( date_tuple(Start_date=d_list[0], End_date=d_list[-1]) )
            else: coupon_tuple_list.append( date_tuple(Start_date='Date 신규 유형', End_date='Date 신규 유형') )

    coupon_date_list = sorted([list(t) for t in coupon_tuple_list], key=lambda x:x[0] )

    return pd.DataFrame(coupon_date_list, columns=['Start_date', 'End_date'])
###################################### [Get_coupon_range] ####################################################################################################################    
def Get_coupon_range(sheet_text):
    """
    [Parameters]
    * sheet_text : 월수익 지급 부분만 Cut_Sheet
    [Variable]
    * date_pattern_ : 평가일 패턴 2개 (신규 유형일 경우, 추가)
    [Process]
    쿠폰 지급 sheet에서 쿠폰 지급 조건 parsing 후, return
    """
    def get_inequality(index, condition):
        """
        [Parameters]
        * index : 쿠폰 지급 조건
        * conditon :  Range condition 값
        [Process]
        - Upper R/B, Lower R/B 각각 판단 후, 유형에 따라 Return
        I1. x% 이상 or 초과
        I2. x% 이하 or 미만 """
        try:
            ineq_up_re = re.compile('이상|초과|크거나같(은|고)')      
            ineq_down_re = re.compile('이하|미만|작(은|고)')      
            ineq_up = {'이상' : '>=', '초과' : '>', '크거나같은' : '>=', '크거나같고' : '>='}
            ineq_down = {'이하' : '>=', '미만' : '>', '작은' : '>', '작고' : '>'}
            def ineq_search(index, regex): return regex.search(index)
 
            if ineq_search(index, ineq_up_re) != None: # @ I1 @
                ineq = ineq_up.get(ineq_search(index, ineq_up_re).group())
                return 'x %s %s' % (ineq, condition)
            elif ineq_search(index, ineq_down_re) != None: # @ I2 @
                ineq = ineq_down.get(ineq_search(index, ineq_down_re).group())
                return '%s %s x' % (condition, ineq)
            else: return 'Inequality 신규 유형'
        except: return 'Inequality parsing Error'

    range_pattern_1 = re.compile('최초기준.+[\d\.\W]+%.+경우')
    range_pattern_2 = re.compile('별도의지급조건없이.+쿠폰지급')  ## 트루 14906회 / rcpNo=20220315000866
    range_pattern_3 = re.compile('기초자산의종가와상관없이')  ## NH 21523회 / rcpNo=20220121000481  [1]
    try:
        if range_pattern_1.search(sheet_text) != None:
            r = range_pattern_1.search(sheet_text).group()
            range_cond = re.sub('[\[\]\(\)]', '', re.search('[\d\.\W]+%', r).group())
            coupon_range = get_inequality(r, range_cond)
        elif range_pattern_2.search(sheet_text) != None or range_pattern_3.search(sheet_text) != None:
            coupon_range = 'x >= 0%'
        else: coupon_range = 'Coupon range 신규 유형'
    except: coupon_range = 'Coupon range Parsing Error'

    return coupon_range
###################################### [Get_coupon_payoff] ####################################################################################################################    
def Get_coupon_payoff(sheet_text):
    """
    [Parameters]
    * sheet_text : 월수익 지급 부분만 Cut_Sheet
    [Variable]
    * payoff_pattern_ : 쿠폰 수익 패턴 5개 (신규 유형일 경우, 추가)
    [Process]
    쿠폰 지급 sheet에서 쿠폰 수익 parsing 후, return
    """
    def payoff_parsing(index): return '{:g}'.format( float(re.sub('[^\d\.]', '', re.search('[\d\.\W]+%', index).group())) / 100 )

    payoff_pattern_1 = re.compile('액면(금|가|잔)액.?[\d\.]+%')
    payoff_pattern_2 = re.compile('원금.?[\d\.]+%')
    payoff_pattern_3 = re.compile('(쿠폰|월수익)지급일마다[\d\.]+%')
    payoff_pattern_4 = re.compile('월수익지급(\(세전\))?.?[\d\.]+%')
    payoff_pattern_5 = re.compile('월단위수익인?[\d\.]+%')
    try:
        if payoff_pattern_1.search(sheet_text) != None:
            pay = payoff_pattern_1.search(sheet_text).group()
            return payoff_parsing(pay)
        elif payoff_pattern_2.search(sheet_text) != None:
            pay = payoff_pattern_2.search(sheet_text).group()
            return payoff_parsing(pay)
        elif payoff_pattern_3.search(sheet_text) != None:
            pay = payoff_pattern_3.search(sheet_text).group()
            return payoff_parsing(pay)
        elif payoff_pattern_4.search(sheet_text) != None:
            pay = payoff_pattern_4.search(sheet_text).group()
            return payoff_parsing(pay)
        elif payoff_pattern_5.search(sheet_text) != None:
            pay = payoff_pattern_5.search(sheet_text).group()
            return payoff_parsing(pay)            
        else: return 'Coupon Payoff 신규 유형'

    except: return 'Coupon Payoff Parsing Error'
################################################################################################################################################################################################################
##################################################################################################################################################################################################################

rcept_no = '20211222000129'

url = "http://dart.fss.or.kr/dsaf001/main.do?rcpNo="+rcept_no
find_dcmno_re = re.compile('dcmNo\'\]\s?\=\s?\"(.*?)\"')
dcmno = find_dcmno_re.findall(urlopen(url).read().decode('utf-8'))[0]
url_to_crawl = f"http://dart.fss.or.kr/report/viewer.do?rcpNo={rcept_no}&dcmNo={dcmno}&eleId=1&offset=0&length=0&dtd=dart3.xsd"
print(url_to_crawl)

soup = BeautifulSoup(urlopen(url_to_crawl).read(),'html.parser')

try: sheet = Cut_Sheet(soup)[2] # 반복 / file_to_save0
except:
    try: sheet = Cut_Sheet(soup, end_text='통지방법\s?및\s?절차$')[0] ### 키움증권 '기타 중요사항' 없음
    except IndexError as e: print(f'전체 Cut_Sheet list number 오류 : {e}')
    except Exception as e: print(f'전체 Cut_Sheet 오류 : {e}')

try: date_sheet = Cut_Sheet(sheet, '상환금액의\s?지급에\s?관한\s?사항$','기타사항$|중요사항$|통지방법\s?및\s?절차$')[0]
except Exception as e: print(f'상환 내용 Cut_Sheet 오류 : {e}')
date_sheet_text = re.sub('[\xa0\n\s]', '',date_sheet.get_text())

#if re.search('월\s*수익', str(date_sheet)) or re.search('쿠폰\s*지급', str(date_sheet)) != None:
""" [먼슬리 Sheet 구분] coupon_sheet """
for x in [re.compile('월\s*수익\s*지급'), re.compile('쿠폰\s*지급')]:
    start_text = date_sheet.find(text=x)
    if start_text != None: break    
end = '가나다라'['가나다라'.find(start_text.strip()[0]) + 1]
end_text = date_sheet.find(text=re.compile(f'{end}\.?\s*[가-힣]+상환'))

coupon_sheet = Cut_Sheet(date_sheet, start_text, end_text)[0]
coupon_sheet_text = re.sub('[\xa0\n\s]', '', coupon_sheet.get_text())

""" [지급일] coupon_lag 
쿠폰 상환 lag 다른 경우, 순차적으로 정리 """
lag_findall = re.findall('[\d\W]+영업일', coupon_sheet_text)
if len(lag_findall) == 1: coupon_lag = re.sub('[^\d]', '', lag_findall[0])
elif len(lag_findall) >= 2: coupon_lag = ','.join([re.sub('[^\d]', '', x) for x in lag_findall])  ## 쿠폰 Lag 2개: 한국투자, 미래에셋

""" [쿠폰 지급 관찰 기초자산] """
Observe_under = Get_observe_under_coupon(coupon_sheet_text)

""" [쿠폰 평가일] """
Coupon_date_df = Get_coupon_date(coupon_sheet)

""" [쿠폰 지급 조건 Range] """
Coupon_range = Get_coupon_range(coupon_sheet_text)

""" [쿠폰 수익] """
Coupon_payoff = Get_coupon_payoff(coupon_sheet_text)

""" Symbol, Range, Payoff 리스트 생성 """
coupon_date_num = len(list(Coupon_date_df.iloc[:,0]))
Observe_under_coupon = [Observe_under] * coupon_date_num 
Coupon_range_list = [Coupon_range] * coupon_date_num
Coupon_payoff_list = [Coupon_payoff] * coupon_date_num

""" [먼슬리 Dataframe] 차수, 심볼, 쿠폰평가일, 상환조건, 수익률  """
Coupon_date_df.insert(0, 'No', list(range(1, coupon_date_num+1)) )
Coupon_date_df.insert(1, 'Symbol', Observe_under_coupon )
Coupon_date_df.insert(4, 'Range', Coupon_range_list )
Coupon_date_df.insert(5, 'Fixed', Coupon_payoff_list )

Coupon_date_df.index = list(range(1, coupon_date_num+1))
Df_mtly = Coupon_date_df

print('쿠폰 Lag : ' + coupon_lag)

Df_mtly
