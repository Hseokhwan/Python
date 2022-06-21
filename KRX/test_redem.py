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
#from Crawling_Method import *

# %%
"""
[함수 목록]
Cut_Sheet : 투자설명서 내에서 각 종목별로 필요한 부분만 가져와서 List로 저장
Get_observe_under : 상환 조건 별 기초자산 관찰 갯수 확인
Get_date : 상환 평가일 Parsing 
Get_range : 상환 조건 Parsing
Get_payoff : 상환 수익률 Parsing
"""
###################################### [Cut_Sheet] ####################################################################################################################
def Cut_Sheet(soup,start_text='모집\s?또는\s?매출의\s?개요$',end_text='중요사항$'):
    file_to_save=[]
    for tag_s,tag_e in zip(soup.find_all(string=re.compile(start_text)), soup.find_all(string=re.compile(end_text))):
        limits = len(tag_s.parent.find_all_next(['p','table'], recursive=False)) - len(tag_e.parent.find_all_next(['p','table'], recursive=False))
        before_file = tag_s.parent.find_all_next(['p','table'], recursive=False, limit=limits)
        before_file.insert(0, tag_s.find_parent('p'))
        file_to_save.append(BeautifulSoup(''.join(map(str, before_file)), 'html.parser'))
    return file_to_save

###################################### [Get_observe_under] ####################################################################################################################
def Get_observe_under(index_list):
    """ [Observed underlying parsing] ALL 아닌 것 -> ALL 순으로 parsing  ** 유형 추가 필요 """
    observe_under_list = []
    for i in [x.replace(' ', '') for x in index_list]:
        if re.search('((모든|세)기초자산중)?어느두기초자산|2개의?기초자산만?', i) != None: observe_under_list.append('TWO')
        elif re.search('모든.+기초|기초.+(각|모두|하나|한)|(하락|상승).+기초.+(없는|있는)|(하나|한).+(기초)?.+라도|기초.+최초.+경우', i) != None: observe_under_list.append('ALL')
        else: observe_under_list.append(observe_under_list[-1])
    return observe_under_list

###################################### [Get_date] ####################################################################################################################
def Get_date(index_list):
    """ [Date parsing]
    1. 평가일 갯수 체크 및 YYYY-MM-DD 형식으로 변경
    2. 평가일 갯수에 따라 Start_date, End_date 지정해서 date_tuple_list에 저장 후 Dataframe return """    
    def date_sub(index): return re.sub('[^\d]', '-', index)[:-1]
    def date_tuple(Start_date='', End_date=''):
        """ 상환평가일 파싱 후 namedtuple로 저장 """
        d_tuple = namedtuple('d_tuple', 'Start_date, End_date')
        return d_tuple(Start_date, End_date)
    def date_form(index):
        date_split = index.split('-')
        y = date_split[0]
        m = date_split[1]
        d = date_split[2]
        if len(y) == 2: y = '20' + y
        if len(m) == 1: m = '0' + m
        if len(d) == 1: d = '0' + d
        return y + '-' + m + '-' + d

    date_tuple_list = []
    for d in index_list:
        date_day = re.findall('\d+일', ''.join(filter(str.isalnum, d))) ## Day
        date_ymd = [date_sub(i) for i in re.findall('\d+[가-힣]\d+[가-힣]\d+[가-힣]', ''.join(filter(str.isalnum, d)))] ## YYYYMMDD / 공백 및 특수문자 제거, 년월일 치환
        date_ymd = [date_form(i) for i in date_ymd] ## YYYYMMDD 형식 맞추기 (ex. 21-8-8 > 2021-08-08)

        if len(date_day) > 1: ## 평가일 2개 이상인 경우 (ex. 2022년 6월 21일, 22일, 23일)
            date_tuple_list.append( date_tuple(Start_date = date_ymd[0], End_date = date_ymd[0])[:8] + date_sub(date_day[-1]) )
        else:
            if len(date_ymd) == 1: date_tuple_list.append( date_tuple(Start_date = date_ymd[0], End_date = date_ymd[0]) )
            elif len(date_ymd) > 1: date_tuple_list.append( date_tuple(Start_date = date_ymd[0], End_date = date_ymd[-1]) )  ## (ex. 2022년 6월 21일, 2022년 6월 22일, 2022년 6월 23일)
            else: date_tuple_list.append( date_tuple(Start_date = 'Date 신규 유형', End_date = 'Date 신규 유형') ) # Date parsing Error
    return pd.DataFrame([list(t) for t in date_tuple_list], columns=['Start_date', 'End_date'])

###################################### [Get_range] ####################################################################################################################
def Get_range(index_list, date_sheet_text):
    """ [Range, Barrier parsing]
    1. Barrier 체크 -> 터치 유무 -> 기호 포함 정리
    2. Range 갯수에 따라 기호 포함 정리
    * 새로운 유형인 경우, Range pattern 에 pattern 추가 및 range_tuple_list에 넣을 수 있도록 유형 추가 """
    def barrier_touch(index):
        """ Barrier 인 경우, No Touch/Touch 판단 """
        try:
            if re.search('없(는|었던)경우|없으며|적이?없고', index) != None: return '(N)' ## No Touch 조건
            elif re.search('있(는|었던)경우|있으며|적이?있고', index) != None: return '(T)' ## Touch 조건       
            else: return '터치 확인' #### 오류
        except: return 'Touch parsing Error'
    def barrier_partial(index):
        partial_re = re.compile('\d차[가-힣\(\)]+부터\d차[가-힣\(\)]+까지')
        if partial_re.search(index) != None: return 'Barrier Partial'
        else: return ''
    def range_tuple(Range='', Barrier='', Custom=''):
        """ Range/Barrier 파싱 후 namedtuple로 저장 """
        range_tuple = namedtuple('range_tuple', 'Range, Barrier, Custom')
        return range_tuple(Range, Barrier, Custom)
    def range_parsing(index): return re.sub('[^\d.%]', '', re.search('[\d\.\[\]\(\)]+%', index).group())
    def get_inequality(index, condition=None, reverse=False):
        """
        [Upper R/B, Lower R/B 각각 판단 후, 유형에 따라 Return]
        * conditon : Barrier 또는 Range condition 값, reverse=True 인 경우 값 필요없음
        * reverse : 만기상환 Range 없는 경우(reverse=True), 이전 Range 반대로 부호 변경 후 Return
        I1. x% ~ y% 사이에 있는 경우
        I2. x% 이상 or 초과
        I3. x% 이하 or 미만 """
        try:
            ineq_up_re = re.compile('이상|초과|크거나같(은|고)')      
            ineq_down_re = re.compile('이하|미만|작(은|고)')      
            ineq_up = {'이상' : '>=', '초과' : '>', '크거나같은' : '>=', '크거나같고' : '>='}
            ineq_down = {'이하' : '>=', '미만' : '>', '작은' : '>', '작고' : '>'}
            def ineq_search(index, regex): return regex.search(index)

            if reverse == False:
                if ineq_search(index, ineq_up_re) != None and ineq_search(index, ineq_down_re) != None and index.count('%') == 2: # @ I1 @
                    condition = [range_parsing(i) for i in condition]
                    condition = sorted(condition, key=lambda x:int(x[:x.find('%')]))
                    return '%s %s x %s %s' % ( condition[-1], ineq_down.get(ineq_search(index, ineq_down_re).group()), ineq_up.get(ineq_search(index, ineq_up_re).group()), condition[0] )
                else:
                    index_re = re.search('최초기준.+%.{,10}', index).group()
                    if ineq_search(index_re, ineq_up_re) != None: # @ I2 @
                        return 'x %s %s' % ( ineq_up.get(ineq_search(index_re, ineq_up_re).group()), condition )
                    elif ineq_search(index_re, ineq_down_re) != None: # @ I3 @
                        return '%s %s x' % ( condition, ineq_down.get(ineq_search(index_re, ineq_down_re).group()) )
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

    """ Range pattern """
    barrier_re_1 = re.compile('최초기준.{,10}%.{,15}적이?(한번(이라)?도)?(있|없)(는|었던)경우')
    barrier_re_2 = re.compile('((평가|관찰)일.{,6}까지|기간중).*최초기준.+%.*(있|없)(는|었던)경우')
    barrier_re_3 = re.compile('최초기준.{,10}%.{,15}기초자산이?(없|있)으며')
    barrier_re_4 = re.compile('최초기준.{,10}%.{,10}하락한적이?(있|없)고')

    range_re_2range_1 = re.compile('최초기준.{,10}%.*(이고|이며).*최초기준.{,10}%[^적(하락|상승)]+경우')
    range_re_2range_2 = re.compile('최초기준.{,10}%.{,15}%[^적(하락|상승)]+경우')
    range_re_2range_3 = re.compile('최초기준.{,10}%.{,5}%.{,5}인경우')
    
    range_re_1 = re.compile('최초기준.+%.+인경우')
    range_re_2 = re.compile('최초기준.+%[^적]+(있|없)는경우')
    range_re_3 = re.compile('최초기준.+%.*보다[^적(하락|상승)]{,10}경우')
    range_re_4 = re.compile('최초기준.+%.*(이상|이하)경우')

    """ 특이한 경우 """
    range_re_true = re.compile('해당하지않고.+만기상환기준비율.+최대손실률') ## 트루 13361회 만기상환조건 (rcpNo=20200918000310)

    range_tuple_list = []
    """ 함수 인자값으로 받은 index_list 파싱"""
    for r in [x.replace(' ', '') for x in index_list]:
        """ Barrier 터치여부 체크 """
        #print(r)
        barrier_re = barrier_re_1.search(r) or barrier_re_2.search(r) or barrier_re_3.search(r) or barrier_re_4.search(r)
        range_re_2range = range_re_2range_1.search(r) or range_re_2range_2.search(r) or range_re_2range_3.search(r)
        range_re = range_re_1.search(r) or range_re_2.search(r) or range_re_3.search(r) or range_re_4.search(r)
        """
        [Barrier 있는 경우]
        1. Barrier Condition 파싱 - barrier_cond
        2. Barrier 터치 유무, 부등호 파싱 - barrier_parse
        3. 배리어 관찰기간 partial 체크
        4. Range 파싱
        B1. Barrier 제외하고 Range 2개인 경우 (ex. 80%이상 100%미만)
        B2. Range 없음, Barrier 있음: (ex. ~~해당하지 않고 Barrier 경우, ~~충족하지 못하고 Barrier 경우 등등) 
        B3. Range 있음, Barrier 있음 1: (ex. 최초 ~ 미만이고 Barrier 경우 or 최초 ~ 미만인 경우, Barrier 경우)
        B4. Range 있음, Barrier 있음 2: (ex. 최초 ~ 하락한적 있고 ~ 최초 ~ 이상인 경우)
        """
        if barrier_re != None:
            try:
                b_r_re_1 = re.compile('(해당|상환|만족|충족|발생)(하지|되지)(않|못하였?)고') ## Range 표기가 없는 경우 ##
                b_r_re_2 = re.compile('최초.+%.*(이(고|며)|경우,).*최초.+%.+적.*(있|없)는경우') ## Range 표기가 있는 경우 ## 1
                b_r_re_3 = re.compile('최초.+%.+적이?있고.*최초.+%.+인경우') ## Range 표기가 있는 경우 ## 2

                barrier_cond = range_parsing(barrier_re.group())
                barrier_parse = '{} {}'.format(get_inequality(barrier_re.group(), barrier_cond), barrier_touch(barrier_re.group()))
                partial_ = barrier_partial(r)
                if range_re_2range != None: # @ B1 @  ## Range 2개 ## 
                    r_list = re.findall('[\d\.]+%[가-힣]{,5}', range_re_2range.group())
                    range_parse = get_inequality(range_re_2range.group(), r_list)
                elif b_r_re_1.search(r) != None: # @ B2 @ 
                    prev_range = range_tuple_list[-1][0] if range_tuple_list[-1][0].count('%') == 1 else range_tuple_list[-1][0][range_tuple_list[-1][0].find('x'):]   ## if 이전차수 Range가 1개인 경우, else 2개인 경우
                    range_parse = get_inequality(prev_range, reverse=True)
                elif b_r_re_2.search(r) != None: # @ B3 @
                    range_cond = re.search('최초.+%.*(이(고|며)|경우,)', r).group()
                    range_parse = get_inequality(range_cond, range_parsing(range_cond))
                elif b_r_re_3.search(r) != None: # @ B4 @
                    range_cond = re.search('최초기준.{,10}%.{,10}인경우', r).group()
                    range_parse = get_inequality(range_cond, range_parsing(range_cond))
                else: ## Barrier만 있거나 Range 신규 유형 ##
                    range_parse = 'Range 신규 유형'
                
                range_tuple_list.append( range_tuple(Range = range_parse, Barrier = barrier_parse, Custom = partial_) )
            except: range_tuple_list.append( range_tuple(Barrier='Parsing Error') ) 
    
        else: ### Barrier 없는 경우 ###
            """
            [Barrier 없는 경우]
            R1. Range 2개인 경우 (ex. 80% 이상 100% 미만)
            R2. Range 1개인 경우 (ex. 80% 이상)
            R3. 특이유형: 만기에 Range 표시가 없음 - 트루 13361회 만기상환조건 (rcpNo=20200918000310)
            """
            try:
                if range_re_2range != None: # @ R1 @  # Range 2개 #
                    r_list = re.findall('[\d\.]+%[가-힣]{,5}', r)
                    range_parse = get_inequality(r, r_list)
                elif range_re != None: # @ R2 @  # Range 1개 #  ** 추가 있을 시 range_re 이 순서 위에 새로 추가 해야함
                    range_cond = range_parsing(range_re.group())
                    range_parse = get_inequality(r, range_cond)
                elif range_re_true.search(r) != None: # @ R3 @   ### 특이 유형 ###
                    range_parse = '%s > x' % (re.sub('[^\d%]', '', list(range_tuple_list[-1])[0]))
                elif '조건 신규 유형' in r : ### 투자설명서 만기 수익률지급 부분 확인 ###
                    range_parse = '조건분리 신규 유형'
                else: ### Range pattern에 없음 새로 추가 ###
                    range_parse = '상환조건 신규 유형'
                
                range_tuple_list.append( range_tuple(Range = range_parse) )
            except: range_tuple_list.append( range_tuple(Range='Parsing Error') ) 

    #print(range_tuple_list)
    """ range_tuple_list -> DataFrame """
    return pd.DataFrame([list(t) for t in range_tuple_list], columns=['Range', 'Barrier (N/T)', 'Custom'])

###################################### [Get_payoff] ####################################################################################################################
def Get_payoff(payoff_list, date_list, date_sheet_text): ## date_list : 최초기준일 + 조기상환일 + 만기평가일
    """
    [Payoff parsing]
    1. 투자설명서 원본 수익률 parsing - def index_parsing
    2. payoff_pattern 찾기 - if re.search((fix or gear)_pattern_n)
    3. payoff 유형에 따라 Fixed, Gear 등 분류 - def payoff_tuple, pay_tuple_list.append()   * 수익률 잘못 가져오는 것을 방지하기 위해 re.fullmatch 함수 사용
    * 새로운 유형인 경우, Payoff pattern 에 pattern 추가 및 pay_tuple_list에 넣을 수 있도록 유형 추가
    """
    def index_parsing(index): 
        """
        [index_parsing] 투자설명서 원본에서 pattern 추출을 위해 문자 및 기호 변형
        1. 원본에서 불필요한 수익 설명 제거 - def index_remove
        2. 부등호 통일, 괄호 제거 - translate(str.maketrans)
        3. 원본에서 각 문자열 표현 통일 - index_compile_dict_n, re.sub
        """
        """ 불필요한 수익설명 제외 / 테이블에 없는 비율(ex. 만기상환비율, rcpNo=20200918000310) 텀싯에서 찾아서 넣기 """
        #print(index)
        if '연' in index and index.count('%') >= 2: index = re.sub('연[\d.]+%?', '', index) ## 확정수익에 불필요한 연수익까지 같이 표기되어 있는 경우, 연수익 제거 [ex. 액면 x 18.00%(연 6% 지급)            
        if re.search('([가-힣]*액면[가-힣]*.{,1})?[\d\.\-]+%\(.+\)', index, re.DOTALL) != None: index = re.sub('\(.+\)', '', index, re.DOTALL) ## 확정수익에 불필요한 설명 표기 제거 [ex. 액면 x 80.00%(-20% 원금손실 지급)]
        if re.search('\W[가-힣]*손실률\W', index): index = re.sub('[\-\d\.]+%\W[가-힣]*손실률[\W][\-\d\.]+%', '', index, re.DOTALL) ## 0% >= 원금손실률 > -30% 제거
        ## if '실물인도' in index: rcpNo=20210226003896   미래에셋대우 제29520회
        ##  만기상환비율, rcpNo=20200918000310 KR6KS00001Q2	트루(ELS)13361 ##
        
        """ 기호 통일 및 괄호 삭제 """
        symbol_trans = index.translate(str.maketrans('×xX÷[]{}()=', '***/       ')).replace(' ', '') ## 기호 통일 및 괄호, = 삭제
        """ 문자열 통일 및 정리 """
        index_compile_dict_1 = {'([가-힣]*기준[가-힣]*)*원금(\s?지급)?':'Notional', '[\d\(\)가-힣]*액면[가-힣]*':'Notional', '[가-힣]*기준종목의?':'', '가격변동률':'Exp/Base', 'MA\*':'MAX', \
            '(기초자산|가격)상승률':'Exp/Base', '(기초자산|가격)하락률':'Base/Exp', '\*?만기[가-힣]+하락[가-힣]+기준[가-힣]*':''}
        index_compile_dict_2 = {'[가-힣]*만기\s*(상환)*\s*평가[가-힣]*':'Exp', '[가-힣]*최초\s*기준[가-힣]*':'Base', '만기상환금액=':'', 'Notional\+Notional\*':'Notional*'}

        index_compile_dict_EB = {'100[\d\.]*%\+Exp/Base(\-1)?':'Exp/Base', '100[\d\.]*%\-Exp/Base(\-1)?':'Base/Exp', 'Exp/Base-1':'Exp/Base', '1-Exp/Base':'Base/Exp'}
        index_compile_dict_BE = {'Base/Exp-1':'Base/Exp', '100[\d\.]*%\+Base/Exp(\-1)?':'Base/Exp'}

        """ Gearing 인 부분, Fixed 인 부분 분리하여 정리 """
        if '최초' not in symbol_trans:
            for regex, sub_ in index_compile_dict_1.items(): symbol_trans = re.sub(regex, sub_, symbol_trans, flags=re.I)
        elif '최초' in symbol_trans: 
            for regex, sub_ in {**index_compile_dict_1, **index_compile_dict_2}.items(): symbol_trans = re.sub(regex, sub_, symbol_trans, flags=re.I)
        """ 만기평가/최초기준, 최초기준/만기평가 분리하여 정리 """
        if 'Exp/Base' in symbol_trans:
            for regex, sub_ in index_compile_dict_EB.items(): symbol_trans = re.sub(regex, sub_, symbol_trans, flags=re.I)
        if 'Base/Exp' in symbol_trans:
            for regex, sub_ in index_compile_dict_BE.items(): symbol_trans = re.sub(regex, sub_, symbol_trans, flags=re.I)
        #print(symbol_trans)
        return symbol_trans

    parsing_list = [index_parsing(re.sub('[\xa0\s]', '', i)) for i in payoff_list] 
    #print(parsing_list)
    """ Payoff pattern """
    fix_pattern_1 = re.compile('(Notional\*?)?[\d\.]+%?[\+\-][\d\.\[\]]+%') ## 100% (+|-) 18.00% | 100 (+|-) 18.00% 
    fix_pattern_2 = re.compile('Notional\*?[\d.\-]+%') ## Notional * 118.00%
    fix_pattern_3 = re.compile('[\d.\-]+%') ## 18.00%
    fix_pattern_4 = re.compile('(Notional\*?)?연[\d\.]+%') ## 연 6.00%
    fix_pattern_5 = re.compile('Notional') ## 원금지급 , 액면금액
    fix_pattern_6 = re.compile('(Notional)?손실발생[\d\.\-]+%(Notional)?[\d\.\-]+%보장') ## 손실발생 %, 원금보장 % / 미래에셋증권 제29867회 / rcpNo=20211126000342

    gear_pattern_1 = re.compile('(Notional\*?)?(100[\d\.]*%\+)?MAX[\w\*%,/\-\+\.]+') ## MAX(Gear 100%, 80.00%)  |MAX[/\-\+.,\w*%]+
    gear_pattern_2 = re.compile('(Notional\*?)?Exp/Base\*[\d\.]+%\+[\d\.]+%') ## Gearing 100% + Bonus 20.00%
    gear_pattern_3 = re.compile('(Notional\*?)?Exp/Base\*[\d\.]+%?') ## Gearing 80.00%
    gear_pattern_4 = re.compile('(Notional\*?100[\d\.]*%\+)?(Notional\*?)?Exp/Base') ## Gearing 100.00%
    gear_pattern_5 = re.compile('(Notional\*?)?Exp/Base\*[\d\.]+%?(Notional)?([\d\.\-]+%보장|최대손실률\W?[\d\.\-]+%[가-힣]*)') ## Gearing 100.00% and (원금 80% 보장 or 최대손실률 -20%) * 최대손실률 뒤에 필요없는 문자열 있을수 있음. 키움증권 1564회 / rcpNo=20210416000516

    gear_pattern_1_ = re.compile('(Notional\*?)?Base/Exp\*[\d\.]+%\+[\d.]+%') ## Gearing -100% + Bonus 20.00%
    gear_pattern_2_ = re.compile('(Notional\*?)?Base/Exp\*[\d\.]+%') ## Gearing -80.00%
    gear_pattern_3_ = re.compile('(Notional\*?)?Base/Exp') ## Gearing -100.00%

    """ Payoff 유형을 namedtuple로 저장 """
    pay_tuple_list = []
    def payoff_tuple(Fixed='', Gear='', Bonus='', Cap='', Floor=''):
        """ Payoff 파싱 후 namedtuple로 저장 """
        pay_tuple = namedtuple('pay_tuple', 'Fixed, Gear, Bonus, Cap, Floor')
        return pay_tuple(Fixed, Gear, Bonus, Cap, Floor)
    """ [fixed_pay] Payoff 숫자만 가져온 후, float 변환 / 100 """
    def findall_pay(index): return [ (float(i)/100) for i in re.findall('[\d\.\-]+', index) ]
    def search_pay(index): return float(re.search('[\d\.\-]+', index).group()) / 100
    for p in parsing_list:
        print(p)
        if 'Base' not in p:
            """
            [Fixed 수익 지급인 경우]
            * 공통: 1차 조기상환수익률이 100% 이상인 경우, (Payoff 값/100) - 1
            P1. 유형 1-1 : 액면*100%+x% 인 경우, x%
                    유형 1-2 : 액면*x%(+|-)y% 인 경우, x% 
            P2. 액면*x% 인 경우, if 0% 초과 and 1차 조기상환%>=100% : (상환%/100)-1 else: x% 
            P3. x% 확정 수익만 있는 경우, 확정수익
            P4. 연수익 표기만 있는 경우, 조기상환 텀으로 계산한 수익 + 이전 차수의 수익률
            P5. 원금지급, 액면금액 으로만 표기되어 있는 경우, 0%
            P6. 원금손실/보장 % 있는 경우
            """
            try:
                if fix_pattern_1.fullmatch(p) != None: # @ P1 @
                    pay = '{:g}'.format(findall_pay(p)[-1]) 
                elif fix_pattern_2.fullmatch(p) != None: # @ P2 @
                    pay = '{:g}'.format(search_pay(p) - 1) if search_pay(p) > 0 and search_pay(parsing_list[0]) >= 1 else '{:g}'.format(search_pay(p))
                elif fix_pattern_3.fullmatch(p) != None: # @ P3 @
                    pay = '{:g}'.format(search_pay(p))
                elif fix_pattern_4.fullmatch(p) != None: # @ P4 @
                    def annual_payoff(index, div): return '{:g}'.format( float(pay_tuple_list[-1]._asdict()['Fixed']) + (search_pay(index) / div) ) if len(pay_tuple_list) >= 1 else '{:g}'.format( search_pay(index) / div )
                    date_diff = datetime.strptime(date_list[len(pay_tuple_list) + 1], '%Y-%m-%d') - datetime.strptime(date_list[len(pay_tuple_list)], '%Y-%m-%d')
                    if 190 >= date_diff.days >= 170: pay = annual_payoff(p, 2) # 조기상환 텀 6개월
                    elif 100 >= date_diff.days >= 80: pay = annual_payoff(p, 4) # 조기상환 텀 3개월
                    elif 130 >= date_diff.days >= 110: pay = annual_payoff(p, 3) # 조기상환 텀 4개월
                    else: pay = '연수익률: 조기상환 텀 확인'
                elif fix_pattern_5.fullmatch(p) != None: pay = '0' # @ P5 @  
                elif fix_pattern_6.fullmatch(p) != None: # @ P6 @  
                    pay = '{:g}'.format(search_pay(re.search('(Notional)?[\d\.\-]+%보장', p).group()))
                else:
                    pay_tuple_list.append( payoff_tuple(Fixed = 'Fixed 신규 유형'))
                    continue
                pay_tuple_list.append( payoff_tuple(Fixed = pay) ) 
            
            except: pay_tuple_list.append( payoff_tuple(Fixed = 'Fixed parsing Error'))

        elif 'Exp/Base' in p:
            """
            [Gearing 가격 상승률 수익 지급인 경우]
            P1. MAX[Gearing, x%] 인 경우, Gearing / Floor 추가
            P2. Gearing + x% 인 경우, Gearing / Bonus 추가
            P3. Gearing * x% 인 경우, 참여율 계산
            P4. Gearing만 있는 경우, Gearing 1
            P5. Gearing + 원금보장% 있는 경우, Floor 추가
            """
            try:
                if gear_pattern_1.fullmatch(p) != None: # @ P1 @
                    g = gear_pattern_1.fullmatch(p).group()
                    for i in g[g.index('MAX')+3:].split(','):
                        if 'Base' in i:
                            if gear_pattern_3.fullmatch(i) != None: gear = '{:g}'.format(search_pay(i))
                            elif gear_pattern_4.fullmatch(i) != None: gear = '1'
                            elif re.fullmatch('Exp/Base\*[\d\.]+%?\+100[\d\.]*%', i) != None: gear = '{:g}'.format(search_pay(i)) ## 하나금융투자 제12389회, rcpNo=20210430001044
                        else: floor = '{:g}'.format(search_pay(i) - 1) if search_pay(i) >= 0 else '{:g}'.format(search_pay(i))
                    pay_tuple_list.append( payoff_tuple(Gear=gear, Floor=floor) )
                elif gear_pattern_2.fullmatch(p) != None: # @ P2 @
                    pay_parse = parse('Exp/Base*{}%+{}%', gear_pattern_2.search(p).group()) 
                    pay_tuple_list.append( payoff_tuple(Gear = '{:g}'.format(float(pay_parse[0]) / 100), Bonus = '{:g}'.format(float(pay_parse[1]) / 100)) )
                elif gear_pattern_3.fullmatch(p) != None: # @ P3 @
                    pay_tuple_list.append( payoff_tuple(Gear = '{:g}'.format(search_pay(p))) )
                elif gear_pattern_4.fullmatch(p) != None: # @ P4 @
                    pay_tuple_list.append( payoff_tuple(Gear = '1'))
                elif gear_pattern_5.fullmatch(p) != None: # @ P5 @
                    gear = search_pay(gear_pattern_3.search(p).group())
                    floor = search_pay(re.search('(Notional)?[\d\.]+%보장|최대손실률\W?[\d\.\-]+%[가-힣]*', p).group())
                    pay_tuple_list.append( payoff_tuple(Gear = '{:g}'.format(gear), Floor = '{:g}'.format(floor - 1 if floor > 0 else floor)) )
                else: pay_tuple_list.append( payoff_tuple(Gear = 'Gear E/B 신규 유형'))

            except: pay_tuple_list.append( payoff_tuple(Gear = 'Gear E/B parsing Error'))
        
        elif 'Base/Exp' in p:
            """
            [Gearing 가격 하락률 수익 지급인 경우]
            P1. Gearing + x% 인 경우, -Gearing / Bonus 추가
            P2. Gearing * x% 인 경우, 참여율 계산
            P3. Gearing만 있는 경우, Gearing -1
            """
            try:
                if gear_pattern_1_.fullmatch(p) != None: # @ P1 @
                    pay_parse = parse('Base/Exp*{}%+{}%', gear_pattern_1_.fullmatch(p).group()) 
                    pay_tuple_list.append( payoff_tuple(Gear = '-{:g}'.format(float(pay_parse[0]) / 100), Bonus = '{:g}'.format(float(pay_parse[1]) / 100)) )  ########
                elif gear_pattern_2_.fullmatch(p) != None: # @ P2 @
                    pay_tuple_list.append( payoff_tuple(Gear = '-{:g}'.format(search_pay(p))) )
                elif gear_pattern_3_.fullmatch(p) != None: pay_tuple_list.append( payoff_tuple(Gear = '-1')) # @ P3 @
                else: pay_tuple_list.append( payoff_tuple(Gear = 'Gear B/E 신규 유형'))

            except: pay_tuple_list.append( payoff_tuple(Gear = 'Gear B/E parsing Error'))
            
    #print(pay_tuple_list)
    """ pay_tuple_list -> DataFrame  """
    return pd.DataFrame([list(t) for t in pay_tuple_list], columns=['Fixed', 'Gear', 'Bonus', 'Cap', 'Floor'])

##################################################################################################################################################################################################################
##################################################################################################################################################################################################################

rcept_no = '20210928000135' # 20220111000125

url = "http://dart.fss.or.kr/dsaf001/main.do?rcpNo="+rcept_no
find_dcmno_re = re.compile('dcmNo\'\]\s?\=\s?\"(.*?)\"')
dcmno = find_dcmno_re.findall(urlopen(url).read().decode('utf-8'))[0]
url_to_crawl = f"http://dart.fss.or.kr/report/viewer.do?rcpNo={rcept_no}&dcmNo={dcmno}&eleId=1&offset=0&length=0&dtd=dart3.xsd"
print(url_to_crawl)

soup = BeautifulSoup(urlopen(url_to_crawl).read(),'html.parser')

try: sheet = Cut_Sheet(soup)[0] # 반복 / file_to_save0
except:
    try: sheet = Cut_Sheet(soup, end_text='통지방법\s?및\s?절차$')[-1] ### 키움증권 일부 상품 end_text인 '기타 중요사항' 없음 / rcpNo=20200521000111 334회 
    except IndexError as e: print(f'전체 Cut_Sheet list number 오류 : {e}')
    except Exception as e: print(f'전체 Cut_Sheet 오류 : {e}')


try: date_sheet = Cut_Sheet(sheet, '상환금액의\s?지급에\s?관한\s?사항$','기타사항$|중요사항$|통지방법\s?및\s?절차$')[0]
except Exception as e: print(f'상환 내용 Cut_Sheet 오류 : {e}')
date_sheet_text = re.sub('[\xa0\n\s]', '',date_sheet.get_text())

if '조기상환' in date_sheet_text:
    """
    [Dataframe] 조기상환조건, 조기상환일, 만기상환조건 테이블
    """
    df_range_list = pd.read_html(str(date_sheet), match='조기상환\s*발생\s?조건', header=0) ## 자동조기상환 발생조건  
    if len(df_range_list) == 1: df_range = df_range_list[0]
    else: print('자동조기상환 발생조건 테이블 2개 확인!!!') # *****  Switch 상품 구분 해야함   ******

    df_date = pd.read_html(str(date_sheet), match='조기상환\s*평가일', header=0)[0]# or pd.read_html(str(date_sheet), match='상환\s?금액', header=0)[0] ## 상환금액 
    df_expiry = pd.read_html(str(date_sheet), match='만기\s?(평가|상환)', header=0)[0] ## 만기상환
    """ 
    [조기상환 조건/만기상환 조건/수익률 분리 및 추가 생성]
    1. 조기/만기상환 조건 Range, Barrier가 합쳐져 있을 시 분리('경우' 2개인 경우) - redem_range, exp_range
    2. 만기상환에서 조건 분리된 경우, 분리된 행만큼 갯수 일치하도록 수익률 추가 - exp_pay_list
    3. '경우'라는 단어가 없는 경우, Pass 
    4. 만기상환 수익률(exp_pay_list)에 수익률 외에 주석이 들어있는 경우, exp_range 와 exp_pay_list 갯수 비교하여 exp_pay_list 가 더 많을경우 exp_pay_list 마지막 삭제 
    """
    redem_range, exp_range = [], []
    for range_cond in df_range.iloc[:,-1]:
        try:
            if range_cond.count('경우') == 1: redem_range.append(range_cond.replace('\xa0', ' ')) if re.search('최초\s*기준\s*(가격|지수)의[\d\s.\[\]]+%', range_cond) != None else redem_range.append('조기상환 조건 신규 유형')
            elif range_cond.count('경우') >= 2: redem_range.extend([i.replace('\xa0', ' ')+'경우' for i in range_cond.split('경우') if re.search('최초기준(가격|지수)의[\d\s.\[\]]+%', i) != None])
        except: redem_range.append('조기상환 조건 분리 Error')

    exp_pay_list = list(df_expiry.iloc[:,-1])
    exp_range_re = re.compile('최초\s*기준\s*(가격|지수)의?[\d\s.\[\]×xX]+%|만기\s*평가[\s가-힣]+/최초\s*기준[\s가-힣]+.+경우')  ### 2번째 표현식은 Range 조건에 '%' 가 없음 -> 트루 13361회 만기상환조건 (rcpNo=20200918000310) 
    for num, range_cond in enumerate(df_expiry.iloc[:,0]):
        try:
            if range_cond.count('경우') == 1: exp_range.append(range_cond.replace('\xa0', ' ')) if exp_range_re.search(range_cond) != None else exp_range.append('만기상환 조건 신규 유형')
            elif range_cond.count('경우') >= 2: 
                if '경우,' not in range_cond: exp_range.extend([i.replace('\xa0', ' ')+'경우' for i in range_cond.split('경우') if exp_range_re.search(i) != None])
                elif '경우,' in range_cond: exp_range.append(range_cond.replace('\xa0', ' ')) if exp_range_re.search(range_cond) != None else exp_range.append('만기상환 조건 신규 유형')
                ### 현대차증권 제2066회 , 경우가 2개인데 배리어+레인지 같이 있는 유형 있음  rcpNo=20200114000125 ###
                if len(exp_range) > num + 1: exp_pay_list.insert(num + 1, exp_pay_list[num])    
        except: exp_range.append('만기상환 조건 분리 Error') 
    if len(exp_range) == num: del exp_pay_list[-1]
    """
    [No_list] 상환 차수 
    상환평가일에서 차수 가져온 후 만기상환 조건 개수에 맞춰서 차수 생성
    """
    redem_no = [re.search('\d{,2}', x).group() for x in list(df_date.iloc[:,0])]
    exp_no = [str(int(redem_no[-1]) + 1)] * len(exp_range)
    No_list = redem_no + exp_no
    """
    [Observe_under_list] 관찰 기초자산 갯수
    """
    Observe_under_list = Get_observe_under(redem_range + exp_range)
    """
    [Date_df] 최초기준일, 조기상환평가일, 만기평가일        
    1. 최초기준일, 조기상환평가일, 만기평가일 parsing - base_date / redem_date / exp_date 
    2. 조기상환평가일 + 만기평가일(만기 상환조건 갯수만큼 추가)
    """
    base_date_re = re.compile('최초\s*(기준.{,3}평가일|관찰일)[\d\W년월일]{10,20}') # 최초\s*(기준.{,3}평가일|관찰일).{,5}[\d\s\[\]]+[가-힣][\d\s\[\]]+[가-힣][\d\s\[\]]+일
    base_date_text = base_date_re.search(date_sheet_text).group()
    base_date = re.sub('[^\d]', '-', re.search('\d+[가-힣]\d+[가-힣]\d+[가-힣]', ''.join(filter(str.isalnum, base_date_text))).group()[:-1]) ## 최초기준일 ##

    redem_date = list(df_date.filter(regex=re.compile('상환\s*평가일')).iloc[:,0])  ## 조기상환평가일 ##
    exp_date_re = re.compile('만기(상환)?s*평가일[\d\W년월일]{10,}')
    exp_date = [''.join(filter(str.isalnum, exp_date_re.search(date_sheet_text).group()))] * len(exp_range)

    Date_df = Get_date(redem_date + exp_date)
    """
    [Redem, Exp_lag]
    """
    try: Redem_lag = re.sub('[^\d]', '', re.search('조기\s*상환.{,10}(후|부터)[\d\s\[\]\(\)]+(거래소)?\s*영업일', date_sheet_text).group())
    except: Redem_lag = 'Lag parsing Error'
    try: Exp_lag = re.sub('[^\d]', '', re.search('만기.{,15}(후|부터)[\d\s\[\]\(\)]+(거래소)?\s*영업일', date_sheet_text).group())
    except: Exp_lag = Redem_lag
    print('Redem = ' + Redem_lag), print('Exp = ' + Exp_lag)
    """
    [Range_Barrier_df] 상환 Range, Barrier - 조기상환 + 만기상환
    """
    Range_Barrier_df = Get_range(redem_range + exp_range, date_sheet_text)
    """
    [Payoff_df] 조기상환, 만기상환 Payoff
    1. Payoff 연수익률 계산을 위해 최초기준일 추가 - annual_calc
    2. 조기상환, 만기상환 Payoff parsing
    """
    annual_calc = list(Date_df.iloc[:,-1])
    annual_calc.insert(0, base_date)
    try: Payoff_df = Get_payoff(list(df_date.iloc[:,-1]) + exp_pay_list, annual_calc, date_sheet_text)
    except Exception as e:
        Payoff_df = pd.DataFrame(['Payoff 확인'] * len(No_list)) ## Date_list에서 최초기준일 삭제 ##
    #print(base_date, exp_date[0])
    """
    [Df_final] No_list, Symbol_list, Date_df, Range_Barrier_df, Payoff_df
    """
    Df_ = pd.DataFrame(list(zip(No_list, Observe_under_list)), columns=['No', 'Symbol'])#, index=list(range(1,len(No_list)+1)))
    Df_
    Df_final = pd.concat([Df_, Date_df, Range_Barrier_df, Payoff_df], axis=1)
    Df_final.index = list(range(1,len(No_list)+1))

Df_final

