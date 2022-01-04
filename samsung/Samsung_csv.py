from typing import final
import docx2txt
import re
import os
import pdfplumber
from pandas.core.base import IndexOpsMixin
import Samsung_List
import pandas as pd

def range_index(s_word, n, e_word):
    s = []
    e = []
    for i in text:
        s.append(i)
        if s_word in i:
            break
    for i in text[len(s)+1:]:
        e.append(i)
        if e_word in i:
            break
    return text[len(s)+n:len(s)+len(e)]

def find_index(s_word, n):
    s = []
    for i in text:
        s.append(i)
        if s_word in i:
            break
    return text[len(s)+n]

def len_list(word, n):
    l = []
    for i in text:
        l.append(i)
        if word in i:
            break
    return len(l) + n

def payoff(index):
    if index.count('%') == 2:
        index = re.sub(r'[^0-9%.]','', index)    
        s = index.find('%')+1
        e = index.find('%',s)+1
        return index[s:e]
    else: 
        index = re.sub(r'[^0-9%.x]','', index)
        s = index.find('x')+1
        e = index.find('%')+1
        return index[s:e]

def range_value(index):
    index = re.sub(r'[^0-9%.x]','', index)
    s = index.find('x')+1
    return index[s:]

def list2df(list, n):
    df = []
    for i in range(n):
        try: df.append(list[i])
        except: df.append('')
    return df

def pdf_index(word):
    l = word.find('[')+1
    n = word.find(']')
    return word[l:n]  

all_df = []

path = r'Y:\(0000)금융공학연구소\402.파생상품팀\파생상품팀 이슈\06. Live\20210517_삼성증권 Live\텀싯_2110이후'
fd_list = os.listdir(path)

for file_num, krs_code in Samsung_List.dict_list.items():
    # 파일 이름 찾기 
    for file_name in fd_list:
        if '최종' in file_name: continue
        elif 'OTC' in file_name:
            if f'_{file_num}_' in file_name: break
            else: file_name = ''
        elif 'SWAP' in file_name:
            if f'_{file_num}_' in file_name: break
            else: file_name = ''
        elif str(file_num) in file_name: break
        else: file_name = ''
    
    try:
        ##### Word 파일 크롤링 ##### 디지털콜, 낙인, 노낙인, 리자드, 먼슬리, SWAP
        if 'docx' in file_name:
            text = docx2txt.process(fr'{path}\{file_name}').replace('\xa0', ' ').split('\n')
            text = list(filter(None, text))

            ### Word 공통 ###
            # 지급일 / 최초기준일 / 만기평가일 
            lag = find_index('최종기준가격 결정일(불포함)', -1)
            lag = re.sub(r'[^0-9]','', lag)
            base_date = find_index('(2) 최초기준가', 1)[1:-1]
            exp_date = find_index('만기일', 0)[1:-1]

            # 기초자산 
            underlying = []
            under = []
            for i in text:
                if '최초기준가격 결정일 현재' in i: underlying.append(i)
            for i in underlying:
                n = i.find(']')
                under.append(i[1:n])

            # Word 상품 구조
            for struc in text:
                if '리자드' in struc: break
                elif '스텝' in struc: break
                elif '지우개' in struc: break
                elif '월지급' in struc: break
                elif '디지털' in struc: break
                elif '분기지급식' in struc: break
                else: struc = '구조 확인!!!!'
            
            ### 디지털콜 ###
            if '디지털' in struc:
                # 발행가액 / 발행일 / 만기상환일 / 행사가격
                issue_price = re.sub(r'[^0-9]', '', find_index('(3) 1증권당 발행가액', 0))
                issue_date = find_index('(9) 발행일', 0)[1:-1]
                exp_pay_date = find_index('만기일', 2)[1:-1]
                Event_price = payoff(find_index('(5) 행사가격', 0))

                # 만기 payoff
                pay_df = []
                for i in text:
                    if '액면가액 x' in i: pay_df.append(i)
                exp_pay_up = payoff(pay_df[0])
                exp_pay_down = payoff(pay_df[1])

                exp_pay = Event_price
                exp_barrier = ''
                redem_pay = ''
                lizard_num = ''
                lizard_pay = ''
                lizard_range = ''
                coupon_pay = exp_pay_down
                coupon_range = exp_pay_up
                redem_range = ''
                redem_date = ''
                coupon_date = ''

            ### 디지털콜 제외 구조 ###
            else:
                # 발행가액
                try: price = find_index('(2) 발행가액', 0) 
                except : price = find_index('초기교환금액', 0)

                if 'USD' in price: ccy = 'USD'
                else: ccy = 'KRW'
                price = re.sub(r'[^0-9x.]', '', price)
                s = price.find('x')+1
                issue_price = round(float(price[s:]) * 100, 0)

                # 발행일 / 만기상환일 / 조기상환일  
                try: issue_date = find_index('(7) 발행일', 0)[1:-1]
                except:
                    try: issue_date = find_index('유효일', 0)[1:-1]
                    except: 
                        try: issue_date = find_index('초기교환금액 지급일', 0)[1:-1]
                        except: issue_date = ''        
                        
                exp_pay_date = find_index('만기일', 3)[1:-1]   
                redem_d = range_index('(4) 중간기준가', 1, '각 기준가격 결정일')
                redem_date = []
                for i in redem_d:
                    redem_date.append(i[4:])

                # 조기상환 payoff / 만기 payoff
                pay_df = []
                for i in text:
                    if '액면가액 x' in i: pay_df.append(i)
                    elif '명목금액 x' in i: pay_df.append(i)
                redem_pay = payoff(pay_df[0])

                for i in pay_df:
                    if '만기' in i: 
                        exp_pay = payoff(i)
                        break

                # 조기상환 range
                s_redem = len_list('(5) 중간기준가', 1)
                e = s_redem + (len(underlying) + 1) * len(redem_date)
                redem_r = text[s_redem:e]

                redem_range = []
                for i in range(1, len(redem_r), len(underlying) + 1):
                    i = redem_r[i]
                    redem_range.append(range_value(i))

                # 지우개 #
                if '지우개' in struc:
                    eject_n = int(re.sub(r'[^0-9]', '', find_index('(6) 단독관찰 기초자산', 1)))
                    s_redem = len_list('(7) 중간기준가', 1)
                    e = s_redem + (len(underlying) + 1) * eject_n
                    redem_r = text[s_redem:e]
                    eject_r = text[e:e + (len(redem_date) - eject_n) * 2]

                    redem_range.clear()
                    for i in range(1, len(redem_r), len(underlying) + 1):
                        i = redem_r[i]
                        redem_range.append(range_value(i))

                    for i in range(1, len(eject_r), 2):
                        i = eject_r[i]
                        redem_range.append(range_value(i))

                # 먼슬리 Range, payoff, redem, coupon #
                if '월지급' in struc or '분기지급식' in struc:
                    coupon_pay = redem_pay

                    coupon_range = range_value(find_index('(5) 월수익 행사', 0))

                    coupon_date = []
                    coupon_d = range_index('(4) 월수익 중간기준가', 1, '각 기준가격 결정일')
                    for i in coupon_d: coupon_date.append(i[4:])
                    coupon_date.append(exp_date)

                    redem_date.clear()
                    coupon_n = re.sub(r'[^0-9]', '', find_index('(8) 중간기준가', 0))
                    for i in range(int(coupon_n), len(coupon_date)-1, int(coupon_n)+1):
                        redem_date.append(coupon_date[i])

                    redem_range.clear()
                    s_redem = len_list('(10) 중간기준가', 1)
                    e = s_redem + (len(underlying) + 1) * len(redem_date)
                    redem_r = text[s_redem:e]
                    for i in range(1, len(redem_r), len(underlying) + 1):
                        i = redem_r[i]
                        redem_range.append(range_value(i))

                    exp_pay = '0%'
                    redem_pay = '0%'
                else: 
                    coupon_pay = ''
                    coupon_range = ''
                    coupon_date = ''

                # 만기 range
                exp_r = find_index('만기행사가격', 0)
                exp_range = range_value(exp_r)
                redem_range.append(exp_range)

                # 리자드 Range / 차수 / pay #
                if '리자드' in struc:
                    lizard = range_index('리자드 행사', -1, '이행의무')
 
                    lizard_r = []
                    for i in lizard:
                        if '최초기준가' in i: lizard_r.append(i)
                    lizard_range = []
                    for i in range(0, len(lizard_r), len(underlying)):
                        i = lizard_r[i]
                        lizard_range.append(range_value(i))

                    lizard_num = []
                    lizard_pay = []
                    for i in pay_df:
                        if '리자드' in i:
                            n = i.find('차')
                            lizard_num.append(i[n-1])
                            lizard_pay.append(payoff(i))
                else:
                    lizard_range = ''
                    lizard_num = ''
                    lizard_pay = ''
                
                ## 낙인 배리어
                try: exp_barrier = range_value(find_index('하락한계', 0))
                except: exp_barrier = ''  

            ### Word 공통 ###
            # SWAP 
            if 'SWAP' in  file_name:
                div = 'SWAP '
                try:
                    swap_df = range_index('(5) 스프레드', 0, '자동조기해지')
                    swap_spread = swap_df[0]
                    if '13' not in swap_df:
                        swap_date = []
                        for i in range(2, 13):
                            d = swap_df[swap_df.index(str(i))-1]
                            swap_date.append(d)
                        swap_date.append(exp_pay_date)
                    else: swap_date = ''
                except: pass
            else: 
                div = ''
                swap_spread = ''
                swap_date = ''  
        
        ##### PDF 파일 크롤링 ##### 먼슬리
        elif 'pdf' in file_name:
            with pdfplumber.open(fr'{path}\{file_name}') as pdf:
                text = []
                for i in range(30):
                    try:
                        page = pdf.pages[i]
                        text += page.extract_text().split('\n')
                    except: break

            for struc in text:
                if '월지급' in struc: break
                else: struc = '구조확인!!!'

            # 발행가액
            price = find_index('발행가액', -1)
            if 'USD' in price: ccy = 'USD'
            else: ccy = 'KRW'

            price = re.sub(r'[^0-9x.]', '', price)
            s = price.find('x')+1
            issue_price = round(float(price[s:]) * 100, 0)

            # 발행일/만기일/만기상환일/최초기준일/Lag
            issue_date = pdf_index(find_index('발행일', -1))
            exp_date = pdf_index(find_index('만기일', -1))
            exp_pay_date = pdf_index(find_index('만기상환금액', -1))
            base_date = pdf_index(find_index('(2) 최초기준가격', -1))
            lag = pdf_index(find_index('최종기준가격 결정일(불포함)', -1))

            # 기초자산
            underlying = []
            under = []
            for i in text:
                if '최초기준가격 결정일 현재' in i: underlying.append(i)
            for i in underlying:
                l = i.find('[')+1
                n = i.find(']')
                under.append(i[l:n])

            # 쿠폰평가일
            coupon_date = []
            coupon_d = range_index('(4) 월수익 중간기준가', -1, '각 기준가격 결정일')
            for i in coupon_d:
                if '차' in i:
                    l = i.find(':')+2
                    coupon_date.append(i[l:].strip())
            coupon_date.append(exp_date)

            # 조기상환일
            redem_date = []
            coupon_n = re.sub(r'[^0-9]', '', find_index('(8) 중간기준가', 0))
            for i in range(int(coupon_n), len(coupon_date)-1, int(coupon_n)+1):
                redem_date.append(coupon_date[i])

            # 쿠폰 payoff
            coupon_pay = find_index('액면가액 x', 0)[:-2]

            # 쿠폰, 조기상환, 만기 Range
            redem_df = []
            for i in text:
                if '최초기준가격 x' in i: redem_df.append(payoff(i))

            coupon_range = redem_df[0]

            redem_range = []
            for i in range(len(underlying), len(redem_df), len(underlying)):
                i = redem_df[i]
                redem_range.append(range_value(i))

            # 배리어
            for i in text:
                if '하락한계' in i:
                    exp_barrier = redem_range[-1]
                    redem_range = redem_range[:-1]
                    break
                else: exp_barrier = ''

            redem_pay = '0%'
            exp_pay = '0%'
            lizard_num = ''
            lizard_pay = ''
            lizard_range = ''
            div = ''
            swap_spread = ''
            swap_date = ''
            
        else: continue

        ### Word, PDF 공통 ###
        # 기초자산 순서
        undercd = []
        under_order = {'WTI선물':'CL1 COMDTY', '브렌트유선물' : 'CO1 COMDTY', 'USDKRWFixingRate':'USDKRW',
                'KOSPI200': 'I.101', 'HSCEI': 'I.HSCE', 'HSI': 'I.HSI', 'HSCEI 환헤지 지수': 'HSCEKRWH INDEX:746',
                'S&P500': 'I.GSPC', 'S&P500 ESG 지수': 'SPESG INDEX', 'S&P500 환헤지 지수': 'SPXNKH INDEX:743',
                'S&P500 콴토 지수': 'SP5QUKAP INDEX:635', 'NKY225': 'I.N225', 'EUROSTOXX50': 'SX5E INDEX', 'EUROSTOXX50 환헤지 지수': 'IX5MKHKP INDEX',
                'EUROSTOXX50 ESG 지수': 'SX5EESG INDEX', 'EUROSTOXX50콴토 지수':'IX5QEKKP INDEX:636','DAX': 'I.GDAXI', 
                'CSI300': 'SHSZ300 INDEX:285',  '삼성전자 보통주(005930)': 'KR7005930003', 'SK하이닉스 보통주(000660)': 'KR7000660001',
                '현대차 보통주(005380)': 'KR7005380001', 'NAVER 보통주(035420)': 'KR7035420009', 'POSCO 보통주(005490)': 'KR7005490008',
                'LG화학 보통주(051910)': 'KR7051910008', 'KT 보통주(030200)':'KR7030200000', '한국전력':'KR7015760002', '카카오 보통주(035720)':'KR7035720002',
                '하나금융지주 보통주(086790)':'KR7086790003'}
        for key, val in under_order.items():
            if key in under: undercd += {val}
        if len(undercd) != len(underlying): undercd = underlying

        # 부킹툴 구조
        if '스텝' in struc:
            if exp_barrier == '':
                structure = 'StepDn_NoKI'
            else: structure = 'StepDn_KI'
        elif '리자드' in struc:
            if exp_barrier == '':
                structure = 'StepDn_NoKI_[Lizard]'
            else: structure = 'StepDn_KI_[Lizard]'
        elif '지우개' in struc:
            structure = '지우개'
        elif '월지급' in struc or '분기지급식' in struc:
            if exp_barrier == '':
                structure = 'StepDn_NoKI_MtlyCpn'
            else: structure = 'StepDn_KI_MtlyCpn'
        elif '디지털' in struc:
            structure = 'DigitalCall'
        else: structure = struc

        # 만종평 
        if 'docx' in file_name:
            for i in text:
                if '산술평균' in i:
                    avg_df = find_index('(2) 최종기준가', 1)
                    avg_close = avg_df[avg_df.find('[')+1 : avg_df.find(']')]
                    exp_date = avg_close + exp_date
                    structure += '_만종평'
                    break
        elif 'pdf' in file_name:
            for i in text:
                if '산술평균' in i:
                    structure += '_만종평'
                    break  
        
        under_type = 'Equity'
        undercd_df = list2df(undercd, 5)
        lizard_num_df = list2df(lizard_num, 5)
        lizard_pay_df = list2df(lizard_pay, 5)
        lizard_range_df = list2df(lizard_range, 5)
        redem_range_df = list2df(redem_range, 15)
        avg_close = ''
        redem_date_df = list2df(redem_date, 15)
        coupon_date_df = list2df(coupon_date, 60)
        swap_date_df = list2df(swap_date, 12)     

        data_list = []     
        data_list.extend([div+str(file_num)+'회', str(int(issue_price)), issue_date, exp_pay_date, ccy, krs_code, structure, under_type, lag] 
            + undercd_df + [exp_pay, exp_barrier, redem_pay] + lizard_num_df + lizard_pay_df + lizard_range_df\
            + [coupon_pay, coupon_range] + redem_range_df + [base_date, exp_date, avg_close] + redem_date_df + coupon_date_df\
            + [swap_spread] + swap_date_df)

        if len(data_list) == 140 :
            all_df.append(data_list)

    except: continue

col_array = ["회차", "발행가액", "발행일", "만기상환일","화폐단위", "KRS코드", "Structure", "기초자산Type", "Settle_Date_Lags"]\
        + ["기초자산"+str(i+1) for i in range(5)] + ["만기수익률", "만기장벽", "중도상환수익률"]\
        + ["Lizard차수"+str(i+1) for i in range(5)] + ["Lizard수익률"+str(i+1) for i in range(5)]\
        + ["LizardRange"+str(i+1) for i in range(5)] + ["쿠폰수익률", "쿠폰Range"]\
        + ["구간Range" +str(i+1) for i in range(15)] + ["기초자산기준일", "만기평가일", "종평여부"]\
        + ["구간종료일" +str(i+1) for i in range(15)] + ["쿠폰평가일" +str(i+1) for i in range(60)]\
        + ["SWAP_Spread"] + ["SWAP쿠폰평가일" +str(i+1) for i in range(12)]

final_df = pd.DataFrame(all_df, columns=col_array).transpose() 

final_df.to_csv(fr'Y:\(0000)금융공학연구소\402.파생상품팀\파생상품팀 이슈\06. Live\20210517_삼성증권 Live\부킹툴\{Samsung_List.effect_date}.csv', mode='w', encoding='utf-8-sig')

