import pandas as pd
import re
import pdfplumber
import docx2txt

def read_file(file):
    if 'docx' in file:
        text = docx2txt.process(file).replace('\xa0', ' ').replace('\t', '').split('\n')
        text = list(filter(None, text))

    elif 'pdf' in file: 
        with pdfplumber.open(file) as pdf:
            text = []
            for i in range(30):
                try:
                    page = pdf.pages[i]
                    for i in page.extract_text().split('\n'):
                        if ' ' != i: text.append(i.strip())
                except: break

    return text

def save_csv(all_df, path):
    col_array = ["Title", "발행가액", "발행일", "만기상환일","화폐단위", "Structure", "기초자산Type", "Settle_Date_Lags"]\
        + ["기초자산"+str(i+1) for i in range(5)] + ["만기수익률", "만기장벽", "중도상환수익률"]\
        + ["Lizard차수"+str(i+1) for i in range(5)] + ["Lizard수익률"+str(i+1) for i in range(5)]\
        + ["LizardRange"+str(i+1) for i in range(5)] + ["쿠폰수익률", "쿠폰Range"]\
        + ["구간Range" +str(i+1) for i in range(15)] + ["기초자산기준일", "만기평가일", "종평여부"]\
        + ["구간종료일" +str(i+1) for i in range(15)] + ["쿠폰평가일" +str(i+1) for i in range(60)]

    final_df = pd.DataFrame(all_df, columns=col_array).transpose()
    
    return final_df.to_csv(path, mode='w', encoding='utf-8-sig')

class Crawling_Function():
    def __init__(self, text):
        self.text = text

    def range_index(self, s_word, n, e_word):
        s = []
        e = []
        for i in self.text:
            s.append(i)
            if s_word in i:
                break
        for i in self.text[len(s)+1:]:
            e.append(i)
            if e_word in i:
                break
        return self.text[len(s)+n:len(s)+len(e)]

    def find_index(self, word, n):
        s = []
        for i in self.text:
            s.append(i)
            if word in i:
                break
        return self.text[len(s)+n]

    def len_list(self, word, n):
        l = []
        for i in self.text:
            l.append(i)
            if word in i:
                break
        return len(l) + n

    def payoff(self, index):
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

    def range_value(self, index):
        index = re.sub(r'[^0-9%.x]','', index)
        s = index.find('x')+1
        return index[s:]

    def list2df(self, list, n):
        df = []
        for i in range(n):
            try: df.append(list[i])
            except: df.append('')
        return df

    def pdf_index(self, word):
        l = word.find('[')+1
        n = word.find(']')
        return word[l:n] 

    def data2list(self, title, issue_price, issue_date, exp_pay_date, ccy, structure, lag, underlying, 
                    exp_pay, exp_barrier, redem_pay, lizard_num, lizard_pay, lizard_barrier, 
                    coupon_pay, coupon_range, redem_range, base_date, exp_date, redem_date, coupon_date):

        # 기초자산 코드 변환, 정렬
        undercd = []
        under_order = {'WTI선물':'CL1 COMDTY', '브렌트유선물' : 'CO1 COMDTY', 'USDKRWFixingRate':'USDKRW',
                'KOSPI200': 'I.101', 'HSCEI': 'I.HSCE', 'HSI': 'I.HSI', 'HSCEI 환헤지 지수': 'HSCEKRWH INDEX:746', 'HSCEKRWH':'HSCEKRWH INDEX:746',
                'S&P500': 'I.GSPC', 'S&P500 ESG 지수': 'SPESG INDEX', 'S&P500 환헤지 지수': 'SPXNKH INDEX:743', 'SPXNKH':'SPXNKH INDEX:743',
                'S&P500 콴토 지수': 'SP5QUKAP INDEX:635', 'NKY225': 'I.N225', 'NIKKEI225':'I.N225', 'EUROSTOXX50': 'SX5E INDEX', 'EUROSTOXX50 환헤지 지수': 'IX5MKHKP INDEX',
                'IX5MKHKP':'IX5MKHKP INDEX', 'EUROSTOXX50 ESG 지수': 'SX5EESG INDEX', 'EUROSTOXX50콴토 지수':'IX5QEKKP INDEX:636','DAX': 'I.GDAXI', 
                'CSI300': 'SHSZ300 INDEX:285',  '삼성전자 보통주(005930)': 'KR7005930003', 'SK하이닉스 보통주(000660)': 'KR7000660001',
                '현대차 보통주(005380)': 'KR7005380001', 'NAVER 보통주(035420)': 'KR7035420009', 'POSCO 보통주(005490)': 'KR7005490008',
                'LG화학 보통주(051910)': 'KR7051910008', 'KT 보통주(030200)':'KR7030200000', '한국전력':'KR7015760002'}
        try:
            for key, val in under_order.items():
                if key in underlying: undercd += {val}
        except: pass
        if len(undercd) != len(underlying): undercd = underlying

        # 크롤링 데이터 리스트
        under_type = 'Equity'
        undercd_df = self.list2df(undercd, 5)
        lizard_num_df = self.list2df(lizard_num, 5)
        lizard_pay_df = self.list2df(lizard_pay, 5)
        lizard_barrier_df = self.list2df(lizard_barrier, 5)
        redem_range_df = self.list2df(redem_range, 15)
        avg_close = ''
        redem_date_df = self.list2df(redem_date, 15)
        coupon_date_df = self.list2df(coupon_date, 60)     
      
        data_list = []     
        data_list.extend([title, issue_price, issue_date, exp_pay_date, ccy, structure, under_type, lag] 
            + undercd_df + [exp_pay, exp_barrier, redem_pay] + lizard_num_df + lizard_pay_df + lizard_barrier_df\
            + [coupon_pay, coupon_range] + redem_range_df + [base_date, exp_date, avg_close] + redem_date_df + coupon_date_df)

        return data_list