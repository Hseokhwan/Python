import re
from Method_All import Crawling_Function

def hana_pdf(text):  # 노낙인/낙인/리자드/먼슬리
    df = Crawling_Function(text)
    # 종목명
    title = df.find_index('하나금융투자', -1).strip()

    # 기초자산
    underlying = list(filter(None, df.find_index('차 중간행사가격', -3).replace('구분','').split(' ')))

    # 발행가액/ccy
    price = df.find_index('발행가격', -1)
    if '[' in price: issue_price = float(df.pdf_index(price)) * 100
    else: issue_price = str(float(re.sub(r'[^0-9.]', '', price)[1:]) * 100)
    if 'USD' in df.find_index('발행가액', -1): ccy = 'USD'
    else: ccy = 'KRW'
    
    # 발행일/만기상환일/Lag/최초기준일/만기일
    issue_date = df.find_index('발 행 일', -1).strip()
    issue_date = issue_date[issue_date.find(':')+2 :]
    exp_pay_date = df.find_index('만 기 일', -1).strip()
    exp_pay_date = exp_pay_date[exp_pay_date.find(':')+2 :]
    lag = re.sub(r'[^0-9]', '', df.find_index('평가일(불포함) 후', -1))
    base_date = df.find_index('기준가격 결정일:', -1).strip()
    base_date = base_date[base_date.find(':')+2 :]
    exp_date = df.find_index('만기평가일:', -1).strip()
    exp_date = exp_date[exp_date.find(':')+2 :]
    
    # 먼슬리, 먼슬리 X 데이터 분리
    for Mtly in text:
        if '이자지급' in Mtly: break
        else: Mtly = ''
    data = []
    Mtly_data = []
    Mtly_data_redem = []
    for i in text:
        if Mtly == '': 
            if '기초자산의 기준가격의' in i: data.append(i)
        else:
            if '기초자산의 기준가격의' in i: Mtly_data.append(i)
            if '차 중간행사가격' in i: Mtly_data_redem.append(i)
            
    ## 먼슬리 X : 조기상환일/조기상환Range/조기상환payoff/만기 Range/쿠폰pay/쿠폰Range/쿠폰평가일
    if Mtly == '':
        redem_date = []
        redem_range = []
        redem_pay = []          
        for i in data:
            redem_date.append(i[i.find('년')-4 : i.find('일')+1].strip())
            redem_range.append(  re.sub(r'[^0-9]', '', i[i.find('기준') : i.find('%')]) + '%'  )
            redem_pay.append(   str(round(float(i[i.find('%')+1 : i.rfind('%')].strip()) - 100, 4)) + '%'   )  
        coupon_pay = ''
        coupon_range = ''
        coupon_date = ''

    ## 먼슬리 : 조기상환일/조기상환Range/조기상환payoff/만기 Range/쿠폰pay/쿠폰Range/쿠폰평가일
    else:
        redem_date = []
        redem_range = []
        redem_pay = []
        for i in Mtly_data[len(Mtly_data)-len(Mtly_data_redem):]:
            redem_date.append(i[i.find('년')-4 : i.find('일')+1].strip())
            redem_range.append(  re.sub(r'[^0-9]', '', i[i.find('기준') : i.find('%')]) + '%'  )
            redem_pay.append(   str(round(float(i[i.find('%')+1 : i.rfind('%')].strip()) - 100, 4)) + '%'   )  

        l = Mtly_data[0]
        coupon_pay = l[l.find('%')+1 : l.rfind('%')].strip() + '%'
        coupon_range = re.sub(r'[^0-9]', '', l[l.find('기준') : l.find('%')]) + '%'
        coupon_date = []
        for i in Mtly_data[:-len(Mtly_data_redem)]: coupon_date.append(i[i.find('년')-4 : i.find('일')+1].strip())

    # 만기Range/만기payoff
    exp_pay = str(round(float(re.sub(r'[^0-9.]', '', df.find_index('만기행사가격보다', 0))) - 100, 4)) + '%'
    exp_range = re.sub(r'[^0-9:.%]', '', df.find_index('만기행사가격:', -1))
    redem_range.append(   exp_range[exp_range.find(':')+1:]   )

    # 배리어 전체(만기/리자드)
    barrier_data = [] 
    lizard_data = []
    for i in text: 
        if '행사정지가격:' in i: barrier_data.append(i)

    if len(barrier_data) != 0:
        for i in barrier_data[:]:
            if '차' in i: 
                lizard_data.append(i)
                barrier_data.remove(i)
        if len(barrier_data) != 0:
            exp_barrier = re.sub(r'[^0-9:.%]', '', str(barrier_data))   
            exp_barrier = exp_barrier[exp_barrier.find(':')+1:] 
        else: exp_barrier = ''
    else: exp_barrier = ''

    # 리자드 차수/수익률/배리어
    lizard_num = []
    lizard_pay = []
    lizard_barrier = []
    if len(lizard_data) != 0:
        for i in lizard_data:
            lizard_num.append(   i[i.find('차')-1]   )
            lizard_b = re.sub(r'[^0-9:.%]', '', i)
            lizard_barrier.append(   lizard_b[lizard_b.find(':')+1:]   )
        for i in text:
            if '액면잔액 *' in i:
                lizard_pay.append(   str(round(float(re.sub(r'[^0-9.]', '', i[i.find('*')+2 : i.find('%')+1])) - 100, 4)) + '%'   )   
    else: 
        lizard_num = ''
        lizard_pay = ''
        lizard_barrier = ''

    # 상품구조
    if exp_barrier == '':
        if lizard_barrier != '': structure = 'StepDn_NoKI_[Lizard]'
        elif coupon_pay != '': structure = 'StepDn_NoKI_MtlyCpn'
        else: structure = 'StepDn_NoKI'
    else:
        if lizard_barrier != '': structure = 'StepDn_KI_[Lizard]' 
        elif coupon_pay != '': structure = 'StepDn_KI_MtlyCpn'
        else: structure = 'StepDn_KI'

    
    return df.data2list(title, issue_price, issue_date, exp_pay_date, ccy, structure, lag, underlying, 
                exp_pay, exp_barrier, redem_pay[0], lizard_num, lizard_pay, lizard_barrier, 
                coupon_pay, coupon_range, redem_range, base_date, exp_date, redem_date, coupon_date)

