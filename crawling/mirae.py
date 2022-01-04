import re
from Method_All import Crawling_Function

def mirae_docx_A(text):
    df = Crawling_Function(text)
    
    # 종목명
    title = text[0][  : text[0].find('회') +1  ]

    # 기초자산
    underlying = []
    for i in text:
        if '최초기준가격 평가일의' in i: underlying.append(    i[i.find('의') + 2 : i.rfind('의')]    )
    
    # 발행가액/ccy
    try: 
        price = df.find_index('발행가액', 0)
        issue_price = str(float(re.sub(r'[^0-9.]', '', price)) * 100)
    except :
        price = df.find_index('발행가액', -1)
        issue_price = str(float( re.sub(r'[^0-9.]', '', price[price.find('액면금액') : ]) ) * 100) 
    if 'USD' in df.find_index('발행가액', -1): ccy = 'USD'
    else: ccy = 'KRW'

    # 발행일/만기상환일/Lag/최초기준일/만기일
    issue_date = df.find_index('발 행 일', -1)
    issue_date = issue_date[ issue_date.find(':') + 1 : ].strip()
    exp_pay_date = df.find_index('만 기 일', -1)
    exp_pay_date = exp_pay_date[ exp_pay_date.find(':') + 1 : ].strip()
    lag = re.sub(r'[^0-9]', '', df.find_index('상환평가일(불포함)', -1))
    base_date = df.find_index('최초기준가격 평가일:', -1).strip()
    base_date = base_date[ base_date.find(':') + 1 : ].strip()
    exp_date = df.find_index('만기평가일 :', -1).strip()
    exp_date = exp_date[ exp_date.find(':') + 1 : exp_date.find('(') ].strip()
    
    # 조기상환 date, range, pay
    redem_dataframe = df.range_index('차 자동조기상환', -1, '상환평가일(불포함)')

    redem_date = []
    redem_range = []
    redem_pay = []

    for i in redem_dataframe:
        if '년' in i : redem_date.append( i[ i.find(':') + 1 : ].strip() )   
        if '상환조건' in i: redem_range.append( re.sub(r'[^0-9.%]', '', i) )
        if '상환금액' in i: 
            try: redem_pay.append( i[ i.index('+') + 1 : i.rindex('%') + 1 ].strip() )
            except: redem_pay.append( str(float(re.sub(r'[^0-9]', '', i)) - 100) + '%' )

    # 리자드 num, pay, barrier
    lizard_num = []
    lizard_pay = []
    lizard_barrier = []
    for i in redem_dataframe: 
        if '하락한 적이' in i:
            lizard_num.append( i[ i.find('차') - 1 ] )
            lizard_barrier.append(  re.sub(r'[^0-9%.]', '', i[ i.rfind('차') + 1 : ])  )

    if lizard_barrier != []:
        for i in lizard_num:
            lizard_pay.append(redem_pay[int(i)])
            del redem_pay[int(i)]
            del redem_range[int(i)]
            del redem_date[int(i)]
    else: 
        lizard_num = ''
        lizard_pay = ''
        lizard_barrier = ''

    # 만기 range, pay, barrier
    exp_dataframe = df.range_index('상환평가일(불포함)', 0, '최초기준가격 평가일:')

    exp_barrier = []
    for i in exp_dataframe:
        if '가.' in i: exp_range = re.sub(r'[^0-9.%]', '', i)[1:]  # exp_range
        if '액면금액' in i and '%' in i:                                                 # exp_pay
            try: exp_pay = i[ i.index('+') + 1 : i.rindex('%') + 1 ].strip() 
            except: exp_pay = str(float(re.sub(r'[^0-9]', '', i)) - 100) + '%' 
        if '하락한 적이' in i: exp_barrier.append( re.sub(r'[^0-9.%]', '', i)[1:] )                 # exp_barrier
        
    if exp_barrier == []: exp_barrier = ''
    else: exp_barrier = exp_barrier[0]

    redem_range.append(exp_range)
    redem_pay.append(exp_pay)

    # 먼슬리 coupon range, coupon pay, coupon date
    for Mtly in text:
        if '수익지급평가가격' in Mtly: break
        else: Mtly = ''

    if Mtly != '':
        Mtly_dataframe = df.range_index('수익지급평가가격', -1, '수익지급평가일(불포함)')

        coupon_range = re.sub(r'[^0-9.%]', '', Mtly_dataframe[0])
        coupon_pay = re.sub(r'[^0-9.%]', '', Mtly_dataframe[1])
        
        coupon_date = []    
        for i in Mtly_dataframe:
            if '년' in i: coupon_date.append(i)
        coupon_date.sort()

    else: 
        coupon_pay = ''
        coupon_range = ''
        coupon_date = ''

    # 상품구조
    if exp_barrier == '':
        if lizard_barrier != '': structure = 'StepDn_NoKI_[Lizard]'
        elif coupon_pay != '': structure = 'StepDn_NoKI_MtlyCpn'
        else: structure = 'StepDn_NoKI'
    else:
        if lizard_barrier != '': structure = 'StepDn_KI_[Lizard]' 
        elif coupon_pay != '': structure = 'StepDn_KI_MtlyCpn'
        else: structure = 'StepDn_KI'

    if ccy == 'USD' and 'Mtly' not in structure:
        coupon_date = redem_pay

    
    return df.data2list(title, issue_price, issue_date, exp_pay_date, ccy, structure, lag, underlying, 
                exp_pay, exp_barrier, redem_pay[0], lizard_num, lizard_pay, lizard_barrier, 
                coupon_pay, coupon_range, redem_range, base_date, exp_date, redem_date, coupon_date)