import cv2
import openpyxl
import pytesseract
import datetime
import numpy as np
import re

def thresholding(image):
    return cv2.threshold(image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]

def remove_noise(image):
    return cv2.blur(image,(2,2)) 
    
def close(image):
    kernel = np.ones((1,1),np.uint8)
    return cv2.morphologyEx(image, cv2.MORPH_CLOSE, kernel)

def dilation(image):
    kernel = np.ones((1,1),np.uint8)
    return cv2.dilate(image, kernel, iterations = 1)

# Data list
def divide_list(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]    

now = datetime.datetime.now() 
sysdate = now.strftime('%y%m%d')

file = r'C:\Users\shhwang2\Desktop\Vol Surface\변동성곡면 데이터_10Y.xlsx'
save_file = fr'C:\Users\shhwang2\Desktop\Vol Surface\{sysdate}\{sysdate}_데이터.xlsx'
wb = openpyxl.load_workbook(file)

for file_num in range(1,9):
    img = cv2.imread(fr'C:\Users\shhwang2\desktop\Vol Surface\{sysdate}\00{file_num}.png', cv2.IMREAD_GRAYSCALE)

    image_binary = thresholding(img)
    image_noise = remove_noise(image_binary)
    image_dilate = dilation(image_noise)
    image_close = close(image_dilate)
    
    pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract'
    raw_data = pytesseract.image_to_string(image_close, lang=None, config='-c preserve_interword_spaces=1 --psm 6').strip().replace(',','.').replace('\n',' ').split(' ')
    raw_data = re.sub(r"[^0-9,]", '', str(raw_data)).split(',')
    raw_data = list(filter(None, raw_data))

    try:
        raw_data = list(map(int, raw_data))
    except Exception as e:
        print(fr'{file_num}.png :', e)

    if len(raw_data) != 135:
        print(fr'{file_num}.png : 개수 체크 필요!, {len(raw_data)}')

    data_list = list(divide_list(raw_data, 9))

    if file_num == 1:
        Sheet = 'KOSPI2'
    elif file_num == 2:
        Sheet = '005930'
    elif file_num == 3:
        Sheet = '105560'
    elif file_num == 4:
        Sheet = '000660'
    elif file_num == 5:
        Sheet = '005380'
    elif file_num == 6:
        Sheet = '035420'
    elif file_num == 7:
        Sheet = 'KOSPI_60'
    elif file_num == 8:
        Sheet = 'KOSPI_140'
    ws = wb[Sheet]

    ws.delete_rows(1,20)
    for row in data_list:
        ws.append(row)
    print(f'{Sheet} Sheet Save!')

wb.save(save_file)
