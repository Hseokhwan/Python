import os
import Method_All
from mirae import mirae_docx_A
from imap_tools import MailBox
from imap_tools import A, AND, OR, NOT
import datetime
import time

path = r'C:\Users\shhwang2\Desktop\python\Crawling\Mirae_Crawling'

#mailbox = MailBox('gw.fnpricing.com')
with MailBox('gw.fnpricing.com').login('deriva@fnpricing.com','Fnp2021!4',initial_folder='INBOX.deriva수신.ELS | DLS | DLB') as mailbox:
    os.chdir(path)

    for msg in mailbox.fetch(A(date=datetime.date.today()),reverse=True):         
        #print(msg.date.strftime('%Y-%m-%d'),msg.from_,msg.subject)
        if '사모계약서 송부' in msg.subject:
            for att in msg.attachments:
                if 'doc' in att.filename or 'pdf' in att.filename:
                    with open(fr'{path}\{att.filename}','wb') as f:
                        f.write(att.payload)  

time.sleep(5)

# Crawling
fd_list = os.listdir(path)

all_df = []

for file in fd_list:
    if 'docx' in file or 'pdf' in file:
        text = Method_All.read_file(fr'{path}\{file}')
        if '인수계약서' in file: 
            try: all_df.append(mirae_docx_A(text))
            except: pass

Method_All.save_csv(all_df, fr'{path}\{datetime.date.today()}.csv')





