from imap_tools import MailBox
from imap_tools import A, AND, OR, NOT
import datetime
import os

#mailbox = MailBox('gw.fnpricing.com')
with MailBox('gw.fnpricing.com').login('deriva@fnpricing.com','Fnp2022!1',initial_folder='INBOX.deriva수신.ELS | DLS | DLB') as mailbox:
    Samsung_path = r'Y:\(0000)금융공학연구소\402.파생상품팀\파생상품팀 이슈\06. Live\20210517_삼성증권 Live'
    path=fr'{Samsung_path}\텀싯_2110이후'
    os.chdir(path)

    for msg in mailbox.fetch(A(date=datetime.date.today()),reverse=True):
        #print(msg.date.strftime('%Y-%m-%d'),msg.from_,msg.subject)
        if '[삼성증권] ELS발행리스트' in msg.subject:
            for att in msg.attachments:
                if 'application/octet-stream' in att.content_type:
                    if att.filename.endswith('xls') or att.filename.endswith('xlsx'):
                        with open(fr'{Samsung_path}\발행리스트\{att.filename}','wb') as f:
                            f.write(att.payload)  
                    elif '최종' in att.filename:
                        with open(fr'{Samsung_path}\최종텀싯_2110이후\{att.filename}','wb') as f:
                            f.write(att.payload)    
                    else:
                        with open(att.filename,'wb') as f:
                            f.write(att.payload)
