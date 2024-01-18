import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
from babel.numbers import format_currency
import numpy as np

import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host='206.189.96.57',
                                         database='platform',
                                         user='PlatBI',
                                         password='BIAIPass!2019204PurumPum')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)

Depositors_1k = pd.read_sql_query("with base as ( \
select customer_fk, merchant_fk, (amount/rate_to_eur) as amount_euro,\
currency_fk,date(c_date) as txn_date  from platform.customer_transactions as a \
left join (select distinct id, rate_to_eur from platform.currencies where is_valid = 1) as b \
on a.currency_fk = b.id \
where trx_type = 'DEPOSIT' \
and status in ('APPROVED','SUCCESSFUL')), \
\
base_2 as ( \
select customer_fk, txn_date, sum(amount_euro) as deposits from base \
group by 1,2), \
\
base_3 as ( \
select customer_fk, txn_date,deposits, \
sum(deposits) over ( PARTITION by customer_fk order by txn_date asc ) as total_dpst from base_2), \
\
base_4 as ( \
select *, ROW_NUMBER()over(PARTITION by customer_fk order by txn_date asc) as 750_date  from base_3 \
where total_dpst >= 750) \
\
select a.customer_fk, c.name as merchant_name,d.country_desc as country, txn_date as date_of_reaching_750, \
case when b.email like '%blocked%' then 1 else 0 end as is_blocked from base_4 as a \
left join platform.customers as b \
on a.customer_fk = b.id \
left join platform.merchants as c \
on b.merchant_fk = c.id \
left join platform.countries d \
on b.country_fk = d.id \
where 750_date = 1 \
order by txn_date", con=connection)


date = dt.datetime.today()-  timedelta(1)
date_1 = date.strftime("%m-%d-%Y")
filename = f'VIP_Customers_list_{date_1}.xlsx'

with pd.ExcelWriter(filename) as writer:
    Depositors_1k.reset_index(drop=True).to_excel(writer, sheet_name="VIP_List",index=False)


sub = f'VIP_Customers_list - {date_1}'


#!/usr/bin/python
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

def send_mail(send_from,send_to,subject,text,server,port,username='',password=''):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ', '.join(recipients)
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text))

    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(filename, "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename={filename}')
    msg.attach(part)

    #context = ssl.SSLContext(ssl.PROTOCOL_SSLv3)
    #SSL connection only working on Python 3+
    smtp = smtplib.SMTP_SSL(server, port)
    smtp.login(username,password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()
    
subject = sub
body = f"Hi,\n\n Attached contains list of VIP customers as of {date_1}\n\nThanks,\nSaketh"
sender = "sakethg250@gmail.com"
recipients = ["alberto@crystalwg.com","saketh@crystalwg.com",'marcos@crystalwg.com','anna.penkova@1clickgames.com','erika@crystalwg.com','sandra@crystalwg.com','Ron@crystalwg.com','lina.betcoco@gmail.com']
password = "xjyb jsdl buri ylqr"
send_mail(sender, recipients, subject, body, "smtp.gmail.com", 465,sender,password)



