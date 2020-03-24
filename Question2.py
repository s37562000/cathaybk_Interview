import requests
import pandas as pd
from pandas.core.frame import DataFrame
import numpy as np
import urllib
import re
from bs4 import BeautifulSoup
import csv
import smtplib
import time
import random
import json
from email.mime.text import MIMEText
from email.utils import formataddr
import pymongo
from pymongo import MongoClient
import pprint

#mongo_db setting
mongo_url = "mongodb://127.0.0.1:27017"
client = MongoClient(mongo_url)
db = client['591_rent']
collection = db["591_rent_data"]
#client.drop_database("591_rent") 刪除資料庫


headers = { #手機版本header
'User-Agent': 'Mozilla/5.0 (Linux; Android 8.1.0; ALP-AL00 Build/HUAWEIALP-AL00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/63.0.3239.83 Mobile Safari/537.36 T7/10.13 baiduboxapp/10.13.0.11 (Baidu; P1 8.1.0)'
}


#程式完成後用email提醒自己
def send_mail(message):
	EMAIL = '*************'
	PWD = '*************'

	sender = to = EMAIL
	text = message
	msg = MIMEText(text.encode('utf-8'), _charset='utf-8')
	msg['Subject'] = 'Python 程式提醒'
	msg['From'] = sender
	msg['To'] = to
	try:
		server = smtplib.SMTP_SSL('smtp.gmail.com', 465) #gmail的amtp設置
		server.ehlo()
		server.login(EMAIL, PWD)
		server.sendmail(sender, to, msg.as_string())
		server.close()
		print('Email 已寄出')
	except Exception as e:  
		print('Email 寄送失敗')
		print(e)

#取得每棟物件的id
def get_json(row,regionid):
	#手機版的url
    url = 'https://m.591.com.tw/mobile-list.html'
    #regionid: 台北市:1 新北市：3
    #傳入參數
    params = {
        'type': 'rent',
        'dropDown': 1,
        'version' : 2017,
        'firstRow': row,
        'regionid' : regionid,
        'region_id' : regionid,
        '_signature': 'NDA2MzU0Y2EyYzUzMjMzNmQ2NDAwZGEyZjBlYzljZmE=',
        '_timestamp': '1584687616',
        '_randomstr': 'dd7kTpsG',
        '_appid': 'nqqpaGbhpu'
    }
    
    resp = requests.get(url, params = params ,headers=headers)#傳入參數
    resp.encoding = 'utf-8'
    total_rows = resp.json()['totalRows']#第一個參數為總筆數
    json = resp.json()['data'] #回傳json資料回來
    
    return total_rows,json     

#取得每棟物件的詳細資料
def get_data(county): #從頁面獲取後面含有id的資料回來
    total_rows = get_json(0,county)[0]#獲取總筆數，後面用來設置for迴圈用
    county_dict = {"台北市":1, "新北市": 3} #用字典去獲取regionid，為了以後的擴充性
    try: #例外處理
        regionid = county_dict[county]
    except:
        print("county error")
    for row in range(0,total_rows,8): #每8筆為一個梯次
        data_df = pd.DataFrame(get_json(row,regionid)[1])
        houseid = data_df['houseid']#獲取一個梯次的每個物件的id
        print(row)
        for h_id in houseid:
            xhr_url = 'https://m.591.com.tw/iphone-houseRecordNew.html'
            try: #用物件id獲取詳細資料回來
                new_data = requests.get(xhr_url, params = {'id' : h_id} ,headers=headers)
                new_json = new_data.json()['data'] #獲取json
                if type(new_json) == type({}):
                    collection.insert_one(new_json) #如果是正確的格式就存到mongoDB
                    print(h_id)
            except:
                print(h_id+" error") 
                send_mail(h_id+" error") #如果是錯誤的格式就送出email提醒
            time.sleep(random.uniform(0,5)) #設置隨機秒數的等待

get_data("台北市")
