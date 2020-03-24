#套件
import pandas as pd
from pandas.core.frame import DataFrame
import csv
import locale
from os import listdir
from os.path import isfile, isdir, join
import codecs,locale,chardet
import math
import re
locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' ) 

def FindAllFiles(mypath): #自動尋找資料夾的所有檔案（會避開資料夾）回傳list
    # 指定要列出所有檔案的目錄
    files = listdir(mypath)
    # 取得所有檔案與子目錄名稱
    all_file = []
    # 以迴圈處理
    for f in files:
        # 產生檔案的絕對路徑
        fullpath = join(mypath, f)
        # 判斷 fullpath 是檔案還是目錄
        if isfile(fullpath):
            all_file.append(fullpath)
    return all_file #回傳絕對路徑 格式為list

num_dict = {"一": "1", "二": "2", "三": "3", "四": "4", 
"五": "5", "六": "6", "七": "7", "八": "8", "九": "9", "十": ""}

def tran(str_num):#國字數字轉阿拉伯數字
    num = ""
    str_num = str_num[:-1]
    if str_num[0] == "十":
        if len(str_num) == 1:
            num_dict["十"] = "10"
        else:
            num_dict["十"] = "1"
    else:
        if len(str_num) == 2:
            num_dict["十"] = "0"
        else:	
            num_dict["十"] = ""
    # 以字典的放式對中文數字作轉換
    for str in str_num:
        for key in num_dict:
            if key == str:
                num += num_dict[key]
                break
    return int(num)

#讀取檔案路徑並重新排列（macos會有.DS_Store隱藏物件需要做剔除）
#File_list = sorted(FindAllFiles("/Users/hayashishiken/Desktop/國泰數據處理面試/data")) #windows
File_list = sorted(FindAllFiles("/Users/hayashishiken/Desktop/國泰數據處理面試/data")[1:])#macos
name_list = ["df_a","df_b","df_e","df_f","df_h"]#設定變數名稱
df = []
for i in range(len(File_list)):#分別建立dataframe變數
    locals()[name_list[i]] =pd.read_csv(File_list[i]).drop(index = [0]) #去除不要的欄位
    df.append(locals()[name_list[i]]) #字串轉換成變數名稱並賦予值
all_df = pd.concat(df,axis=0, ignore_index=True)
new_df = all_df.dropna(subset=["總樓層數"])#去掉總樓層數為空值的欄位好作後續的處理
new_df.reset_index(drop=True, inplace=True)#重新賦予index
new_df["總樓層數"] = new_df["總樓層數"].apply(lambda x:tran(x))#轉換樓層的值

#filter_a.csv
condition1 = new_df["主要用途"] == "住家用" #撰寫條件
condition2 = new_df["建物型態"].str.contains("住宅大樓")
condition3 = new_df["總樓層數"] >= 13
final_df = new_df[(condition1 & condition2 & condition3)]
final_df.to_csv("filter_a.csv", encoding="utf-8")#儲存為csv檔

#filter_b.csv
total = len(all_df)#計算總件數
#計算總車位數、總價元、車位總價數
parking_spaces = all_df['交易筆棟數'].str.split('車位',expand=True)[1].astype('int64').sum()
avg_price = all_df['總價元'].astype('int64').mean()
avg_cars_price = sum(all_df['車位總價元'].astype('int64'))/parking_spaces
    
filter_b_df = pd.DataFrame(
    {"總件數":total,
     "總車位數":parking_spaces,
     "平均總價元":avg_price,
     "平均車位總價元":avg_cars_price
    }, index=[0], columns= ["總件數","總車位數","平均總價元","平均車位總價元"] )
filter_b_df.to_csv("filter_b.csv", encoding="utf-8")#儲存為csv檔


