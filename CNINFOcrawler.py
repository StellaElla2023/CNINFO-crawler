#%%
import requests
import random
import os
import pandas as pd
from fake_useragent import UserAgent
import re
from time import sleep
from tqdm import tqdm

os.chdir(r".")
file_CODE = 'code.xlsx'
file_FRinfo = r'FRinfo.xlsx'
folder_PDFs = 'Annual Reports'

#%%
def main():
    global CODE 
    CODE = get_code(file_CODE)
    # dropout(file_CODE)   # add in delisted companies
    CODE = CODE[CODE['category']=='A股']  # focus on A shares
       
    crawler(file_FRinfo)   # crawl the singal file indentification, announcementId
    download(folder_PDFs)   # write down PDFs based on announcementId

#%%
def purify(code,data):
    comp = data[data['code'] == code]
    for index, file in comp.iterrows():
        if ('修' in file['file'] or '更' in file['file']) and file['file'].split('（')[0] in list(comp['file']):
            if '（' in file['file']:
                old = file['file'].split('（')[0]
            else:
                old = file['file'].split('(')[0]
            print(data[(data['code']==code) & (data['file']==old)])
            data = data.drop(data[(data['code']==code) & (data['file']==old)].index)
            comp = comp.drop(comp[(comp['code']==code) & (comp['file']==old)].index)
        if 'H' in file['file']:
            data = data.drop(data[(data['code']==code) & (data['file']==file['file'])].index)
            comp = comp.drop(comp[(comp['code']==code) & (comp['file']==file['file'])].index)  
            # print(data[(data['code']==code) & (data['file']==file['file'])])       

def crawler(file):
    global data
    if os.path.exists(file):
        data = pd.read_excel(file)
    else:
        data = pd.DataFrame(columns=['year','code','company','file','announcementId'])
    a = 0
    trial = 0
    while a < len(CODE):    
        if CODE.loc[a,'code'] in data.code.tolist():
            a += 1
            continue
        comp = CODE.loc[a]
        try:
            info = company(comp)
        except:
            info = company(comp)
        if len(info) == 0 and trial < 3:
            trial += 1
            #a -= 1
        else:
            data = pd.concat([data,info],ignore_index = True)
            a += 1
            trial = 0
            data.to_excel(file,index=False)
        sleep(random.random())
    # data = changeCode(data)
    data = data.drop_duplicates(subset=['announcementId'],keep='first')
    for code in CODE['code']:
        purify(code,data)
    data.to_excel(file,index=False)

def changeCode(data):    
    for index,row in CODE.iterrows():
        if row['code'] > 100000:
            continue
        indexls = data[data['company'].str.contains(row['zwjc'].split('*')[-1])].index
        indexls = indexls.append(data[data['code']==row['code']].index)
        names = set(list(data[data['code']==row['code']]['company']))
        for name in names:        
            indexls = indexls.append(data[data['company']==name].index)
        code = str(row['code'])
        while len(code) < 6:
            code = '0' + code
        for index in indexls:
            data.loc[index,'code'] = code
        
def getFailedCode(CODE,code):   
    if code in list(CODE['code']):
        # pass companies that already existing in the list
        return
    print(code)
    while len(str(code)) != 6:
        code = '0' + str(code)
    # set query
    url= 'http://www.cninfo.com.cn/new/information/topSearch/detailOfQuery'
    data = {
        'keyWord': code,
        'maxSecNum': '10',
        'maxListNum': '5'}
    # crawl
    for i in range(10):
        try:
            headers = {'User-Agent':str(UserAgent().random),
               'Referer':'http://www.cninfo.com.cn/new/fulltextSearch?notautosubmit=&keyWord={}'.format(code)}        
            response = requests.post(url, headers = headers,data=data)
            js = response.json()['keyBoardList']
            break
        except:
            sleep(random.random())
            continue
    if js == []:
        return
    item = pd.DataFrame(js)
    item['category'] = item['category'].apply(lambda x: x.strip())
    CODE = pd.concat([CODE,item],ignore_index=True)
    return
        
def dropout(file):
    for code in tqdm(range(1,4000)):
        getFailedCode(CODE,code)
    for code in tqdm(range(300000,301000)):
        getFailedCode(CODE,code)
    for code in tqdm(range(600000,602000)):
        getFailedCode(CODE,code)
    for code in tqdm(range(603000,604000)):
        getFailedCode(CODE,code)
    for code in tqdm(range(605000,605500)):
        getFailedCode(CODE,code)
    for code in tqdm(range(688000,688985)):
        getFailedCode(CODE,code)
    CODE.to_excel(file,index=False)

def company(comp):
    info = pd.DataFrame(columns=['year','code','company','file','announcementId'])
    print('----------------------{}----------------------'.format(comp['zwjc']))
    page = 1
    while True:
        singlePage = pageData(page,comp)
        if singlePage == 0:
            break
        
        for i in singlePage:
            try:
                year = re.findall(r'(\d+)',i['announcementTitle'])[0]
            except:
                year = 0
            # use keywords in the file name to delete irrelevant files
            delete = ['股东大会','延迟','指标','关于','提前','补充','摘要','半年','公告','取消','意见','董事','快报','英文','变更','债券','第','财务','工作','延期','制度','说明','的','制度','对','书','审计','回复','附件']

            for word in delete:
                if word in i['announcementTitle']:
                    break
            else:
                print(i['announcementTitle'])
                info.loc[len(info)] = [year,i['secCode'],i['secName'],i['announcementTitle'],i['announcementId']]
        page += 1
    print()
    return info

def pageData(page,comp):
    code = str(comp['code'])
    while len(code) != 6:
        code = '0' + code
    query_path= 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
    headers = {'User-Agent':str(UserAgent().random),
               #'Referer':'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&checkedCategory=category_ndbg_szsh'}
               'Referer':'http://www.cninfo.com.cn/new/disclosure/stock?stockCode={}&orgId={}'.format(code, comp['orgId'])}
    query= {'pageNum': str(page),                      
            'pageSize': '30',                                    
            'stock': '{},{}'.format(code, comp['orgId']),
            #'seDate': '{}-01-01~{}-01-01'.format(num,num+1),
            #'category':'category_ndbg_szsh'}
            'searchkey':'年度报告'}
    response= requests.post(query_path,headers = headers,data = query)
    
    # if page is empty, all the information of the company has been crawled. Hence, set a flag as 0.
    try:
        js = response.json()['announcements']
        js[0]
        return js
    except:
        return 0

def download(folder):
    frame_download = 'http://www.cninfo.com.cn/new/announcement/download?bulletinId='
    for i in range(100):
        try:
            for index,row in data.iterrows():
                code = str(row["code"])
                while len(code) != 6:
                    code = '0' + code
                name= code+ '_' + row['company']+ '_' + row['file']+ '.pdf' 
                name = name.replace('*','').replace(' ','')
                filePath = os.path.join(folder,name)
                if not os.path.exists(folder):
                    os.mkdir(folder)
                if not os.path.isfile(filePath): 
                    download = frame_download + str(row['announcementId'])       
                    header = {'User-Agent':str(UserAgent().random)}
                    r= requests.get(download,headers = header)
                    f= open(filePath, "wb")
                    f.write(r.content)
                    f.close()
                    sleep(random.random()*2)
                    print(name)
            else:
                break
        except:
            sleep(20)
            continue

def get_code(file):
    # get the linking table between A share codes and singal identity in the cninfo website
    if os.path.exists(file):
        CODE = pd.read_excel(file)
        return CODE
    
    urls = ['http://www.cninfo.com.cn/new/data/szse_stock.json',\
         'http://www.cninfo.com.cn/new/data/hke_stock.json',\
         'http://www.cninfo.com.cn/new/data/gfzr_stock.json']
    headers ={'User-Agent': UserAgent().random}
    codes = pd.DataFrame(columns = ['orgId','category','code','pinyin','zwjc'])
    
    for url in urls:
        js = requests.get(url, headers=headers).json()['stockList']    
        item = pd.DataFrame(js) 
        codes = pd.concat([codes,item])

    codes.to_excel(file,index=False)
    print('Code finish!')
    print()
    
    return codes    

#%% 
main()
