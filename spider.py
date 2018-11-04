#-*- coding: UTF-8 -*-

import sys
import os
import time
import urllib.request,urllib.error,urllib.parse
from urllib.parse import urlencode,urlparse
from urllib.request import Request,urlopen,quote
import numpy as np
import json
import http.cookiejar
import pprint, pickle
import pandas as pd
import time



def spider_get(url,params):
    """
    此函数主要是构造网页头，获取指定url的回应。
    url:传递链接地址
    params:相应参数
    """
    params = urllib.parse.urlencode(params)
    params=bytes(params,'utf8')
    url1=url
    try:  
        request = Request(url1)
        request.add_header('Accept',"application/json, text/javascript, */*; q=0.01")
        request.add_header('Accept-Encoding',"gzip, deflate")
        request.add_header('Connection',"keep-alive")
        request.add_header('Content-Type', "application/x-www-form-urlencoded; charset=UTF-8")
        request.add_header("Cookie","_ulta_id.CM-Prod.e9dc=a945893c01c22fe2;_ulta_ses.CM-Prod.e9dc=afd85e03ece9757f;lkNrr3VQZJ=MDAwM2IyNTRjZDAwMDAwMDAwNDUwUEpoMBAxNTQwNDQxOTgx")
        request.add_header('Host',"www.chinamoney.com.cn")
        request.add_header('Origin',"http://www.chinamoney.com.cn")
        request.add_header('Referer', "http://www.chinamoney.com.cn/chinese/jgxxpl/?entyDefinedCode=300868")
        request.add_header('User-Agent', "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36")
        request.add_header('X-Requested-With',"XMLHttpRequest")
        respose = urllib.request.urlopen(request,params).read()

        data = json.loads(respose)

        return data
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        print (e)


#Some User Agents
hds=[{'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
{'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'}]


def deal_spider(item_tag,item_name,item_type,data):
    """
    根据爬取类型分别处理
        :param item_tag: 爬取实体编号
        :param item_name: 爬取实体名
        :param item_type: 爬取类型
        :param data: 参数
    """
    # num=int(data["data"]["total"])
    if((item_type=="重大事项" ) or (item_type=="财务报告") or (item_type=="评级报告")):
        ref=[[item_tag,item_name,item_type,d["releaseDate"].split()[0],d["title"],"http://www.chinamoney.com.cn/dqs/cm-s-notice-query/fileDownLoad.do?contentId="+d["contentId"]+"&priority=0&mode=open"] for d in data["data"]["resultList"]]
        df = pd.DataFrame(ref, columns=['entyDefinedCode', 'entyFullName',"公告类型","发布日期","公告标题","下载地址"])
        return df
    else:
        """
        对于除重大事项等三项以外的类型要再次提交获取数据
        """
        df = pd.DataFrame(columns=['entyDefinedCode', 'entyFullName',"公告类型","发布日期","债券全称","公告标题","下载地址"])
        for t in data["data"]["resultList"]:
            # print (t["bondDefinedCode"])
            ztitle=t["bondFullName"]
            params = {
                "secondType": "0601,0602,0701,1001,1103,1104,1301,1302,1303,1304,1801,1802,1901,1902,2001,2002",
                "bondDefinedCode": t["bondDefinedCode"],
                "publishCode": 1
            }
            url3="http://www.chinamoney.com.cn/ags/ms/cm-u-notice-md/PublishInfoContentList"
            others=spider_get(url3,params)
            # print (others)
            
            ref1=[[item_tag,item_name,item_type,d["releaseDate"].split()[0],ztitle,d["title"],"http://www.chinamoney.com.cn/dqs/cm-s-notice-query/fileDownLoad.do?contentId="+d["contentId"]+"&priority=0&mode=open"]  for d in others["data"]["resultList"] if ((d["suffix"]!=None) and (d["attSize"]==1))]

            ref2=[[item_tag,item_name,item_type,d["releaseDate"].split()[0],ztitle,d["title"],"http://www.chinamoney.com.cn"+d["draftPath"]]  for d in others["data"]["resultList"] if d["suffix"]==None]
            ref3=[]
            for d in others["data"]["resultList"]:
                if ((d["suffix"]!=None) and (d["attSize"]>1)):
                    for i in range(0,10):
                        url4="http://www.chinamoney.com.cn/dqs/cm-s-notice-query/fileDownLoad.do?mode=save&contentId="+d["contentId"]+"&priority="+str(i)
                        try:  
                            request = Request(url4)
                            request.add_header('Accept',"application/json, text/javascript, */*; q=0.01")
                            request.add_header('Accept-Encoding',"gzip, deflate")
                            request.add_header('Connection',"keep-alive")
                            request.add_header('Content-Type', "application/x-www-form-urlencoded; charset=UTF-8")
                            request.add_header("Cookie","_ulta_id.CM-Prod.e9dc=a945893c01c22fe2;_ulta_ses.CM-Prod.e9dc=afd85e03ece9757f;lkNrr3VQZJ=MDAwM2IyNTRjZDAwMDAwMDAwNDUwUEpoMBAxNTQwNDQxOTgx")
                            request.add_header('Host',"www.chinamoney.com.cn")
                            request.add_header('Origin',"http://www.chinamoney.com.cn")
                            request.add_header('Referer', "http://www.chinamoney.com.cn/chinese/jgxxpl/?entyDefinedCode=300868")
                            request.add_header('User-Agent', "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36")
                            request.add_header('X-Requested-With',"XMLHttpRequest")
                            respose = urllib.request.urlopen(request)
                            data = respose.getheader("Content-Disposition") 
                            if (data!=None):
                                data=data.split("\"",maxsplit=2)[1]
                                data=data.encode('latin1').decode('utf8').split(".")[0]
                                ref3.append([item_tag,item_name,item_type,d["releaseDate"].split()[0],ztitle,data,url4])
                        except (urllib.error.HTTPError, urllib.error.URLError) as e:
                            print (e)
            ref2.extend(ref1)
            ref2.extend(ref3)
            # print(ref2)
            # ref=ref.update([[item_tag,item_name,item_type,d["releaseDate"],d["title"], for d in data["data"]["resultList"] if d["suffix"]==None])
            cox = pd.DataFrame(ref2, columns=['entyDefinedCode', 'entyFullName',"公告类型","发布日期","债券全称","公告标题","下载地址"])
            df=pd.concat([df,cox],axis=0, ignore_index=True)
        return df


def item_spider(item_tagt):
    """
        :param item_tagt: 实体号
    """ 
    item_tag=item_tagt
    item_tag=305930
    pageSize=2000;
    try_times=0
    url1='http://www.chinamoney.com.cn/ags/ms/cm-u-notice-md/ReportAndNotice'
    url2='http://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/SecondBondListByBondType'
    cookie = http.cookiejar.CookieJar()
    handler = urllib.request.HTTPCookieProcessor(cookie)
    opener = urllib.request.build_opener(handler)
    urllib.request.install_opener(opener)

    url='http://www.chinamoney.com.cn/dqs/rest/cm-s-account/getUT'

#   获取 UT
    try:
        req = Request(url, headers=hds[0])
        source_code = urllib.request.urlopen(req).read()
        # print(source_code)
        data = json.loads(source_code)
        ut=data["data"]["UT"]
        ts=data["head"]["ts"]

    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        print (e)


# 获取实体名称

    fxtype=[["zqpj","中期票据",100010],["dqrzq","短期融债券",100006],["qyz","企业债",100004],["tycd","同业存单",100041],["zczczq","资产支持证券",999999],["fgkdxzwrzgj","非公开定向债务融资工具",100050],["ejzbgj","二级资本工具",100054],["cdqrzq","超短期融资券",100029],["ptjrz","普通金融债",100007],["dfzfz","地方政府债",100011],["decd","大额存单",100058],["xmsyzq","项目收益债券",100057],]

    item_name=None
    fx = pd.DataFrame(columns=['entyDefinedCode', 'entyFullName',"公告类型","发布日期","债券全称","公告标题","下载地址"])
    for t in fxtype:
        params = {
        "secondType": "0601,0602,0701,1001,1103,1104,1301,1302,1303,1304,1801,1802,1901,1902,2001,2002",
        "bondTypeCode": t[2],
        "entyDefinedCode": item_tag,
        "issueStartYear": "all",
        "publishCode": 1,
        "pageNo": 1,
        "pageSize": pageSize,
        }    
        fxxx=spider_get(url2,params)
        # print (fxxx)
        if(fxxx["data"]["total"]!=0):
            if (fxxx["data"]["resultList"][0]["bondFullName"].split("2", maxsplit=1)[0]!=None):
                item_name=fxxx["data"]["resultList"][0]["bondFullName"].split("2", maxsplit=1)[0]
                print(item_name)
                break
        else:
            continue

    if (item_name==None):
        params = {
        "entyDefinedCode": item_tag, 
        "publishCode":1,
        "secondType": 1002,
        "pageNo":1,
        "pageSize":pageSize, 
        "ut": ut
        }
        zdsx=spider_get(url1,params)
        if(zdsx["data"]["total"]!=0):
            # print(zdsx["data"]["resultList"][0]["title"].split("2", maxsplit=1))
            if (zdsx["data"]["resultList"][0]["title"].split("2", maxsplit=1)[0]!=None):
                item_name=zdsx["data"]["resultList"][0]["title"].split("2", maxsplit=1)[0]
                print(item_name)

# 获取中期票据等相关材料

    for t in fxtype:
        params = {
        "secondType": "0601,0602,0701,1001,1103,1104,1301,1302,1303,1304,1801,1802,1901,1902,2001,2002",
        "bondTypeCode": t[2],
        "entyDefinedCode": item_tag,
        "issueStartYear": "all",
        "publishCode": 1,
        "pageNo": 1,
        "pageSize": pageSize,
        }    
        fxxx=spider_get(url2,params)
        fxxx=deal_spider(item_tag, item_name,t[1],fxxx)
        fx=pd.concat([fx,fxxx],axis=0, ignore_index=True)

# 获取重大事项
    params = {
    "entyDefinedCode": item_tag, 
    "publishCode":1,
    "secondType": 1002,
    "pageNo":1,
    "pageSize":pageSize, 
    "ut": ut
    }

    zdsx=spider_get(url1,params)
    # print(zdsx)
    zdsx=deal_spider(item_tag, item_name,"重大事项",zdsx)

# 获取财务报告

    params = {
    "entyDefinedCode": item_tag, 
    "publishCode":1,
    "secondType": "0901",
    "pageNo":1,
    "pageSize":pageSize, 
    "ut": ut
    }

    cwbg =spider_get(url1,params)
    # print(zdsx)
    cwbg=deal_spider(item_tag, item_name,"财务报告",cwbg)

# 获取评级报告

    params = {
    "entyDefinedCode": item_tag, 
    "publishCode":1,
    "secondType": "1105,1103",
    "pageNo":1,
    "pageSize":pageSize, 
    "ut": ut
    }

    pjbg=spider_get(url1,params)
    pjbg=deal_spider(item_tag, item_name,"评级报告",pjbg)

    final1=pd.concat([zdsx,cwbg,pjbg],axis=0, ignore_index=True)
    print(final1,fx)
    return final1,fx

def do_spider(item_tag_lists):
        """
    创建两个DataFrame分别存储
        :param item_tag_lists: 实体号列表
    """
    item_lists1=pd.DataFrame(columns=('entyDefinedCode', 'entyFullName',"公告类型","发布日期","公告标题","下载地址"))
    item_lists2=pd.DataFrame(columns=('entyDefinedCode', 'entyFullName',"公告类型","发布日期","债券全称","公告标题","下载地址"))
    for item_tag in item_tag_lists:
        print (item_tag)
        if(int(item_tag)%3==0):
            time.sleep(2)
        item_listt=item_spider(item_tag)
        item_list1=item_listt[0]
        item_list2=item_listt[1]
        item_lists1=pd.concat([item_lists1,item_list1],axis=0, ignore_index=True)
        item_lists2=pd.concat([item_lists2,item_list2],axis=0, ignore_index=True)
    pd.set_option('display.max_rows',2000)
    pd.set_option('display.max_colwidth',2000)
    # print (item_lists1,item_lists2)
    item_lists1.to_excel("11.xlsx")
    item_lists2.to_excel("12.xlsx")
    return item_lists1,item_lists2


if __name__=='__main__':
    with open("D:/zyxu/pandas/company_id_list.list", 'rb') as pkl_file:
        item_tag_lists = pickle.load(pkl_file)
    # pprint.pprint(item_tag_lists)
    item_lists=do_spider(item_tag_lists[100:101])
    