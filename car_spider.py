#!/bin/python
# -*- coding: utf-8 -*-
# Author: Syman Li
# @Time : 2019-05-27 10:45
import requests
import oss2
import itertools
from urllib import parse
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

requests.adapters.DEFAULT_RETRIES = 12

MYSQL_CONF = {
    'user': 'root',
    'password': 'xxxx',
    'host': '192.168.21.215',
    'database': 'spider_db',
    'port': 3306
}

OSS_AccessKeyId='LTAIk01111J2f9x'
OSS_AccessKeySecret='11111aaaaaaaaaaaaaaa'

BASE_URL = 'https://car.autohome.com.cn'
BRAND_ICON_URL='https://car.m.autohome.com.cn/jingxuan/brand/index.html'
BRAND_URL='https://car.autohome.com.cn/price/brand-%s.html'
SERIES_URL='https://car.autohome.com.cn/price/series-%s.html'
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36"}


engine = create_engine(
    "mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s?charset=utf8" % MYSQL_CONF,
    encoding='utf-8', pool_recycle=3600, pool_size=10
)

def brand_fetch():
    '''
    获取汽车之家品牌信息
    :return:
    '''
    response=requests.get(BRAND_ICON_URL,headers=HEADERS)
    soup = BeautifulSoup(response.text,'html.parser')
    brand_list = soup.find(name='div',attrs={'id':'div_ListBrand'}).find_all(name='li')
    brand_list_old = engine.execute('select brand_id from brand_info')
    brand_id_proxy = set(brand_id[0] for brand_id in brand_list_old)
    for brand in brand_list:
        brand_id = brand['v']
        brand_name = brand.find(name='strong').text
        brand_logo_url = brand.find(name='img').get('src') if  brand.find(name='img').get('src')  else  brand.find(name='img').get('data-src')
        brand_logo_url = 'http:' + brand_logo_url
        brand_item = {
            'brand_id': int(brand_id),
            'brand_name': brand_name,
            'brand_logo_url': brand_logo_url,
        }
        if brand_item['brand_id'] not in brand_id_proxy:
            oss_logo_url = brand_ico_upload(brand_id, brand_logo_url)
            brand_item['oss_logo_url'] = oss_logo_url
            engine.execute(
                "insert into brand_info (brand_id,brand_name,brand_logo_url) values(%(brand_id)s, %(brand_name)s, %(oss_logo_url)s)",
                brand_item
            )
            print('上传完成！')
def brand_ico_upload(brand_id,brand_logo_url):
    '''
    获取汽车之家品牌LOGO数据流上传到阿里云OSS
    :return:
    '''
    auth = oss2.Auth(OSS_AccessKeyId, OSS_AccessKeySecret)
    bucket= oss2.Bucket(auth, 'oss-cn-shanghai.aliyuncs.com', 'phantom-test-oss')
    logo_filename = brand_logo_url.split('/')[-1]
    down_logofile = requests.get(brand_logo_url)
    upload = bucket.put_object('resource/brand_logo/'+logo_filename,down_logofile)
    return parse.unquote(upload.resp.response.url)

def fetch_series():
    '''
    获取汽车之家车型数据
    :return:
    '''
    print('***启动车系数据抓取***')
    brand_info = engine.execute('select brand_id,brand_name from brand_info')
    brand_detail = list(set((item[0],item[1]) for item in brand_info))
    series_data = []
    for brand in brand_detail:
        brand_id = brand[0]
        brand_name = brand[1]
        try:
            response = requests.get(BRAND_URL % str(brand_id), headers=HEADERS)
            soup = BeautifulSoup(response.text, 'html.parser')
            # series_list = soup.find_all_next(name='div', attrs={'class': 'list-dl-text'})
            series_list = soup.select('.list-dl-text a')
            for series_item in series_list:
                series_id = series_item['href'].split('-')[1].split('.')[0]
                series_name = series_item['title'].split('(')[0].strip()
                series_url = SERIES_URL % str(series_id)
                series_item = {
                    'brand_id':brand_id,
                    'brand_name':brand_name,
                    'series_id':series_id,
                    'series_name':series_name,
                    'series_url':series_url,
                }
                if series_item not in series_data:
                    series_data.append(series_item)
                else:
                    print('发现重复项',series_item)
        except Exception as e:
            print(brand_id,'获取异常!')
            brand_detail.append(brand)
            continue
    print('***车系数据抓取完成***')
    return series_data
def fetch_series_detail():
    '''
    获取汽车之家车系数据(包含在售、停售，不包含预售)
    :return:
    '''
    series_data = fetch_series()
    #series_data = [ {'brand_id': 75, 'brand_name': '比亚迪', 'series_id': '489', 'series_name': '比亚迪S8', 'series_url': 'https://car.autohome.com.cn/price/series-489.html'},]
    print('***启动抓取车型数据***')
    series_detail = []
    old_detail_id = engine.execute("select CONCAT(brand_name,detail_id) AS old_brid from vehicle_info")
    all_detail_id = set(result['old_brid'] for result in old_detail_id)
    excloud_key = ['免税', ]
    for series_item in series_data:
        series_id = series_item['series_id']
        series_url = series_item['series_url']
        try:
            # 抓取在售车型
            response = requests.get(series_url, headers=HEADERS)
            soup = BeautifulSoup(response.text, 'html.parser')
            data = soup.select('.interval01-list-cars-infor p a')
            data_endsales = soup.select('.tab-nav ul li a')
            excloud_key = ['免税',]
            # 抓取停售车型
            for i in data_endsales:
                if i.text in '停售':
                    endsales_url=BASE_URL + i['href']
                    response = requests.get(endsales_url, headers=HEADERS)
                    soup=BeautifulSoup(response.text, 'html.parser')
                    data+=(soup.select('.interval01-list-cars-infor p a'))
                    page_data = soup.select('.price-page02 .page a')
                    #抓取停售分页数据
                    if page_data:
                        temp_url = set(url['href'] for url in page_data)
                        for url in temp_url:
                            if 'series' in url:
                                response = requests.get(BASE_URL + url , headers=HEADERS)
                                soup = BeautifulSoup(response.text, 'html.parser')
                                data += (soup.select('.interval01-list-cars-infor p a'))
            data=set(data)
            for item in data:
                if item.text in excloud_key:
                    continue
                detail_id = item['href'].split('/')[4]
                detail_name = item.text.strip()
                detail_info_url = 'http:' + item['href'].split('#')[0]
                series_item['detail_id'] = detail_id
                if series_item['brand_name'] + detail_id not in all_detail_id:
                    print('新增车型数据：',series_item)
                    series_item['detail_id'] = detail_id
                    series_item['detail_name'] = detail_name
                    series_item['detail_info_url'] = detail_info_url
                    engine.execute(
                        "insert into vehicle_info (brand_id,brand_name,brand_name_ext,series_id,series_name,series_name_ext,series_url,detail_id,detail_name,detail_name_ext,detail_info_url) values \
                        (%(brand_id)s,%(brand_name)s,%(brand_name)s,%(series_id)s,%(series_name)s,%(series_name)s,%(series_url)s,%(detail_id)s,%(detail_name)s,%(detail_name)s,%(detail_info_url)s)",
                        series_item
                    )
        except Exception as e:
            print(series_id,series_url,'获取异常!',e)
            series_data.append(series_item)
            continue
    print('***车型数据抓取完成***')
if __name__ == "__main__":
    brand_fetch()
    fetch_series_detail()
