'''
url分析：
1、官网链接：https://www.lianjia.com/
2、分城市链接：https://gz.lianjia.com/   ---此处以广州为例
3、分类别链接：https://gz.lianjia.com/zufang/  ---此处以租房为例
4、筛选项链接：https://gz.lianjia.com/zufang/tianhe/rt200600000001/  ---此处筛选天河区及整租类型
5、页面以3000套为限，此处再筛选限制一个循环在3000套以内，此处筛选面积：
https://gz.lianjia.com/zufang/tianhe/rt200600000001ra0/
6、翻页：https://gz.lianjia.com/zufang/tianhe/pg3rt200600000001ra0/
即，最终的目标url为：
https://城市.lianjia.com+/租房/+/区域/+pg+出租方式+面积/
*** 此处我想爬取广州整租的房源信息，经分析页面，按区+面积筛选可以做到每页都在3000以内，故，爬取策略为：
step 1:从 https://gz.lianjia.com/zufang/rt200600000001/ 入手，获取每个区(同步操作）
step 2:区+面积 构造新的url: https://gz.lianjia.com/zufang/{区}/rt200600000001{ra+面积类型编码}/  (同步操作)
step 3:构造翻页url: https://gz.lianjia.com/zufang/{区}/{pg+页码}rt200600000001{ra+面积类型编码}/ (同步操作)
step 4:获取每一个房子的url,并生成列表 (异步操作)
step 5:从step 4的列表中访问每个房子的url获取信息 (异步操作)
'''

import requests
import random
import asyncio
import aiohttp
import time
from bs4 import BeautifulSoup
import re
import pandas as pd
import csv
from aiohttp import TCPConnector
from lxml import html
etree = html.etree


# 设置爬虫头随机，随机获取浏览器用户信息
def get_ua():
    ua_list = [
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)',
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
        'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
        'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11',
        'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11',
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)',
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)',
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)',
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)',
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)',
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)',
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
        'Mozilla/5.0 (Linux;u;Android 4.2.2;zh-cn;) AppleWebKit/534.46 (KHTML,likeGecko) Version/5.1 Mobile Safari/10600.6.3 (compatible; Baiduspider/2.0;+http://www.baidu.com/search/spider.html)',
        'Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5',
        'Mozilla/5.0 (iPod; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5',
        'Mozilla/5.0 (iPad; U; CPU OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5',
        'Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1',
        'MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1',
        'Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10',
        'Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13',
        'Mozilla/5.0 (BlackBerry; U; BlackBerry 9800; en) AppleWebKit/534.1+ (KHTML, like Gecko) Version/6.0.0.337 Mobile Safari/534.1+',
        'Mozilla/5.0 (hp-tablet; Linux; hpwOS/3.0.0; U; en-US) AppleWebKit/534.6 (KHTML, like Gecko) wOSBrowser/233.70 Safari/534.6 TouchPad/1.0',
        'Mozilla/5.0 (SymbianOS/9.4; Series60/5.0 NokiaN97-1/20.0.019; Profile/MIDP-2.1 Configuration/CLDC-1.1) AppleWebKit/525 (KHTML, like Gecko) BrowserNG/7.1.18124',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0; HTC; Titan)',
        'UCWEB7.0.2.37/28/999',
        'NOKIA5700/ UCWEB7.0.2.37/28/999',
        'Openwave/ UCWEB7.0.2.37/28/999',
        'Mozilla/4.0 (compatible; MSIE 6.0; ) Opera/UCWEB7.0.2.37/28/999']
    agent = random.choice(ua_list)
    return agent


# 获取每个区
def get_area_list():
    response = requests.get(cur_url, headers=header)
    response.encoding = 'utf-8'
    tree = etree.HTML(response.text)
    area_list = []
    # 获取所有符合条件的li标签
    for i in range(2, 13):  # 此处13为手工数的区的个数，后面可计算li标签的个数自动获取个数
        all_li = tree.xpath('/html/body/div[3]/div[1]/div[4]/div[1]/ul[2]/li[{0}]/a/@href'.format(i))
        area_list.append(all_li[0].replace('/zufang/', '').replace('/', ''))
    response.close()
    return area_list


# 区+面积类型编码+整租编码+页码 构造每一页的url
def get_per_page(area):
    # 用于接收获取到的所有页面的url
    all_page_url = []
    for each_area in area:
        for ra in range(6):
            # 构造新的url，包含区跟面积的信息，并获取每个分类下面的总套数
            cur_url = bas_url + each_area + '/' + 'rt200600000001' + 'ra' + str(ra)
            res = requests.get(cur_url, headers=header)
            res.encoding = 'utf-8'
            soup = BeautifulSoup(res.text, 'html.parser')
            count_of_each_category = int(soup.find('span', class_='content__title--hl').text)
            # 计算每个类别下面的总页数,本来是+1的，+2是考虑到range包头不包尾
            page_count = int(count_of_each_category / 30) + 2
            # 构造包含页码在内的最新url
            for page in range(1, page_count):
                cur_url = bas_url + each_area + '/' + 'pg' + str(page) + 'rt200600000001' + 'ra' + str(ra)
                all_page_url.append(cur_url)
                print(each_area, '区---面积类型编码为', ra, '的房源，第', page, '页的url获取成功！')
            res.close()
    return all_page_url


# 同步，获取每个房源的url
def get_per_house_url(page_urls):
    house_urls = []
    for page_url in page_urls:
        time.sleep(random.randint(1, 3))
        response = requests.get(page_url, headers=header)
        response.encoding = 'utf-8'
        tree = etree.HTML(response.text)
        house_urls_of_per_page = tree.xpath('/html/body/div[3]/div[1]/div[5]/div[1]/div[1]/div/div/p[1]/a/@href')
        print(page_url, "页面的所有href值获取成功！")
        n = 1
        for house_url_of_per_page in house_urls_of_per_page:
            if house_url_of_per_page.startswith('/apartment'):
                print('获取到公寓，跳过')
                continue
            else:
                house_url = 'https://gz.lianjia.com' + house_url_of_per_page
                house_urls.append(house_url)
                print(page_url, '页面的第', n, '个房源url构造完成，且已保存到列表中...')
                n += 1
        response.close()
    return house_urls


# 异步协程对象，获取每个房源的信息
async def get_per_house_info(house_url):
    house_info = []
    async with SEMAPHORE:
        async with aiohttp.ClientSession(connector=TCPConnector(ssl=False)) as session:
            async with session.get(house_url, headers=header) as response:
                html = await response.text()
                tree = etree.HTML(html)
                # 以下获取各项信息
                # 输出网页上写的“房源验真编号”
                house_id = tree.xpath('/html/body/div[3]/div[1]/div[3]/div[1]/i/text()')[1].split('：')[1].strip()
                # 房源地址
                district_and_community = tree.xpath('/html/head/meta[10]/@content')[0].split('广州')[1].strip('房屋出租')
                district = district_and_community[0:2]
                community = district_and_community[2:]
                # 房源标题
                name = tree.xpath('/html/body/div[3]/div[1]/div[3]/p/text()')[0].split('·')[1].strip().split(' ')[0]
                # 房源维护时间
                maintenance_time = tree.xpath('/html/body/div[3]/div[1]/div[3]/div[1]/text()')[0].split('：')[1].strip()
                # 出租方式
                rental_menthon = tree.xpath('//*[@id="aside"]/ul/li[1]/text()')[0]
                # 格局
                layout = tree.xpath('//*[@id="aside"]/ul/li[2]/text()')[0].split(" ")[0]
                # 朝向
                orientation = tree.xpath('//*[@id="aside"]/ul/li[3]/span[2]/text()')[0].split(" ")[0]
                # 一大串房屋信息
                house_descrition = tree.xpath('/html/body/div[3]/div[1]/div[3]/div[3]/div[2]/div[3]/ul[1]')[0]
                # 面积
                house_area = house_descrition.xpath('//*[@id="info"]/ul[1]/li[2]/text()')[0].split('：')[1]
                # # 楼层
                floor = house_descrition.xpath('//*[@id="info"]/ul[1]/li[8]/text()')[0].split('：')[1]
                # # 电梯
                elevator = house_descrition.xpath('//*[@id="info"]/ul[1]/li[9]/text()')[0].split('：')[1]
                # # 车位
                parking = house_descrition.xpath('//*[@id="info"]/ul[1]/li[11]/text()')[0].split('：')[1]
                # # 用水
                water = house_descrition.xpath('//*[@id="info"]/ul[1]/li[12]/text()')[0].split('：')[1]
                # # 用电
                electricity = house_descrition.xpath('//*[@id="info"]/ul[1]/li[14]/text()')[0].split('：')[1]
                # #燃气
                gas = house_descrition.xpath('//*[@id="info"]/ul[1]/li[15]/text()')[0].split('：')[1]
                # 费用详情
                fee_details = tree.xpath('/html/body/div[3]/div[1]/div[3]/div[3]/div[2]/div[6]/div/div[2]/div/ul')[0]
                # # 付款方式
                pay_methon = fee_details.xpath('//*[@id="cost"]/div/div[2]/div/ul/li[1]/text()')[0]
                # # 月租
                rent_of_month = fee_details.xpath('//*[@id="cost"]/div/div[2]/div/ul/li[2]/text()')[0]
                # # 押金
                deposit = fee_details.xpath('//*[@id="cost"]/div/div[2]/div/ul/li[3]/text()')[0]
                # # 最近的地铁站及距离
                the_nearest_subway = tree.xpath('/html/body/div[3]/div[1]/div[4]/ul[2]/li/span[1]/text()')
                the_nearest_subway_distance = tree.xpath('/html/body/div[3]/div[1]/div[4]/ul[2]/li/span[2]/text()')
                # 经纬度,先用正则提取包含经纬度的字符串，再切割成经度及维度
                obj = re.compile('g_conf.coord =(?P<loc>.*?);', re.S)  # 预加载正则表达式
                longitude_and_latitude = obj.search(html).group('loc')
                longitude = longitude_and_latitude.split(',')[0].split(': ')[1].strip("'")
                latitude = longitude_and_latitude.rsplit(": ",1)[1].strip('}').strip().strip("'")
                # 房源链接
                house_link = house_url
                # 将所有信息组合
                per_house_info = [house_id,district_and_community,district,community,name, maintenance_time, rental_menthon, layout, orientation, house_area,
                                  floor, elevator, parking, water, electricity, gas, pay_methon, rent_of_month, deposit,
                                  the_nearest_subway, the_nearest_subway_distance, longitude, latitude,house_link]
                house_info.append(per_house_info)
                response.close()
        house_info_df = pd.DataFrame(house_info)
        house_info_df.to_csv('house_info.csv', mode='a', encoding='utf-8', index=False, header=False)
        print(num_semaphore,'个异步协程执行中：',name,'已爬取并写入文件，累计用时：',time.time()-start_time)
        t = random.randint(6,9)
        await asyncio.sleep(t)
        print('爬到',name,'先睡',t,'s')


# 创建异步协程的task任务，封装到列表
async def tasks_get_per_house_info(house_urls):
    tasks = []
    for house_url in house_urls:
        tasks.append(get_per_house_info(house_url))
    await asyncio.wait(tasks)


if __name__ == '__main__':
    start_time = time.time()  # 开始计时
    city = 'gz'
    url = 'https://{0}.lianjia.com/zufang/ '.format(city)
    # 主起始页
    bas_url = url
    # 当前页
    cur_url = url
    # 爬虫头，设置随机浏览器用户信息，且及时关闭访问链接
    header = {
        'User-Agent': get_ua(),
        'Connection': 'close'
    }
    area = get_area_list()
    print('每个区的信息获取成功，并以列表的形式存放在area中！')
    print('开始获取每个区的每个面积类型的每一页......')
    page_urls = get_per_page(area)
    print('每一页的url获取成功！开始获取每一页的所有房源url......')
    house_urls = get_per_house_url(page_urls)
    # 链家会同一个房源多次发布，此处去除重复的项
    df_house_urls = pd.DataFrame(house_urls).drop_duplicates().values.tolist()
    drop_duplicates_house_urls = []
    for df_house_url in df_house_urls:
        drop_duplicates_house_urls.append(df_house_url[0])

    # 设置字段名(文件名必须与上面一模一样）
    field_name = ['house_id', 'district_and_community','district','community','name', 'maintenance_time', 'rental_menthon', 'layout', 'orientation', 'house_area',
                  'floor', 'elevator', 'parking', 'water', 'electricity', 'gas', 'pay_methon', 'rent_of_month',
                  'deposit', 'the_nearest_subway', 'the_nearest_subway_distance', 'longitude', 'latitude','house_link']
    with open('house_info.csv', mode='w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(field_name)

    # 限制并发数量,此处在300-500内随机设置
    num_semaphore = 500
    SEMAPHORE = asyncio.Semaphore(num_semaphore)
    print('创建事件循环，并注册任务到事件上，开始异步协程，获取房源信息......')
    # 创建事件循环
    loop = asyncio.get_event_loop()
    # 将创建好的任务注册到事件中
    loop.run_until_complete(tasks_get_per_house_info(drop_duplicates_house_urls))
    print('恭喜，爬取完毕，总用时：',time.time()-start_time)
