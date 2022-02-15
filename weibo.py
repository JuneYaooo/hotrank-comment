# -*- encoding:utf-8 -*-
from datetime import datetime
import csv
import requests
import time
from requests_html import HTMLSession
import re
import urllib3
from bs4 import BeautifulSoup
import pandas as pd
from pyquery import PyQuery as pq

# 解除警告
urllib3.disable_warnings()


class HotSpider(object):
    def __init__(self,key_list = ["确诊","病毒","感染者","隔离","疫","新冠肺炎","德尔塔","阳性","奥密克戎"]):
        self.key_list = key_list
        self.session = HTMLSession()
        self.cookie = '_T_WM=61174260321; XSRF-TOKEN=83097f; WEIBOCN_FROM=1110006030; BAIDU_SSP_lcr=https://www.google.com.hk/; SCF=Arfp1F3VzD4nI_NTX2J5ls-nuxbC7reVOiLQro05XLXcwGkhrZXIgeP2aYv8r6CRiNKZnB3-jzROSjFRfqU23ao.; SUB=_2A25PA-AFDeRhGeFN6lUY8SzEyTqIHXVsD4BNrDV6PUJbktCOLU32kW1NQEoHEBFv4qBiZdY5BaMbxxTmDm5zrkYv; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWFzo87H2SVHbn1Z834Nscf5NHD95QNe02N1K2E1hzcWs4DqcjeeJDfdJH4qgYEehz7; SSOLoginState=1644662869; MLOGIN=1; M_WEIBOCN_PARAMS=oid%3D4735998870950137%26luicode%3D20000061%26lfid%3D4735998870950137%26uicode%3D20000061%26fid%3D4735998870950137'
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36'

    def GetWeiboRealtimeHotInfos(self):
        headUrl = "https://s.weibo.com/"
        headers = {
            'cookie': self.cookie,
            'user-agent': self.user_agent
        }
        hotUrl = "https://s.weibo.com/top/summary?cate=realtimehot"
        prox = ''
        res = requests.get(hotUrl, proxies={'http': prox, 'https': prox}, headers=headers, verify=False) # 抓取内容
        res.raise_for_status()     # 检测抓取内容是否正常
        # encoding代表Head中的编码方式 apparent_encoding代表Body中编码方式
        # 当出现乱码时，apparent_encoding编码方式更准确
        res.encoding = res.apparent_encoding
        res = BeautifulSoup(res.text, "lxml")
        hot_list = []
        # 遍历热搜的标签
        # #pl_top_realtimehot 根据id, > table > tbody > tr 逐层查找
        for item in res.select("#pl_top_realtimehot > table > tbody > tr"):
            # 按类名.td-01提取热搜排名
            _rank = item.select_one('.td-01').text
            if not _rank:
                continue
            # 按类名.td-02提取热搜关键词
            keyword = item.select_one(".td-02 > a").text
            topic_url = 'https://s.weibo.com/' + item.select_one(".td-02 > a")['href']

            # 提取热搜热度
            heat = item.select_one(".td-02 > span").text

            # 提取热搜标签
            icon = item.select_one(".td-03").text

            # 是否在关键词表里
            flag = 0
            for key in self.key_list:
                if key in keyword:
                    flag = 1
            if flag==1:
                # 获取第一条新闻链接
                article_url = self.GetTop1WeiboUrl(topic_url)
                # 获取微博ID
                weibo_id = self.GetWeiboID(article_url)
                # 获取第一条新闻信息全文, 点赞数, 评论数, 转发数
                longTextContent, attitudes_count, comments_count, reposts_count = self.GetWeiboInfo(weibo_id)


                hot_list.append(
                    {"rank": _rank, "topic": keyword, "heat": heat, "icon": icon, "topic_url": topic_url, "time":
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "is_related": flag, "weibo_id": weibo_id, "longTextContent": longTextContent, "attitudes_count": attitudes_count, "comments_count": comments_count, "reposts_count": reposts_count})
            else:
                hot_list.append(
                    {"rank": _rank, "topic": keyword, "heat": heat, "icon": icon, "topic_url": topic_url, "time":
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "is_related": flag})

        return hot_list

    def GetTop1WeiboUrl(self, topic_url):
        headers = {
            'cookie': self.cookie,
            'user-agent': self.user_agent
        }
        prox = ''
        res = requests.get(topic_url, proxies={'http': prox, 'https': prox}, headers=headers, verify=False)  # 抓取内容
        res.raise_for_status()
        res = BeautifulSoup(res.text, "lxml")
        first_news = res.select("#pl_feedlist_index > div:nth-child(1) > div:nth-child(2) > div.card > div.card-feed > div.content > p.from")
        article_url = first_news[0].select_one("a")['href']
        return article_url

    def GetWeiboID(self, article_url):
        headers_1 = {
            'cookie': self.cookie,
            'user-agent': self.user_agent
        }

        uid_1 = re.findall('/(.*?)\?', article_url)[0]
        uid_2 = uid_1.split('/', 3)[3]

        url_1 = f'https://weibo.com/ajax/statuses/show?id={uid_2}'
        prox = ''
        response = self.session.get(url_1, proxies={'http': prox, 'https': prox}, headers=headers_1,
                                    verify=False).content.decode()
        weibo_id = re.findall('"id":(.*?),"idstr"', response)[0]
        return weibo_id

    def GetWeiboInfo(self, weibo_id):
        base_url2 = 'https://m.weibo.cn/statuses/extend?id='
        url2 = base_url2 + weibo_id
        headers2 = {
            'Host': 'm.weibo.cn',
            'Referer': 'https://m.weibo.cn/status/' + weibo_id,
            'User-Agent': 'User-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1 wechatdevtools/0.7.0 MicroMessenger/6.3.9 Language/zh_CN webview/0'}
        news_response = requests.get(url2, headers=headers2)
        if news_response.status_code == 200:
            news_response = news_response.json().get('data')
            longTextContent = pq(news_response.get("longTextContent")).text()
            attitudes_count = news_response.get('attitudes_count')
            comments_count = news_response.get('comments_count')
            reposts_count = news_response.get('reposts_count')
        else:
            longTextContent, attitudes_count, comments_count, reposts_count = '', '', '', ''
        return longTextContent, attitudes_count, comments_count, reposts_count


    def CrawlCommentMain(self, df):
        headers_2 = {
            "referer": "https://m.weibo.cn/status/Kk9Ft0FIg?jumpfrom=weibocom",
            'cookie': self.cookie,
            'user-agent': self.user_agent
        }
        for index, row in df.iterrows():
            i = 1
            weibo_id = int(row['weibo_id'])
            topic = row['topic']
            # 构造起始地址
            start_url = f'https://m.weibo.cn/comments/hotflow?id={weibo_id}&mid={weibo_id}&max_id_type=0'
            """
                    2.发送请求，获取响应： 解析起始的url地址
                    :return:
                    """
            prox = ''
            response = self.session.get(start_url, proxies={'http': prox, 'https': prox}, headers=headers_2,
                                        verify=False).json()
            """解析第一页评论内容"""
            self.parse_response_data(response, i, topic, weibo_id)
            """提取翻页的max_id"""
            max_id = response['data']['max_id']
            print('max_id', max_id)
            """提取翻页的max_id_type"""
            max_id_type = response['data']['max_id_type']

            """构造GET请求参数"""
            data = {
                'id': weibo_id,
                'mid': weibo_id,
                'max_id': max_id,
                'max_id_type': max_id_type
            }
            # 第2到最后一页
            while True:
                if i % 10 == 0:
                    time.sleep(3)
                elif i % 50 == 0:
                    time.sleep(20)
                elif i % 100 == 0:
                    time.sleep(60)
                start_url = 'https://m.weibo.cn/comments/hotflow?'
                prox = ''
                response = self.session.get(start_url, proxies={'http': prox, 'https': prox}, headers=headers_2,
                                            params=data, verify=False).json()
                if response['ok'] != 1:
                    break

                # 解析第2页~最后1页数据
                self.parse_response_data(response, i, topic, weibo_id)

                """提取翻页的max_id"""
                max_id = response['data']['max_id']
                """提取翻页的max_id_type"""
                max_id_type = response['data']['max_id_type']
                """构造GET请求参数"""
                data = {
                    'id': weibo_id,
                    'mid': weibo_id,
                    'max_id': max_id,
                    'max_id_type': max_id_type
                }
                i+=1

    def parse_response_data(self, response, i, topic, weibo_id):
        """
        从响应中提取评论内容
        :return:
        """
        """提取出评论大列表"""

        data_list = response['data']['data']
        # print(data_list)
        for data_json_dict in data_list:
            # 提取评论内容
            try:
                texts_1 = data_json_dict['text']
                """需要sub替换掉标签内容"""
                # 需要替换的内容，替换之后的内容，替换对象
                alts = ''.join(re.findall(r'alt=(.*?) ', texts_1))
                texts = re.sub("<span.*?</span>", alts, texts_1)
                # 点赞量
                like_counts = str(data_json_dict['like_count'])
                # 评论时间   格林威治时间---需要转化为北京时间
                created_at = data_json_dict['created_at']
                std_transfer = '%a %b %d %H:%M:%S %z %Y'
                std_create_times = str(datetime.strptime(created_at, std_transfer))
                # 性别  提取出来的是  f
                gender = data_json_dict['user']['gender']
                genders = '女' if gender == 'f' else '男'
                # 用户名
                screen_names = data_json_dict['user']['screen_name']

                # print(screen_names, genders, std_create_times, texts, like_counts, topic, weibo_id)
                write_csv(path="/root/nas/comment-crawler/crawl_result.csv",
                               data_row=[screen_names, genders, std_create_times, texts, like_counts, topic, weibo_id])
            except Exception as e:
                print(e)
                continue
        print('*******************************************************************************************')
        print()
        print(f'*****第{i}页评论打印完成*****')


def create_csv(path, csv_head):
    with open(path, 'w', newline='', encoding='utf-8-sig', errors='ignore') as f:
        csv_write = csv.writer(f)
        csv_write.writerow(csv_head)

def write_csv(path, data_row):
    with open(path, 'a+', newline='', encoding='utf-8-sig', errors='ignore') as f:
        csv_write = csv.writer(f)
        csv_write.writerow(data_row)


if __name__ == '__main__':
    key_list = ["确诊","病毒","感染者","隔离","疫","新冠肺炎","德尔塔","阳性","奥密克戎"]
    sp = HotSpider(key_list=key_list)
    wb_hot_list = sp.GetWeiboRealtimeHotInfos()
    wb_hot_df = pd.DataFrame(wb_hot_list)
    wb_hot_df.to_csv('/root/nas/comment-crawler/wb_hot.csv', index=False, encoding='utf-8-sig')
    # wb_hot_df = pd.read_csv('/root/nas/comment-crawler/wb_hot.csv')
    create_csv(path="/root/nas/comment-crawler/crawl_result.csv",
               csv_head=["screen_names", "genders", "std_create_times", "texts", "like_counts", "topic", "weibo_id"])

    sp.CrawlCommentMain(wb_hot_df[wb_hot_df['is_related']==1])
