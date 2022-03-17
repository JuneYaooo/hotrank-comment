#!/usr/bin/env Python
# coding=utf-8
import datetime
import requests
import time
from requests_html import HTMLSession
import re
import urllib3
from bs4 import BeautifulSoup
import pandas as pd
from pyquery import PyQuery as pq
import os
import random
from weibopy import WeiboOauth2, WeiboClient
import json
import webbrowser
import pymysql
from sqlalchemy import create_engine
# 解除警告
urllib3.disable_warnings()

class Spider(object):
    def __init__(self, host, user, passwd,  db, key_list):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.crawl_timestamp = int()
        self.today = datetime.date.today().strftime('%Y%m%d')
        self.curPath = os.getcwd()
        self.curPath = self.curPath + '/' + self.today
        if not os.path.exists(self.curPath):
            os.makedirs(self.curPath)
        self.spider_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        today = datetime.date.today()
        self.yesterday = str(today - datetime.timedelta(days=1))
        self.source = 'none'
        self.key_list = key_list
        self.hot_rank_colmuns = ['Article_Id', 'Article_Url', 'Article_Original_Id', 'Source_Website_Code',
                                 'Source_Website', 'Emotion_Label', 'Title', 'Publisher', 'Text', 'Hot_word',
                                 'Publish_Time', 'Forward_Amount', 'Reply_Amount', 'Like_Amount',
                                 'Negative_Comment_Amount', 'Positive_Comment_Amount', 'Neutral_Comment_Amount',
                                 'News_Category', 'News_Category_Code', 'Public_Opinion_Area',
                                 'Start_Crawler_Time', 'ETLDTC']
        self.comment_columns = ['Article_Id', 'Reply_Id', 'Reply_Source_Website_Code', 'Reply_Source_Website', 'Reply_Username', 'Reply_User_Sex', 'Reply_User_Province', 'Reply_Text', 'Reply_Time', 'Reply_Like_Amount', 'Emotion_Label', 'Emotion_Score', 'Start_Crawler_Time', 'ETLDTC']

    def run(self):
        pass

    def getproxy(self):
        proxy_url = 'https://dps.kdlapi.com/api/getdps/?orderid=954611309144085&num=1&pt=1&format=json&sep=1'
        proxy_response = requests.get(proxy_url)
        if proxy_response.status_code == 200:
            proxy_list = proxy_response.json().get('data').get('proxy_list')
        return proxy_list[0]

    def testip(self, ip):
        try:
            response = requests.get('https://m.weibo.cn/comments', proxies={"https": ip})
            response.close()
        except Exception as e:
            print('connect failed!!', e)
            return False
        else:
            print('success')
            return True

    def crawl_hot_rank(self):
        pass

    def crawl_comment(self):
        pass

    def structure_data(self, raw_data):
        pass

    def get_key_list(self):
        sql = 'select word from search_words'
        word = self.exeSQL(self.host, self.user, self.passwd, self.db, sql)
        key_word_list = []
        for item in word:
            key_word_list.append(item['word'])
        return key_word_list

    def get_rules(self):
        sql = 'select * from rules'
        rules_data = self.exeSQL(self.host, self.user, self.passwd, self.db, sql)
        # source_list,key_words_list,category_list = [],[],[]
        # for item in rules_data:
        #     source_list.append(item['source'])
        #     key_words_list.append(item['key_words'].split(","))
        #     category_list.append(item['category'])
        return rules_data


    def save_db(self, df, table):
        # 再插入新鲜的数据
        engine = create_engine("mysql+pymysql://{}:{}@{}:3306/{}?charset=utf8mb4".format(self.user, self.passwd, self.host, self.db))
        df.to_sql(name=table, con=engine, if_exists='append', index=False, index_label=False)
        print('数据更新成功！')

    def table_exists(self, con, table_name):        #这个函数用来判断表是否存在
        sql = "show tables;"
        tables = [con.execute(sql)]
        tables = [con.fetchall()]
        table_list = re.findall('(\'.*?\')',str(tables))
        table_list = [re.sub("'",'',each) for each in table_list]
        if table_name in table_list:
            return 1        #存在返回1
        else:
            return 0        #不存在返回0

    def exeSQL(self, host, user, passwd, db, sql):
        # 打开数据库连接
        conn = pymysql.connect(host=host, user=user, password=passwd, database=db, charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
        try:
            # 使用 cursor() 方法创建一个游标对象 cursor
            cursor = conn.cursor()
            print('数据库连接成功..')
            # 执行SQL语句
            cursor.execute(sql)
            result = cursor.fetchall()
            # 确认修改
            conn.commit()
            # 关闭游标
            cursor.close()
            # 关闭链接
            conn.close()
            print("语句 {} 执行成功！".format(sql))
            return result
        except Exception as e:
            print("语句 {} 执行失败！".format(sql))
            print('error!! ', e)
            return None



class WeiboAPIToken(object):
    def __init__(self,client_key = '494893131', client_secret = '10ebee87071055c826473d76a7ef3ccb'):
        self.client_key = client_key # 你的 app key
        self.client_secret = client_secret # 你的 app secret

    def getToken(self):
        redirect_url = 'https://api.weibo.com/oauth2/default.html'
        auth = WeiboOauth2(self.client_key, self.client_secret, redirect_url)
        # 获取认证 code
        webbrowser.open_new(auth.authorize_url)
        # 在打开的浏览器中完成操作
        # 最终会跳转到一个显示 「微博 OAuth2.0」字样的页面
        # 从这个页面的 URL 中复制 code= 后的字符串
        # URL 类似这样 https://api.weibo.com/oauth2/default.html?code=9c88ff5051d273522700a6b0261f21e6
        code = input('输入 code:')
        # 使用 code 获取 token
        token = auth.auth_access(code)
        print('your token',token)
        # token 是刚刚获得的 access_token，可以一直使用
        return token['access_token']


class Spider_Weibo(Spider):
    def __init__(self, host, user, passwd, db, key_list):
        super().__init__(host, user, passwd, db, key_list)
        self.key_list = key_list
        self.session = HTMLSession()
        self.session.keep_alive = False
        self.cookie = 'SCF=Arfp1F3VzD4nI_NTX2J5ls-nuxbC7reVOiLQro05XLXc8HXEuy_uCUoXy3lRbdIVPGeTEasxxmcfSsN4be1Fe7w.; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWFzo87H2SVHbn1Z834Nscf5NHD95QNe02N1K2E1hzcWs4DqcjeeJDfdJH4qgYEehz7; _T_WM=58674612657; XSRF-TOKEN=3e3d21; WEIBOCN_FROM=1110006030; SUB=_2A25PGNriDeRhGeFN6lUY8SzEyTqIHXVs4uaqrDV6PUJbkdANLUTdkW1NQEoHEFj9v0r18nmmPx5Nq0czz6vc5jmY; SSOLoginState=1646045878; MLOGIN=1; M_WEIBOCN_PARAMS=oid%3D4741919475503118%26luicode%3D20000061%26lfid%3D4741919475503118%26uicode%3D20000061%26fid%3D4741919475503118'
        # self.cookie = 'SINAGLOBAL=3254529666019.8096.1640676562900; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWrqm4kqPbIEDSGAAuIdzTh5JpX5KMhUgL.Fo-peKBcSo5RS0.2dJLoI0YLxKBLBonL1h5LxKnLBKML1h2LxK.L1-BL1KzLxK-LB.qL1heLxK-LBo5L12qLxKML1-qLBoeLxK-LBKBLBoBt; UOR=,,www.baidu.com; ULV=1645687336502:13:6:3:5133549709088.898.1645687336498:1645597320897; ALF=1677552948; SSOLoginState=1646016953; SCF=Asa-4ANoJXaO18nzjFWun7oPDLXMYHXcfXEoH0_4jWFkSWstFULZRq2AZY032UTY4sZgnRP8EgINvlR8Sod2Cn8.; SUB=_2A25PGEnpDeRhGeNP6lYX9i7EzDWIHXVsbDwhrDV8PUNbmtAKLUXgkW9NTrg3CVG4m7JGevrh-rkEw8ADIJtcIcGt; XSRF-TOKEN=ZZzJjhM6-TWNl54yODrBYJu1; WBPSESS=xAvgykNBjGKWclbaSHnR2kwicLSbmJCednx_YUgZQNrTUh3UysRcbSyJqGtIf5CtF7qWyBEjOU0ITb49dAr1NdYbcm23R8urvDetfDYMAe56Cc6V08YYsJ8GhAwEXDwNJzmeLwyAvpMc3OJK4yZ-9w=='
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'

    def run(self):
        self.key_list = self.get_key_list()
        self.rules_data = self.get_rules()
        hot_rank_df = self.crawl_hot_rank()
        # access_token = WeiboAPIToken().getToken()
        access_token = '2.00Mioh6I0R_WUXef10097fff1jzHnC'
        if hot_rank_df.shape[0] > 0:
            self.save_db(df=hot_rank_df, table="edr_public_opinion_analysis")
            self.crawl_comment(hot_rank_df, access_token)
        else:
            print('没有疫情相关热搜微博')

    def crawl_hot_rank(self):
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
        r = 0
        hot_rank_df = pd.DataFrame(columns=self.hot_rank_colmuns)
        # 遍历热搜的标签
        # #pl_top_realtimehot 根据id, > table > tbody > tr 逐层查找
        for item in res.select("#pl_top_realtimehot > table > tbody > tr"):
            # 按类名.td-01提取热搜排名
            _rank = item.select_one('.td-01').text
            if not _rank:
                continue
            # 按类名.td-02提取热搜
            topic = item.select_one(".td-02 > a").text
            topic_url = 'https://s.weibo.com/' + item.select_one(".td-02 > a")['href']

            # 提取热搜热度
            heat = item.select_one(".td-02 > span").text

            # 提取热搜标签
            icon = item.select_one(".td-03").text

            # 是否在关键词表里
            flag = 0
            hot_words = ""
            for key in self.key_list:
                if key in topic:
                    flag = 1
                    hot_words += "," + key if hot_words != "" else key

            if flag == 1:
                # 获取第一条新闻链接
                article_url, author = self.GetTop1WeiboUrl(topic_url)
                # 当可以获取新闻链接时，才统计
                if article_url != "":
                    # 获取微博ID
                    weibo_id, created_at = self.GetWeiboID(article_url)
                    # 获取第一条新闻信息全文, 点赞数, 评论数, 转发数
                    longTextContent, attitudes_count, comments_count, reposts_count = self.GetWeiboInfo(weibo_id)

                    # 写入前，进行新闻分类
                    news_category = ""
                    news_category_code = ""
                    for item in self.rules_data:
                        source = topic if item['source'] == "topic" else author
                        for word in item['key_words'].split(","):
                            if word in source:
                                news_category += "," + item['category'] if news_category != "" else item['category']
                                news_category_code += "," + item['category_code'] if news_category_code != "" else item[
                                    'category_code']
                                break
                    # 当不归属于任何一类时，划分到公众热点去
                    if news_category == "":
                        news_category = '公众热点'
                        news_category_code = str(99)

                    hot_rank_df.loc[r] = ['PA'+str(weibo_id), article_url, str(weibo_id), "001", "weibo", "", topic,
                                     author, longTextContent, '', created_at, reposts_count, comments_count, attitudes_count,
                         0, 0, 0, news_category,news_category_code,  "area", self.spider_time, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())]
                    r += 1

        return hot_rank_df


    def GetTop1WeiboUrl(self, topic_url):
        headers = {
            'cookie': self.cookie,
            'user-agent': self.user_agent
        }
        prox = ''

        res = requests.get(topic_url, proxies={'http': prox, 'https': prox}, headers=headers, verify=False)  # 抓取内容
        res.raise_for_status()
        # 当出现乱码时，apparent_encoding编码方式更准确
        res.encoding = 'utf-8'
        res = BeautifulSoup(res.text, "lxml")
        try:
            first_news = res.select("#pl_feedlist_index > div:nth-child(4) > div:nth-child(1) > div.card > div.card-feed > div.content > p.from")
            article_url = first_news[0].select_one("a")['href']
            first_news_info = res.select("#pl_feedlist_index > div:nth-child(4) > div:nth-child(1) > div.card > div.card-feed > div.content > div.info > div:nth-child(2)")
            author = first_news_info[0].select_one("a")['nick-name']
        except Exception as e:
            print('获取置顶新闻失败， 继续尝试获取热门新闻！！ ', e)
            try:
                first_news = res.select(
                    "#pl_feedlist_index > div:nth-child(2) > div:nth-child(1) > div.card > div.card-feed > div.content > p.from")
                article_url = first_news[0].select_one("a")['href']
                first_news_info = res.select(
                    "#pl_feedlist_index > div:nth-child(2) > div:nth-child(1) > div.card > div.card-feed > div.content > div.info > div:nth-child(2)")
                author = first_news_info[0].select_one("a")['nick-name']
                print("获取热门新闻成功！\narticle url: {} \nauthor:{}".format( article_url, author))
            except Exception as e:
                print('获取热门新闻失败！！ 放弃获取新闻！！ ', e)
                article_url, author = "",""
        return article_url, author

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
                                    verify=False).json()
        # new_response = response.get('content')
        # content.decode()
        weibo_id = response.get('id')
        created_at = response.get('created_at')
        # re.findall('"id":(.*?),"idstr"', response)[0]
        return str(weibo_id), created_at

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

    def crawl_comment(self, df, access_token):
        client = WeiboClient(access_token)
        start_time = time.time()
        today = datetime.date.today().strftime('%Y%m%d')
        with open('./{}/weibo_record.txt'.format(today), 'a') as f:
            f.write('start time: {}\n'.format(start_time))
        total_comment = 0
        for index, row in df.iterrows():
            # i = 1
            # 测试一下
            if index>1:
                break
            item_start_time = time.time()
            article_id = int(row['Article_Original_Id'])
            topic = row['Title']
            comments_count = row['Reply_Amount']
            print('topic start crawl:', topic)
            comment_list = []  # 保存所有评论正文

            # 获取topic第一条微博的评论列表
            pages = -(-int(comments_count) // 200)
            for p in range(1, pages+1):
                # 初始化
                # self.proxy = self.getproxy()
                # client.session.proxies.update({'http': self.proxy, 'https': self.proxy})
                client.session.headers.update({'User-Agent': self.user_agent})
                # 开始抓取下一页，成功抓到为止
                count_next = 0
                while True:
                    count_next += 1
                    try:
                        result = client.get(suffix='comments/show.json',
                                            params={'id': article_id, 'count': 200, 'page': p})
                        success = True
                    except Exception as e:
                        print('error!!', e)
                        client.session.close()
                        print('关闭 session')
                        success = False
                        time.sleep(5)
                        # 换IP
                        # self.proxy = self.getproxy()
                        # client.session.proxies.update({'http': self.proxy, 'https': self.proxy})
                        # client.session.headers.update({'User-Agent': self.user_agent})
                        # print('更换ip为：{}'.format(self.proxy))
                    if success == True:
                        break
                    elif count_next % 2 == 0:
                        nap = random.uniform(2, 5)
                        print('尝试抓取当前第{}页，连接失败{}次，休眠{}秒'.format(p, count_next, nap))
                        time.sleep(nap)
                    elif count_next % 3 == 0:
                        nap = random.uniform(4, 10)
                        print('尝试抓取当前第{}页，连接失败{}次，休眠{}秒'.format(p, count_next, nap))
                        time.sleep(nap)
                    # 尝试10次，不成功则打断
                    elif count_next > 5:
                        print('尝试抓取当前第{}页，连接失败{}次，中止'.format(p, count_next))
                        break

                if len(result) == 0 or len(result['comments']) == 0:
                    # 随机休息8~20秒
                    nap = random.uniform(8, 20)
                    time.sleep(nap)
                    break
                comments = result['comments']
                tmp_df = pd.DataFrame(columns=self.comment_columns)
                r = 0
                for comment in comments:
                    text = re.sub('回复.*?:', '', str(comment['text']))
                    # text = comment['reply_original_text']
                    province = comment['user']['location']
                    gender = "女" if comment['user']['gender'] == "f" else "男"
                    like_count = 0 # 没找到哪里有
                    comment_list.append(text)
                    tmp_df.loc[r] = ['PA'+str(row['Article_Id'],), 'NR'+str(comment['id']), '001', 'weibo', comment['user']['screen_name'], gender, province, text, str(datetime.datetime.strptime(comment['created_at'], '%a %b %d %H:%M:%S %z %Y')), like_count, '', 0, self.spider_time, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())]
                    r += 1

                # 当前页评论落库
                self.save_db(tmp_df, table="edr_news_reply")
                # 获取剩余调用次数
                result_limit = client.get(suffix='account/rate_limit_status.json')
                remaining_ip_hits = result_limit['remaining_ip_hits']
                reset_time = result_limit['reset_time']
                # 随机休息8~30秒
                nap = random.uniform(8, 30)
                print('已抓取评论 {} 条, 休息{} 秒, 剩余ip调用次数: {}, 下一次重置时间：{}'.format(len(comment_list), nap, remaining_ip_hits, reset_time))
                time.sleep(nap)
                # 单条新闻，超过一万条评论自动停止抓取
                if len(comment_list)>10000:
                    break
            print('**************Topic {} 共获取{}页, 总计{}条评论**************\n'.format(topic, p - 1, len(comment_list)))
            item_end_time = time.time()
            item_cost_time = item_end_time - item_start_time
            with open('./{}/weibo_record.txt'.format(today), 'a') as f:
                f.write('Topic {} 共获取{}页, 总计{}条评论, 花费{}秒, 剩余ip调用次数: {}, 下一次重置时间：{}\n'.format(topic, p - 1,
                                                                                             len(comment_list),
                                                                                             item_cost_time,
                                                                                             remaining_ip_hits,
                                                                                             reset_time))
            total_comment += len(comment_list)
        end_time = time.time()
        cost_time = end_time - start_time
        print('已完成热搜评论获取，总共{}条新闻，{}条评论，用时共计{}秒'.format(df.shape[0], total_comment, cost_time))
        with open('./{}/weibo_record.txt'.format(today), 'a') as f:
            f.write('已完成热搜评论获取，总共{}条新闻，{}条评论，用时共计{}秒'.format(df.shape[0], total_comment, cost_time))


class Spider_Toutiao(Spider):
    def __init__(self, host, user, passwd, db, key_list):
        super().__init__(host, user, passwd, db, key_list)
        self.key_list = key_list
        self.session = HTMLSession()
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'

    def run(self):
        self.key_list = self.get_key_list()
        self.rules_data = self.get_rules()
        hot_rank_df = self.crawl_hot_rank()
        if hot_rank_df.shape[0] > 0:
            self.save_db(df=hot_rank_df, table="edr_public_opinion_analysis")
            self.crawl_comment(hot_rank_df)
        else:
            print('没有疫情相关头条热搜新闻')

    def crawl_hot_rank(self):
        headers = {
            'user-agent': self.user_agent,
            'referer': 'https://www.toutiao.com/',
        }
        hotUrl = "https://i.snssdk.com/hot-event/hot-board/?origin=hot_board"
        prox = ''
        response = requests.get(hotUrl, proxies={'http': prox, 'https': prox}, headers=headers, verify=False)  # 抓取内容
        if response.status_code == 200:
            res = response.json()
            res = res.get('data')
        hot_df = pd.DataFrame(res)
        r = 0
        hot_rank_df = pd.DataFrame(columns=self.hot_rank_colmuns)
        for index, row in hot_df.iterrows():
            rank = index + 1
            topic = row['Title']
            topic_url = row['Url']
            # 提取热搜热度
            heat = row['HotValue']
            # 提取热搜标签
            icon = row['Label']
            # 获取topic id
            topic_id = re.findall(r"topic_id=([0-9]*)", topic_url)[0]
            First_article_Url = 'https://i.snssdk.com/api/feed/topic_innerflow/v1/?query_id={}&offset=-1&count=20&aid=13&category=topic_innerflow&ttfrom=amos_land_normal&stream_api_version=88&client_extra_params=%7B%22is_webview%22:%221%22,%22use_webp%22:%22true%22%7D&style_id=unknown'.format(
                topic_id)

            # 是否在关键词表里
            flag = 0
            hot_words = ""
            for key in self.key_list:
                if key in topic:
                    flag = 1
                    hot_words += "," + key if hot_words != "" else key

            if flag == 1:
                # 获取第一条新闻链接, 摘要, 点赞数, 评论数
                longTextContent, author, create_time,  attitudes_count, comments_count, reposts_count, group_id, item_id = self.GetTop1ToutiaoInfo(
                    First_article_Url)
                # 无法获取文章id，不进行存储
                if group_id == "" or item_id == "":
                    print('{}，无法获取文章id，不进行存储'.format(topic))
                    break
                # 写入前，进行新闻分类
                news_category = ""
                news_category_code = ""
                for item in self.rules_data:
                    source = topic if item['source'] == "topic" else author
                    for word in item['key_words'].split(","):
                        if word in source:
                            news_category += "," + item['category'] if news_category != "" else item['category']
                            news_category_code += "," + item['category_code'] if news_category_code != "" else item[
                                'category_code']
                            break
                # 当不归属于任何一类时，划分到公众热点去
                if news_category == "":
                    news_category = '公众热点'
                    news_category_code = str(99)
                tmp_row_list = ['PA'+str(group_id), First_article_Url, str(group_id)+"-"+str(item_id),
                     "002", "toutiao", "", topic, author, longTextContent, '',
                     create_time, reposts_count, comments_count, attitudes_count, 0, 0, 0, news_category, news_category_code,
                     "area", self.spider_time, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())]
                hot_rank_df.loc[r] = tmp_row_list
                r += 1

        return hot_rank_df

    def GetTop1ToutiaoInfo(self, First_article_Url):
        header = {
            'User-Agent': 'User-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1 wechatdevtools/0.7.0 MicroMessenger/6.3.9 Language/zh_CN webview/0'}
        response = requests.get(First_article_Url, headers=header)
        if response.status_code == 200 and len(response.json().get('data')) > 0:
            res = response.json().get('data')
            content = json.loads(res[0]['content'])
            sub_raw_datas = content['sub_raw_datas'][0] if 'raw_data' not in content['sub_raw_datas'][0] else content['sub_raw_datas'][0]['raw_data']
            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sub_raw_datas['publish_time'])) if 'publish_time' in sub_raw_datas else time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sub_raw_datas['create_time'])) if 'create_time' in sub_raw_datas else ''
            author = sub_raw_datas['user_info']['name'] if 'user_info' in sub_raw_datas and 'name' in sub_raw_datas['user_info'] else sub_raw_datas['user']['screen_name'] if 'user' in sub_raw_datas and 'screen_name' in sub_raw_datas['user'] else sub_raw_datas['user']['info']['name'] if 'user' in sub_raw_datas and 'info' in sub_raw_datas['user'] and 'name' in sub_raw_datas['user']['info']['name'] else sub_raw_datas['source'] if 'source' in sub_raw_datas else ''
            longTextContent = sub_raw_datas['abstract'] if 'abstract' in sub_raw_datas else sub_raw_datas['title'] if 'title' in sub_raw_datas else ''
            comments_count = sub_raw_datas['comment_count'] if 'comment_count' in sub_raw_datas else 0
            attitudes_count = sub_raw_datas['like_count'] if 'like_count' in sub_raw_datas else 0
            group_id =  sub_raw_datas['group_id'] if 'group_id' in sub_raw_datas else sub_raw_datas['id'] if 'id' in sub_raw_datas else ""
            item_id = sub_raw_datas['item_id'] if 'item_id' in sub_raw_datas else sub_raw_datas['id'] if 'id' in sub_raw_datas else ""
            reposts_count = 0
        else:
            longTextContent, author, create_time, attitudes_count, comments_count, reposts_count, group_id, item_id = '','','', 0, 0, 0, '', ''
        return longTextContent, author, create_time, attitudes_count, comments_count, reposts_count, group_id, item_id

    def crawl_comment(self, df):
        headers = {
            'user-agent': self.user_agent
        }
        total_comment = 0
        start_time = time.time()
        today = datetime.date.today().strftime('%Y%m%d')
        with open('./{}/toutiao_record.txt'.format(today), 'a') as f:
            f.write('start time: {}\n'.format(start_time))
        for index, row in df.iterrows():
            item_start_time = time.time()
            # group_id = int(row['group_id'])
            # item_id = int(row['item_id'])
            group_id = int(row['Article_Original_Id'].split("-")[0])
            item_id = int(row['Article_Original_Id'].split("-")[0])
            topic = row['Title']
            p=0
            comment_count=0
            while True:
                try:
                    # 构造起始地址
                    start_url = f'https://www.toutiao.com/article/v2/tab_comments/?aid=24&app_name=toutiao_web&offset={p*20}&count=20&group_id={group_id}&item_id={item_id}&_signature=_02B4Z6wo00d01A4qfzAAAIDAjiiFc.K654gODnuAAGGd9IKTBEf00oca4mphrLRhdeNza5puEyXAWWMPtwGZUH-HDG3uuOFXOWvmgITMPQjHfiAEeoWnC.ZoqcSHODQjpZr3kUSy5DKBWCgybf'
                    """
                            2.发送请求，获取响应： 解析起始的url地址
                            :return:
                            """
                    prox = ''
                    response = self.session.get(start_url, proxies={'http': prox, 'https': prox}, headers=headers,
                                                verify=False).json()
                    if response['message'] != 'success':
                        print("error,response['message'] != 'success'\n",response)
                        break
                    if len(response['data'])==0:
                        break
                    data_list = response['data']
                    r = 0
                    tmp_df = pd.DataFrame(columns=self.comment_columns)
                    for comment_item in data_list:
                        comment_item = comment_item['comment']
                        texts = comment_item['text']
                        user_name = comment_item['user_name']
                        like_counts = comment_item['digg_count']
                        std_create_times = comment_item['create_time']
                        gender = ""
                        tmp_df.loc[r] = ['PA'+str(group_id), 'NR'+'comment_id', '002', 'toutiao', user_name, gender, "province", texts, std_create_times, like_counts, '', 0, self.spider_time, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())]
                        r+=1

                    # 当前页评论落库
                    self.save_db(tmp_df, table="edr_news_reply")
                    comment_count += tmp_df.shape[0]
                    if len(data_list)<20:
                        break
                    p+=1
                except Exception as e:
                    print('error!!',e)
                    break
                # 随机休息3~15秒
                nap = random.uniform(3, 15)
                print('sleep {} seconds'.format(nap))
                time.sleep(nap)
            item_end_time = time.time()
            item_cost_time = item_end_time - item_start_time
            total_comment += comment_count
            print('***********Topic: {}，共计{}页，{}条评论，用时共计{}秒***********\n'.format(topic, p + 1, comment_count, item_cost_time))
            with open('./{}/toutiao_record.txt'.format(today), 'a') as f:
                f.write('已完成Topic {} 评论获取，共计{}页，{}条评论，用时共计{}秒\n'.format(topic, p + 1, comment_count, item_cost_time))
        end_time = time.time()
        cost_time = end_time - start_time
        print('已完成热搜评论获取，总共{}条新闻，{}条评论，用时共计{}秒'.format(df.shape[0], total_comment, cost_time))
        with open('./{}/toutiao_record.txt'.format(today), 'a') as f:
            f.write('已完成热搜评论获取，总共{}条新闻，{}条评论，用时共计{}秒'.format(df.shape[0], total_comment, cost_time))


if __name__ == '__main__':

    # 默认关键词
    key_list = ["确诊","病毒","感染者","隔离","疫","新冠肺炎","德尔塔","阳性","奥密克戎"]
    target_folder = '.'
    if not os.path.exists('{}/{}'.format(target_folder, datetime.date.today().strftime('%Y%m%d'))):
        os.makedirs('{}/{}'.format(target_folder, datetime.date.today().strftime('%Y%m%d')))
    # 微博 API 爬虫
    sp_Weibo = Spider_Weibo('localhost', 'xiaoming', '000000', 'test', key_list=key_list)
    sp_Weibo.run()
    # 头条 爬虫
    sp_Toutiao = Spider_Toutiao('localhost', 'xiaoming', '000000', 'test', key_list=key_list)
    sp_Toutiao.run()

