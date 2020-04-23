import pymysql
import schedule, time, os, sys, traceback, re, threading
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from flask import Flask, request, jsonify

# crawler的父目录加入系统搜索目录，以使得在linux系统中可以import自定义的模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from crawler.utils import base as base_util
from crawler.config.index import config


class Crawler():
    _instance_lock = threading.Lock()

    browser = None
    conn = None
    cursor = None

    def __init__(self):
        pass

    # 实现单例模式
    def __new__(cls, *args, **kwargs):
        if not hasattr(Crawler, "_instance"):
            with Crawler._instance_lock:
                if not hasattr(Crawler, "_instance"):
                    Crawler._instance = object.__new__(cls)
        return Crawler._instance

    def get_conn(self):
        try:
            self.conn.ping(reconnect=True)
        except Exception as e:
            self.conn = pymysql.connect(host=config.MYSQL['host'], port=config.MYSQL['port'],
                                   user=config.MYSQL['user'], passwd=config.MYSQL['password'],
                                   db=config.MYSQL['db'], charset=config.MYSQL['charset'])
        finally:
            self.cursor = self.conn.cursor()


    def get_browser(self):
        if self.browser is None:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('blink-settings=imagesEnabled=false')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-plugins')
            chrome_options.add_argument('--disable-images')
            # chrome_options.add_experimental_option("prefs", {
            #     "download.default_directory": r"D:\\workspace",
            #     "download.prompt_for_download": False,
            #     "download.directory_upgrade": True,
            #     "safebrowsing.enabled": True
            # })
            chrome_options.add_experimental_option("prefs", {
                'profile.default_content_settings.popups': 0,
                'download.default_directory': 'D:\\workspace'
            })
            self.browser = webdriver.Chrome(port= config.CHROME['port'], options=chrome_options)


    # 申明 browser和conn变量
    def init(self):
        self.get_browser()
        self.get_conn()

    def get_categories(self, category_id=None, limit=100):
        category_sql = '''
        select id, url, xpath_title, charset 
        from category 
        where (%s is null and category_state = 0) 
        or (%s is not null and id = %s)
        limit %s
        '''
        self.cursor.execute(category_sql, (category_id, category_id, category_id, limit))
        categories = self.cursor.fetchall()
        t4 = time.time()
        return categories

    def crawl_title(self, categories):
        new_insert_sql = '''
        insert into news (url, title, category_id) 
        select %s, %s, %s from dual 
        where not exists (select 1 from news n where n.url = %s)
        '''
        title_results = {}
        for category in categories:
            category_id, url, xpath, charset = category
            print('\n时间：{}，新闻分类：{}, xpath'.format(base_util.cur_time(), url, xpath))
            title_result = {
                'category_id': category_id,
                'url': url,
                'xpath': xpath,
                'charset': charset,
                'news': None,
                'error': None
            }
            try:
                self.browser.get(url)
                xpath_res = self.browser.find_element_by_xpath(xpath)
                news = xpath_res.find_elements_by_tag_name('a')
                invalid_title = r'^(首页|下一页|上一页|确定|末页|尾页|更多(>>)?|\d+)$'
                invalid_href = r'^(\.\/|\.\.\/)((\.\.\/)+)?$'
                sql_input = []
                news_results = []
                for n in news:
                    href = n.get_attribute('href')
                    title = n.get_attribute('title')
                    if not title:
                        title = n.text
                    if re.match(invalid_title, title) or re.match(invalid_href, href):
                        continue
                    href = base_util.url_format(url, href)[2]
                    sql_input.append((href, title, category_id, href))
                    news_results.append({
                        'url': href,
                        'title': title
                    })
                if len(sql_input) > 0:
                    # print('抓取的结果：', sql_input)
                    self.cursor.executemany(new_insert_sql, sql_input)
                    self.conn.commit()
                title_result['news'] = news_results
            except Exception as e:
                print('爬取标题:{}出错，error: {}'.format(url, traceback.print_exc()))
                title_result['error'] = repr(e)
                self.conn.rollback()
            title_results[url] = title_result
        return title_results

    def get_news(self, category_id=None, limit=100):
        news_sql = '''
            select n.id, n.url, n.category_id, c.xpath_text, c.charset, n.text_state, n.attachment_state
            from news n 
            join category c on n.category_id = c.id 
            where (%s is null and (n.news_state = 0 and (n.text_state = '0' or n.attachment_state = '0')))
            or (%s is not null and c.id = %s)
            and c.xpath_text is not null
            limit %s
            '''
        self.cursor.execute(news_sql, (category_id, category_id, category_id, limit))
        news = self.cursor.fetchall()
        return news

    def get_news_by_url(self, url):
        news_sql = '''
            select n.id, n.url, n.category_id, c.xpath_text, c.charset, n.text_state, n.attachment_state
            from news n 
            join category c on c.id = n.category_id
            where n.url = %s
            '''
        self.cursor.execute(news_sql, url)
        news = self.cursor.fetchall()
        return news

    def text_func(self, new, html):
        text_sql = '''
        replace into texts (news_id, text) values (%s, %s)
        '''
        news_update_text_sql = '''
        update news set text_state = '1' where id = %s
        '''
        news_id, url, category_id, xpath, charset, text_state, attachment_state = new
        text = None
        text_error = None
        try:
            text = html.text.strip()
            # 保存正文内容
            self.cursor.execute(text_sql, (news_id, text))
            # 改变正文已抓取状态
            self.cursor.execute(news_update_text_sql, news_id)
            self.conn.commit()
        except Exception as e:
            print('爬取正文:{}出错，error: {}'.format(url, traceback.print_exc()))
            text_error = repr(e)
            self.conn.rollback()
        return [text, text_error]


    def attachment_func(self, new, html):
        news_update_attachment_sql = '''
        update news set attachment_state = %s where id = %s
        '''
        attachments_sql = '''
        insert into attachments (news_id, path, name, format) values (%s, %s, %s, %s)
        '''
        news_id, url, category_id, xpath, charset, text_state, attachment_state = new
        attachment_results = []
        attachments_error = None
        try:
            # 保存新闻附件
            attachments = []
            links = html.find_elements_by_tag_name('a')
            for link in links:
                href = link.get_attribute('href')
                if re.match('.*\.(xls|xlsx|doc|docx|pdf)', str.lower(href)):
                    attachments.append(base_util.attachment_format(link, url, href))
            videos = html.find_elements_by_tag_name('video')
            for video in videos:
                src = video.get_attribute('src')
                if re.match('.*\.(webm|mp4|ogg)', str.lower(src)):
                    attachments.append(base_util.attachment_format(video, url, src))
            images = html.find_elements_by_tag_name('img')
            for img in images:
                src = img.get_attribute('src')
                if re.match('.*\.(jpg|jpeg|gif|png|bmp)', str.lower(src)):
                    attachments.append(base_util.attachment_format(img, url, src))
            # 附件下载状态， 0-未下载，1-已下载或忽略下载
            attachment_state = '0'
            for attachment in attachments:
                file_url, file_format, file_name = attachment
                # print('附件下载，路径：{}, 格式：{}， 名称：{}'.format(file_url, file_format, file_name))
                attachment_result = {
                    'url': file_url,
                    'format': file_format,
                    'name': file_name,
                    'download_flag': None,
                    'too_small_flag': None,
                }
                source_dir = config.ATTACHMENT_DIR
                # 确定保存附件的文件夹
                file_dir = time.strftime('%Y/%m/%d')
                attachment_dir = os.path.join(source_dir, file_dir)
                if not os.path.exists(attachment_dir):
                    os.makedirs(attachment_dir)
                attachment_name = time.strftime('%H%M%S') + '_' + base_util.random_str() + '.' + file_format
                file_path = os.path.join(file_dir, attachment_name)
                attachment_path = os.path.join(source_dir, file_path)
                attachment_path = base_util.url_linux(attachment_path)
                # 50k以下的图片，不下载
                download_flag, too_small_flag = base_util.download_file(file_url, attachment_path, 50 * 1024)
                if download_flag:
                    attachment_state = '1'
                    if not too_small_flag:
                        self.cursor.execute(attachments_sql, (news_id, base_util.url_linux(file_path), file_name, file_format))
                attachment_result['download_flag'] = download_flag
                attachment_result['too_small_flag'] = too_small_flag
                attachment_results.append(attachment_result)
            # 如果没有附件下载，也变为已完成
            if len(attachments) == 0:
                attachment_state = '1'
            # 修改附件下载状态
            self.cursor.execute(news_update_attachment_sql, (attachment_state, news_id))
            # 每条新闻的正文、附件等数据保存合并提交
            self.conn.commit()
        except Exception as e:
            print('下载附件:{}出错，error: {}'.format(url, traceback.print_exc()))
            attachments_error = repr(e)
            self.conn.rollback()
        return [attachment_results, attachments_error]

    def crawl_text(self, news):
        text_results = {}
        for new in news:
            news_id, url, category_id, xpath, charset, text_state, attachment_state = new
            print('抓取正文和附件，url:{}, xpath:{}'.format(url, xpath))
            text_result = {
                'news_id': news_id,
                'url': url,
                'category_id': category_id,
                'xpath': xpath,
                'charset': charset,
                'text': None,
                'attachments': None,
                'text_error': None,
                'attachments_error': None
            }
            self.browser.get(url)
            html = self.browser.find_element_by_xpath(xpath)
            if text_state == '0':
                text_thread = ResultThread(self.text_func, (new, html))
                text_thread.start()
                # 等待子线程结束后，再往后面执行
                text_thread.join()
                text_result['text'], text_result['text_error'] = text_thread.get_result()
                if text_result['text_error'] is not None:
                    print('抓取正文结果，state: {}'.format(text_result['text_error']))
                else:
                    print('抓取正文结果，state: {}'.format('成功'))
            if attachment_state == '0':
                attachment_thread = ResultThread(self.attachment_func, (new, html))
                attachment_thread.start()
                attachment_thread.join()
                text_result['attachments'], text_result['attachments_error'] = attachment_thread.get_result()
                if text_result['attachments_error'] is not None:
                    print('抓取附件结果, state: {}'.format(text_result['attachments_error']))
                else:
                    print('抓取附件结果，state: {}'.format('成功'))
            text_results[url] = text_result
        return text_results

    def crawler(self):
        try:
            # 初始化conn、cursor和browser
            self.init()
            # 抓取标题和正文附件没有关联关系，所有使用多线程加快速度
            # 爬取标题数据
            ResultThread(self.crawl_title, (self.get_categories(),)).start()
            # self.crawl_title(self.get_categories())
            # 爬取正文数据
            ResultThread(self.crawl_text, (self.get_news(),)).start()
            # self.crawl_text(self.get_news())
            print('批次结束')
        except Warning as w:
            print('warning: {}'.format(w))
        except Exception as e:
            print('error: {}'.format(traceback.print_exc()))
        # finally:
        # ResultThread(close_conn).start()
        #     self.close_browser()

    def test(self, request_id):
        result = {
            'errorCode': '0001',
            'errorMsg': '抓取标题失败',
            'data': None
        }
        try:
            self.init()
            title_results = self.crawl_title(self.get_categories(request_id))
            # 如果爬取的结果不为空，取其中一条
            for key, value in title_results.items():
                news = value.get('news')
                # 如果标题中新闻页列表不为空，取第一条
                result['errorCode'] = '0002'
                result['errorMsg'] = '抓取正文或附件失败'
                for i, new in enumerate(news):
                    url = new.get('url')
                    text_results = self.crawl_text(self.get_news_by_url(url))
                    new.update(text_results.get(url))
                    news[i] = new
                    result['errorCode'] = '0000'
                    result['errorMsg'] = '抓取成功'
                    # 成功获取一条新闻的正文和附件后退出循环
                    break
                value['news'] = news
                result['data'] = value
                break
        except Warning as w:
            print('warning: {}'.format(w))
        except Exception as e:
            print('error: {}'.format(traceback.print_exc()))
            result['errorCode'] = '9999'
            result['errorMsg'] = repr(e)
        # finally:
        #     self.close_conn()
        #     self.close_browser()
        return result

    def close_conn(self):
        if self.conn is not None:
            self.conn.close()

    def close_browser(self):
        if self.browser is not None:
            self.browser.close()

class ResultThread(threading.Thread):
    def __init__(self, func, args=()):
        super(ResultThread, self).__init__()
        self.func = func
        self.args = args
        self.result = None

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except:
            return None

class CrawlThread (threading.Thread):
    def __init__(self, crawler):
        threading.Thread.__init__(self)
        self.crawler = crawler

    def run(self):
        if config.SCHEDULE:
            self.crawler.crawler()
            schedule.every(30).minutes.do(self.crawler.crawler)
            while True:
                schedule.run_pending()

# 创建一个实例
crawler_instance = Crawler()
CrawlThread(crawler_instance).start()

app = Flask(__name__)

# 测试网站爬取的参数是否设置正确
@app.route("/crawler/test", methods=['POST'])
def test():
    request_id = request.form['id']
    result = crawler_instance.test(request_id)
    # 返回json数据
    return jsonify(result)

app.run(host=config.FLASK['host'], port=config.FLASK['port'])


