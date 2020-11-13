#!/usr/bin/python
import json
import os
import sys
import traceback

from flask import Flask
from kafka import KafkaConsumer

# crawler的父目录加入系统搜索目录，以使得在linux系统中可以import自定义的模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from crawler.handler.crawl_handler import get_browser, get_conn
from crawler.spider.no_login_web import crawl_column, crawl_article, ColumnResult, ArticleResult
from crawler.utils.unique_id import next_id
from crawler.config.config_yml import get as config_get
from crawler.utils.base import str_exception

BROWSER = get_browser()
CONN = get_conn()
FLASK_HOST = config_get('flask.host')
FLASK_PORT = config_get('flask.port')
KAFKA_BOOTSTRAP_SERVERS = config_get('spring.kafka.bootstrap-servers')
KAFKA_TOPIC = config_get('kafka.topic.crawler')


def crawl_title(column_id):
    browser = get_browser(BROWSER)
    conn = get_conn(CONN)
    cursor = conn.cursor()
    column_update_error = '''
        update `column` set
        error = %s
        where column_id = %s
        '''
    try:
        # 从数据库查询是否存在columnId的数据
        column_select_by_id = '''
        select column_id, url, xpath_article_title, xpath_article_page, begin_page, crawled_page, charset 
        from `column` where column_id = %s
        '''
        cursor.execute(column_select_by_id, (column_id,))
        res = cursor.fetchone()
        if res is None:
            print('网站栏目不存在column_id：{}'.format(column_id))
            return
        column_id, url, xpath, xpath_article_page, begin_page, crawled_page, charset = res
        # 创建ColumnResult对象
        column_result = ColumnResult(column_id=column_id, url=url, xpath_article_title=xpath,
                                     xpath_article_page=xpath_article_page, begin_page=begin_page,
                                     crawled_page=crawled_page, charset=charset)
        # 爬取栏目中文章标题列表，以及历史数据（下一页）
        column_result = crawl_column(browser, column_result)
        article_insert_sql = '''
        insert into article (article_id, url, title, column_id)
        select %s, %s, %s, %s from dual 
        where not exists (select 1 from article n where n.url = %s)
        '''
        for article_result in column_result.article_list:
            article_id, url, title, column_id \
                = next_id(), article_result.url, article_result.title, article_result.column_id
            cursor.execute(article_insert_sql, (article_id, url, title, column_id, url))
        current_page, crawled_page = column_result.current_page, column_result.crawled_page
        if crawled_page == 1 or current_page > begin_page:
            column_update_page = '''
            update `column` set
                begin_page = %s, 
                crawled_page = %s
            where column_id = %s
            '''
            # 更新获取历史数据的结果信息
            cursor.execute(column_update_page, (current_page, crawled_page, column_id))
        if column_result.error is not None:
            # 更新错误信息
            cursor.execute(column_update_error, (column_result.error, column_id))
        conn.commit()
        print('爬取网站栏目文章列表成功column_id:{}, url: {}, '.format(column_id, url))
    except Exception as e:
        conn.rollback()
        print('爬取网站栏目标题失败column_id:{}, error: {}'.format(column_id, traceback.format_exc()))
        cursor.execute(column_update_error, (str_exception(), column_id))
        conn.commit()


def crawl_text(article_id):
    browser = get_browser(BROWSER)
    conn = get_conn(CONN)
    cursor = conn.cursor()
    article_update_error = '''
    update article set
    error = %s
    where article_id = %s
    '''
    try:
        article_select_by_id = '''
        select a.article_id, a.url, a.crawled_content, a.crawled_html, a.crawled_attachment,
             a.crawled_num, c.charset, c.xpath_article_content 
            from article a 
            join `column` c on c.column_id = a.column_id
            where a.article_id = %s
        '''
        cursor.execute(article_select_by_id, (article_id,))
        article_id, url, crawled_content, crawled_html, crawled_attachment, crawled_num, charset, xpath_article_content = cursor.fetchone()
        if article_id is None:
            print('article_id:{} 的数据不存在'.format(article_id))
            return
        article_result = ArticleResult(article_id=article_id, url=url, xpath=xpath_article_content,
                                       crawled_content=crawled_content, crawled_html=crawled_html,
                                       crawled_attachment=crawled_attachment, crawled_num=crawled_num, charset=charset)
        article_result = crawl_article(browser, article_result)
        attachment_list = article_result.attachment_list
        attachments = []
        file_select_by_md5 = '''
        select file_id, path, name, format, size, md5 from `file` where md5 = %s
        '''
        file_insert = '''
        insert into `file` (file_id, path, name, format, size, md5) 
        values (%s, %s, %s, %s, %s, %s)
        '''
        if attachment_list is not None:
            for attachment in attachment_list:
                file_id, path, name, fmt, size, md5 = next_id(), attachment.path, attachment.name, \
                                                      attachment.fmt, attachment.size, attachment.md5
                cursor.execute(file_select_by_md5, (md5,))
                res = cursor.fetchone()
                if res is not None:
                    sql_file_id, sql_path, sql_name, sql_fmt, sql_size, sql_md5 = res
                    print('文件已存在，file_id：{}'.format(sql_file_id))
                    attachments.append({
                        'fileId': sql_file_id,
                        'name': sql_name
                    })
                else:
                    cursor.execute(file_insert, (file_id, path, name, fmt, size, md5))
                    attachments.append({
                        'fileId': file_id,
                        'name': name
                    })
        if article_result.text is not None:
            print('获取正文text成功')
            update_article_text = '''update article set text = %s, crawled_content = %s where article_id = %s'''
            cursor.execute(update_article_text, (article_result.text, article_result.crawled_content, article_id))
        if article_result.html is not None:
            print('获取正文html成功')
            update_article_html = '''update article set html = %s, crawled_html = %s where article_id = %s'''
            cursor.execute(update_article_html, (article_result.html, article_result.crawled_html, article_id))
        attachments_json = None
        if len(attachments) > 0:
            attachments_json = json.dumps(attachments, ensure_ascii=False)
        if attachments_json is not None or article_result.crawled_attachment == 1:
            print('获取附件成功')
            update_article_attachments = '''
            update article set attachments = %s, crawled_attachment = %s 
                where article_id = %s
            '''
            cursor.execute(update_article_attachments,
                           (attachments_json, article_result.crawled_attachment, article_id))
        print('更新抓取次数：{}'.format(article_result.crawled_num))
        update_article_num = '''update article set crawled_num = %s where article_id = %s'''
        cursor.execute(update_article_num, (article_result.crawled_num, article_id))
        if article_result.error is not None:
            print('抓取正文等失败')
            cursor.execute(article_update_error, (article_result.error, article_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print('爬取网站正文失败：{}'.format(traceback.format_exc()))
        cursor.execute(article_update_error, (str_exception(), article_id))
        conn.commit()


def crawl_consumer():
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','))
    for msg in consumer:
        message = str(msg.value, encoding='utf-8')
        message = json.loads(message, strict=False)
        label = message['label']
        if label == 'crawlTitle':
            title = message['data']
            crawl_title(title['columnId'])
        elif label == 'crawlArticle':
            article = message['data']
            crawl_text(article['articleId'])
        else:
            pass


app = Flask(__name__)
with app.app_context():
    crawl_consumer()
    app.run(host=FLASK_HOST, port=FLASK_PORT)

# 测试网站爬取的参数是否设置正确
# @app.route("/crawler/test", methods=['POST'])
# def test():
#     request_body = request.get_json()
#     request_id = request_body['params']['id']
#     result = crawler_instance.test(request_id)
#     # 返回json数据
#     return jsonify(result)
