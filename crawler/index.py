#!/usr/bin/python
import json
import os
import sys

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
    print('开始爬取网站栏目数据，column_id: {}'.format(column_id))
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
        # 更新错误信息
        cursor.execute(column_update_error, (column_result.error, column_id))
        conn.commit()
        print('爬取网站栏目文章列表成功column_id:{}, url: {}, '.format(column_id, url))
    except Exception as e:
        conn.rollback()
        print('爬取网站栏目标题失败column_id:{}, error: {}'.format(column_id, repr(e)))
        cursor.execute(column_update_error, (str_exception(), column_id))
        conn.commit()


def crawl_text(article_id):
    print('开始爬取网站文章内容及附件：article_id: {}'.format(article_id))
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
        select a.article_id, a.url, a.crawled_text, a.crawled_html, a.crawled_attachment, a.content_id,
             a.crawled_num, c.charset, c.xpath_article_content 
            from article a 
            join `column` c on c.column_id = a.column_id
            where a.article_id = %s
        '''
        cursor.execute(article_select_by_id, (article_id,))
        (article_id, url, crawled_text, crawled_html, crawled_attachment, content_id, crawled_num, charset,
         xpath_article_content) = cursor.fetchone()
        if article_id is None:
            print('article_id:{} 的数据不存在'.format(article_id))
            return
        article_result = ArticleResult(article_id=article_id, url=url, xpath=xpath_article_content,
                                       crawled_text=crawled_text, crawled_html=crawled_html,
                                       crawled_attachment=crawled_attachment, crawled_num=crawled_num, charset=charset)
        article_result = crawl_article(browser, article_result)
        # 保存附件
        attachment_list = article_result.attachment_list
        if attachment_list is not None and len(attachment_list) > 0:
            attachments = []
            file_select_by_md5 = '''
            select file_id, path, name, format, size, md5 from `file` where md5 = %s
            '''
            file_insert = '''
            insert into `file` (file_id, path, name, format, size, md5) 
            values (%s, %s, %s, %s, %s, %s)
            '''
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
            attachments_json = json.dumps(attachments, ensure_ascii=False)
            print('获取附件成功')
            article_update_attachments = '''update article set attachments = %s where article_id = %s'''
            cursor.execute(article_update_attachments, (attachments_json, article_id))
        article_update_crawled_attachment = '''update article set crawled_attachment = %s where article_id = %s'''
        cursor.execute(article_update_crawled_attachment, (article_result.crawled_attachment, article_id))
        # 保存文章内容
        if article_result.text is not None or article_result.html is not None:
            if content_id is None:
                print('新增文章内容content记录')
                content_id = next_id()
                content_insert_sql = '''insert into content (content_id, text, html) values (%s, %s, %s)'''
                article_update_content_id = '''update article set content_id = %s where article_id = %s'''
                cursor.execute(content_insert_sql, (content_id, article_result.text, article_result.html))
                cursor.execute(article_update_content_id, (content_id, article_id))
            else:
                print('更新文章内容content数据')
                content_update_sql = '''update content set text = %s, html = %s where content_id = %s'''
                cursor.execute(content_update_sql, (article_result.text, article_result.html, content_id))
            if article_result.text is not None:
                print('更新文章获取text标记')
                article_update_crawled_text = '''update article set crawled_text = %s where article_id = %s'''
                cursor.execute(article_update_crawled_text, (1, article_id))
            if article_result.html is not None:
                print('更新文章获取html标记')
                article_update_crawled_html = '''update article set crawled_html = %s where article_id = %s'''
                cursor.execute(article_update_crawled_html, (1, article_id))
        # 更新爬取次数
        print('更新抓取次数：{}'.format(article_result.crawled_num))
        update_article_num = '''update article set crawled_num = %s where article_id = %s'''
        cursor.execute(update_article_num, (article_result.crawled_num, article_id))
        # 保存异常信息
        if article_result.error is not None:
            print('抓取正文等失败')
            cursor.execute(article_update_error, (article_result.error, article_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print('爬取网站正文失败：{}'.format(repr(e)))
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
