import os
import re
import threading
import time
import traceback

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from crawler.config.config_yml import get as config_get
from crawler.utils import base as base_util

ATTACHMENT_CRAWLER_DIR = config_get('file.attachment.crawler.dir')
INVALID_TITLE = config_get('crawler.column.invalid.title')
INVALID_HREF = config_get('crawler.column.invalid.href')
HTML_CLEAN_REGEXP = config_get('crawler.article.html.clean.regexp')


class ColumnResult:
    column_id = None
    url = None
    xpath_article_title = None
    xpath_article_page = None
    begin_page = None
    crawled_page = None
    charset = None
    article_list = None
    error = None
    current_page = 0

    def __init__(self, column_id=None, url=None, xpath_article_title=None, xpath_article_page=None, begin_page=None,
                 crawled_page=None, charset=None, article_list=None, error=None):
        if article_list is None:
            article_list = []
        self.column_id = column_id
        self.url = url
        self.xpath_article_title = xpath_article_title
        self.xpath_article_page = xpath_article_page
        self.begin_page = begin_page
        self.crawled_page = crawled_page
        self.charset = charset
        self.article_list = article_list
        self.error = error


class ArticleResult:
    column_id = None
    article_id = None
    url = None
    title = None
    xpath = None
    text = None
    html = None
    attachment_list = None
    crawled_text = None
    crawled_html = None
    crawled_attachment = None
    crawled_num = None
    error = None
    charset = None

    def __init__(self, column_id=None, article_id=None, url=None, title=None, xpath=None, text=None, html=None,
                 attachment_list=None, error=None, crawled_text=0, crawled_html=0, crawled_attachment=0, crawled_num=0,
                 charset=None):
        self.column_id = column_id
        self.article_id = article_id
        self.url = url
        self.title = title
        self.xpath = xpath
        self.text = text
        self.html = html
        self.attachment_list = attachment_list
        self.crawled_text = crawled_text
        self.crawled_html = crawled_html
        self.crawled_attachment = crawled_attachment
        self.crawled_num = crawled_num
        self.error = error
        self.charset = charset


class CrawlThread(threading.Thread):
    def __init__(self, func, args=()):
        super(CrawlThread, self).__init__()
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


def crawl_column(browser, column_result):
    column_id, url, xpath_article_title, charset, xpath_article_page, begin_page, crawled_page = \
        column_result.column_id, column_result.url, column_result.xpath_article_title, column_result.charset, \
        column_result.xpath_article_page, column_result.begin_page, column_result.crawled_page
    print('\n时间：{}，网站栏目获取文章列表：{}, xpath:{}'.format(base_util.cur_time(), url, xpath_article_title))
    try:
        browser.get(url)
        # 显性等待，等待xpath的元素出现程序才往下运行
        xpath_html = WebDriverWait(browser, 5, 0.5).until(
            ec.presence_of_element_located((By.XPATH, xpath_article_title))
        )
        get_article_list(xpath_html, column_result)
        # 历史数据是否已爬取完毕
        if crawled_page == 0:
            print('开始爬取历史数据')
            # 跳转到指定页数
            success_jump_to = jump_to_specific_page(browser, column_result)
            if not success_jump_to:
                return column_result
            # 爬取下一页是否成功、是否爬取完所有历史数据
            success, current_page = True, column_result.begin_page
            # 没有爬取完所有历史数据
            while success:
                success, current_page_next = crawl_next_page(browser, column_result)
                # 当最大页数和当前页数相等，跳出循环
                if current_page >= current_page_next:
                    column_result.crawled_page = 1
                    column_result.begin_page = current_page_next
                    break
                current_page = column_result.current_page
            column_result.current_page = current_page
        else:
            column_result.current_page = column_result.begin_page
    except TimeoutException as e:
        column_result.error = '链接url:{},获取指定xpath：{},超时'.format(url, xpath_article_title)
    except Exception as e:
        column_result.error = '链接url:{},获取指定xpath：{},异常，error: {}' \
            .format(url, xpath_article_title, base_util.str_exception())
        print(column_result.error)
    return column_result


def crawl_next_page(browser, column_result):
    xpath_article_page, begin_page, xpath_article_title, url, column_id = \
        column_result.xpath_article_page, column_result.begin_page, column_result.xpath_article_title, \
        column_result.url, column_result.column_id
    pages = get_links(browser, xpath_article_page)
    #  获取最大页数的下标
    max_page_index, max_page_number = get_specific_link_index(pages)
    if max_page_index == -1:
        return False, max_page_number
    if max_page_index == len(pages) - 1:
        print('最大页数之后没有下一页链接，可以认为已近到最后一页，爬取历史数据完毕。总共{}页'.format(max_page_number))
        column_result.crawled_page = 1
        column_result.current_page = max_page_number
        column_result.error = None
        return True, max_page_number
    next_page_link = pages[max_page_index + 1]
    if next_page_link is None:
        return False, max_page_number
    # 模拟点击下一页
    next_page_link.click()
    column_result.current_page += 1
    try:
        next_page_html = get_html(browser, xpath_article_title)
        get_article_list(next_page_html, column_result)
        print('已爬取第{}页历史数据，url：{}'.format(column_result.current_page, browser.current_url))
        return True, max_page_number
    except TimeoutException as e:
        error = '链接url:{},获取指定xpath：{}超时，error: {}' \
            .format(browser.current_url, xpath_article_title, traceback.print_exc())
        column_result.error = error
        return False, max_page_number
    except Exception as e:
        print(repr(e))
        error = '链接url:{},获取指定xpath：{}异常，error: {}' \
            .format(browser.current_url, xpath_article_title, traceback.print_exc())
        column_result.error = error
        return False, max_page_number


def jump_to_specific_page(browser, column_result):
    xpath_article_page, begin_page = column_result.xpath_article_page, column_result.begin_page
    pages = get_links(browser, xpath_article_page)
    # 跳转到指定页数
    page_index, page_number = get_specific_link_index(pages, begin_page)
    # 当获取的页数大于begin_page，表明已经过了跳转指定页的步骤
    if page_number <= begin_page:
        while page_number != begin_page:
            # 模拟点击，跳转到指定页
            specific_page = pages[page_index]
            specific_page.click()
            pages = get_links(browser, xpath_article_page)
            page_index, page_number_next = get_specific_link_index(pages, begin_page)
            # 如果第二次执行下一页，获得的最大页数与上一次相同，表明已近到最后一页，跳出循环
            if page_number == page_number_next:
                break
            else:
                page_number = page_number_next
        # 跳出循环后，如果当前页数等于指定页数，成功跳转
        if page_number == begin_page:
            # 设置当前页数
            print('成功跳转到指定{}页数'.format(begin_page))
            # 模拟点击，跳转到指定页
            specific_page = pages[page_index]
            specific_page.click()
            column_result.current_page = begin_page
        else:
            column_result.error = '仅能跳转到最大页数：{}，或许指定页数设置值：{}太大，请重新设置'.format(page_number, begin_page)
            print(column_result.error)
            return False
    return True


def get_html(browser, xpath):
    return WebDriverWait(browser, 10, 0.5).until(
        ec.presence_of_element_located((By.XPATH, xpath))
    )


def get_links(browser, xpath):
    html = get_html(browser, xpath)
    return html.find_elements_by_tag_name('a')


def get_specific_link_index(pages, specific_page=-1):
    """
    获取指定分页的下标
    :param pages: 分页区的a链接
    :param specific_page: 指定页数
    :return: 如果指定页数，但未能获取，返回最大页数
    """
    # 指定页数必须大于0
    if specific_page <= 0:
        specific_page = -1
    page_index = -1
    page_number = -1
    reg_number = r'^\d+$'
    for i, a in enumerate(pages):
        text = a.text
        if text is not None:
            text = text.strip()
        if re.match(reg_number, text):
            page_index = i
            page_number = int(text)
            if specific_page > 0 and specific_page == page_number:
                return page_index, page_number
    return page_index, page_number


def get_article_list(html, column_result):
    if html is not None:
        url, column_id = column_result.url, column_result.column_id
        links = html.find_elements_by_tag_name('a')
        for a in links:
            href = a.get_attribute('href')
            title = a.get_attribute('title')
            if not title:
                title = a.text
            if re.match(INVALID_TITLE, title) or re.match(INVALID_HREF, href):
                continue
            href = base_util.url_format(url, href)[2]
            column_result.article_list.append(ArticleResult(column_id=column_id, url=href, title=title))


def crawl_attachment(article_result, attachment_html):
    url = article_result.url
    # 保存新闻附件
    attachments = []
    links = attachment_html.find_elements_by_tag_name('a')
    for link in links:
        href = link.get_attribute('href')
        if href is not None and re.match(r'.*\.(xls|xlsx|doc|docx|pdf)', str.lower(href)):
            attachments.append(base_util.attachment_format(link, url, href))
    videos = attachment_html.find_elements_by_tag_name('video')
    for video in videos:
        src = video.get_attribute('src')
        if src is not None and re.match(r'.*\.(webm|mp4|ogg)', str.lower(src)):
            attachments.append(base_util.attachment_format(video, url, src))
    images = attachment_html.find_elements_by_tag_name('img')
    for img in images:
        src = img.get_attribute('src')
        if src is not None and re.match(r'.*\.(jpg|jpeg|gif|png|bmp)', str.lower(src)):
            attachments.append(base_util.attachment_format(img, url, src))
    attachment_list = []
    errors = []
    attachment_success = True
    for attachment in attachments:
        file_url, file_format, file_name = attachment
        source_dir = ATTACHMENT_CRAWLER_DIR
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
        success, is_filter, message, file = base_util.download_file(file_url, attachment_path, 0)
        if is_filter:
            errors.append(message)
            print('获取附件成功：{}，但是根据规则过滤不下载:{}'.format(file_name, message))
        elif success:
            print('获取附件成功：{}，message:{}'.format(file_name, message))
            file.path = file_path
            file.name = file_name
            file.fmt = file_format
            attachment_list.append(file)
        else:
            print('获取附件失败：{}，message:{}'.format(file_name, message))
            attachment_success = False
            errors.append(message)
    article_result.attachment_list = attachment_list
    if len(errors) != 0:
        article_result.error = ','.join(errors)
    if attachment_success:
        article_result.crawled_attachment = 1
    else:
        article_result.crawled_attachment = 0
    return article_result


def crawl_article(browser, article_result):
    article_id, url, xpath, charset = article_result.article_id, article_result.url, article_result.xpath, \
                                      article_result.charset
    print('时间：{}, 抓取正文和附件，url:{}, xpath:{}'.format(base_util.cur_time(), url, xpath))
    article_result.crawled_num = article_result.crawled_num + 1
    try:
        browser.get(url)
        # 显性等待，等待xpath的元素出现程序才往下运行
        browser_source = WebDriverWait(browser, 5, 0.5).until(
            method=ec.presence_of_element_located((By.XPATH, xpath)),
            message='元素未加载'
        )
        if article_result.crawled_num > 5:
            article_result.error = '爬取网站正文次数已超过5次'
            return article_result
        if article_result.crawled_text == 0:
            html = browser_source.get_attribute('innerHTML')
            if html is not None:
                # '(\s+style=".*")|(\s+class=".*")'
                html = re.sub(HTML_CLEAN_REGEXP, '', html)
            article_result.html = html
            text = browser_source.text
            if text is not None:
                text = text.strip()
            article_result.text = text
            article_result.crawled_text = 1
            article_result.crawled_html = 1
        if article_result.crawled_attachment == 0:
            crawl_attachment(article_result, browser_source)
    except Exception as e:
        error = base_util.str_exception()
        print('爬取网页正文异常：{}', traceback.format_exc())
        article_result.error = error
    return article_result
