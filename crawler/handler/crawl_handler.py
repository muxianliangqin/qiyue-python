import pymysql
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options

from crawler.config.config_yml import get as config_get


def get_web_driver(port):
    """
    静态方法，创建chrome流程器
    :param port: 浏览器启动的端口，如不设定，每次启动都会使用新端口
    :return:
    """
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('blink-settings=imagesEnabled=false')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-plugins')
    chrome_options.add_argument('--disable-images')
    chrome_options.add_experimental_option("prefs", {
        'profile.default_content_settings.popups': 0,
        'download.default_directory': 'D:\\workspace'
    })
    return webdriver.Chrome(port=port, options=chrome_options)


def get_mysql_conn():
    return pymysql.connect(host=config_get('mysql.host'), port=config_get('mysql.port'),
                           user=config_get('mysql.user'), password=config_get('mysql.password'),
                           db=config_get('mysql.db'), charset=config_get('mysql.charset'))


def get_browser(browser=None):
    if browser is None:
        browser = get_web_driver(port=config_get('selenium.chrome.port'))
    # 浏览器对象不可用，重新创建
    try:
        # 获取浏览器句柄，也就是标签页
        handle = browser.current_window_handle
    except WebDriverException as e:
        print('重新创建browser，error:{}'.format(e))
        browser = get_web_driver(port=config_get('selenium.chrome.port'))
    return browser


def get_conn(conn=None):
    """
    初始化mysql连接
    :return:
    """
    try:
        if conn is None:
            conn = get_mysql_conn()
        conn.ping(reconnect=True)
    except Exception as e:
        print('mysql失去连接，重新获取'.format(repr(e)))
        conn = get_mysql_conn()
    return conn

