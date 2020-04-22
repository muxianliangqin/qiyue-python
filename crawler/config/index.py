#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, re

class Config(object):
    # mysql连接参数
    MYSQL = {
        'host': 'localhost',
        'port': 3306,
        'user': 'test',
        'password': 'root123',
        'db': 'crawler',
        'charset': 'utf8',
    }
    # flask启动参数
    FLASK = {
        'host': '0.0.0.0',
        'port': 7130
    }
    # 当前环境
    ENV = 'dev'
    # 附件保存目录
    ATTACHMENT_DIR = 'D:\\application\\workspace\\python\\crawler'

    # get attribute
    def __getitem__(self, key):
        return self.__getattribute__(key)


class ProductionConfig(Config):
    MYSQL = {
        'host': 'localhost',
        'port': 3306,
        'user': 'test',
        'password': 'Ub01D86$',
        'db': 'crawler',
        'charset': 'utf8',
    }

    FLASK = {
        'host': 'localhost',
        'port': 7030
    }

    ENV = 'prod'
    # 附件保存目录
    ATTACHMENT_DIR = '/home/git/file/attachment/crawler'

class DevelopmentConfig(Config):
    MYSQL = {
        'host': 'localhost',
        'port': 3306,
        'user': 'test',
        'password': 'root123',
        'db': 'crawler',
        'charset': 'utf8',
    }

    FLASK = {
        'host': '0.0.0.0',
        'port': 7130
    }

    ENV = 'dev'
    # 附件保存目录
    ATTACHMENT_DIR = 'D:\\application\\workspace\\python\\crawler'


mapping = {
    'dev': DevelopmentConfig,
    'prod': ProductionConfig,
    'default': DevelopmentConfig
}

APP_ENV = os.environ.get('APP_ENV', 'default').lower()
num = len(sys.argv) - 1  #参数个数
if num == 0:
    APP_ENV = 'default'
elif num > 1:
    exit("参数过多,需要0或1个值，实际值：{}个".format(num))
else:
    APP_ENV = sys.argv[1]  # 环境
    if not re.match(r'(dev|prod|default)', APP_ENV):
        exit("参数错误，环境变量只能如下之一：dev|prod|default")

config = mapping[APP_ENV]()  # 实例化对应的环境
