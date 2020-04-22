#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random, re, requests, os, time
from urllib.parse import urlparse, urlunparse

def url_format(url_absolute, url):
    """
    此方法用于格式化url路径
    :param url_absolute:绝对路径
    :param url:需要格式化的路径
    :return : dict
    list = [url_format_bool:是否格式化成功,url_format_absolute:url格式化成绝对路径，url_format_relative:url格式化成最简相对路径]
    """
    # 格式化结果标记
    # print('url_absolute=%s,url=%s' % (url_absolute,url))
    format_bool = True
    url_abs_parse = urlparse(url_absolute)
    if not url_abs_parse.netloc:
        return [False, False, url_absolute, url]
    url_parse = urlparse(url)
    url_abs_levels = url_abs_parse.path.split('/')
    url_levels = url_parse.path.split('/')
    url_abs_copy = url_abs_levels.copy()
    url_levels_copy1 = url_levels.copy()
    url_levels_copy2 = url_levels.copy()
    # url 为绝对路径
    if url_parse.netloc:
        # 域名不同，格式化失败
        if url_parse.netloc != url_abs_parse.netloc:
            return [True, True, url, url]
        # 从左至右判断是否存在相同级别名称，如有则删除
        for i, url_abs in enumerate(url_abs_levels):
            if len(url_levels) > i:
                url_rel = url_levels[i]
                if url_abs == url_rel:
                    url_abs_copy.remove(url_abs)
                    url_levels_copy2.remove(url_rel)
                else:
                    break
            else:
                break
        # 根据绝对路径剩余的层级数，添加相应的相对符号
        for i in range(len(url_abs_copy)):
            if i == 0:
                url_levels_copy2.insert(0, '.')
            elif i == 1:
                url_levels_copy2[0] = '..'
            else:
                url_levels_copy2.insert(0, '..')
        return url_format(url_absolute, '/'.join(url_levels_copy2))
    else:
        for i, url_level in enumerate(url_levels):
            # url相对根目录
            if url_level == '':
                if len(url_levels) == 1:
                    url_levels_copy1.clear()
                    url_levels_copy1.clear()
                else:
                    if url_level != url_levels[-1]:
                        url_abs_copy.clear()
                        url_levels_copy1 = url_levels[1:]
                        url_levels_copy2 = url_levels[1:]
                # 根据绝对路径剩余的层级数，添加相应的相对符号
                url_abs_copy2 = [i for i in url_abs_copy if i not in ['', '.', '..']]
                for i in range(len(url_abs_copy2)):
                    if i == 0:
                        url_levels_copy2.insert(0, '.')
                    elif i == 1:
                        url_levels_copy2[0] = '..'
                    else:
                        url_levels_copy2.insert(0, '..')
                break
            # url同级
            elif url_level == '.':
                if url_abs_copy:
                    url_abs_copy.pop()
                url_levels_copy1.remove(url_level)
            # url往上一级
            elif url_level == '..':
                if url_abs_copy:
                    url_abs_copy.pop()
                url_levels_copy1.remove(url_level)
                if i == 0:
                    if url_abs_copy:
                        url_abs_copy.pop()
            else:
                # url无层级符号，也就是同级
                if url_levels[0] not in ['', '.', '..']:
                    if url_abs_copy:
                        url_abs_copy.pop()
                # url和url_absolute存在层级重复
                elif url_level in url_abs_copy and url_level != url_levels[-1]:
                    url_abs_copy.remove(url_level)
                    url_levels_copy2.remove(url_level)
                else:
                    pass

        formatted_absolute = urlunparse([url_abs_parse.scheme, url_abs_parse.netloc,
                                         '/'.join(url_abs_copy + url_levels_copy1), '', '', ''])
        formatted_relative = urlunparse(['', '', '/'.join(url_levels_copy2), '', '', ''])
    formatted = [format_bool, False, formatted_absolute, formatted_relative]
    return formatted


def random_str():
    rand_str =  ''.join([str(random.choice(range(10))) for i in range(10)])
    return rand_str


def url_linux(url):
    url = re.sub(r'\\+', '/', url)
    url = re.sub(r'/+', '/', url)
    return url

def attachment_format(ele, sup_url, url):
    '''
    :param ele: 附件所在的js节点
    :param sup_url: 附件所在的页面的url
    :param url: 附件的url
    :return:
    '''
    # 附加的绝对路径
    file_url = url_format(sup_url, url)[2]
    # 附件格式
    file_format = file_url[str.rindex(file_url, '.') + 1:]
    # 附件原名称
    if ele.text:
        file_name = ele.text.strip()
    else:
        url_temp = file_url[:str.rindex(file_url, '.')]
        file_name = url_temp[str.rindex(url_temp, '/') + 1:]
    return [file_url, file_format, file_name]


def download_file(url, path, min_size, try_time=0):
    '''
    下载文件
    :param url:
    :param path:
    :param min_size 文件最小尺寸
    :param try_time 尝试下载次数
    :return: [是否下载成功, 是否文件太小]
    '''
    download_flag = False
    too_small_flag = False
    if try_time > 0:
        print('尝试下载次数:{}'.format(try_time))
    max_try_time = 5
    if try_time >= max_try_time:
        os.remove(path)
        return [False, False]
    start_index = 0
    if os.path.exists(path):
        start_index = os.path.getsize(path)
    headers = {
        'Range': 'bytes:{}-'.format(start_index)
    }
    with requests.get(url, headers, stream=True) as response:
        http_size = int(response.headers['Content-Length'])
        # 如果是图片，并且大小小于min_size，则不下载
        if re.match('.*\.(jpg|jpeg|gif|png|bmp)', str.lower(url)) and http_size < min_size:
            too_small_flag = True
            download_flag = True
        else:
            with open(path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            local_size = os.path.getsize(path)
            # 如果http返回的文件大小和下载下来的文件大小不一致，重新下载文件
            if http_size != local_size:
                try_time += 1
                return download_file(url, path, min_size, try_time)
            else:
                download_flag = True
    return [download_flag, too_small_flag]


def cur_time():
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))


def str_time(ti):
    hour = int(ti) // (60 * 60)
    ti -= hour * (60 * 60)
    minute = int(ti) // 60
    ti -= minute * 60
    second = int(ti)
    ti -= second
    millisecond = int(ti * 1000)
    return '{}小时 {}分钟 {}秒 {}毫秒'.format(hour,minute,second,millisecond)