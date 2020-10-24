import yaml
import os
import threading

BASE_DIR = r'D:\application\workspace\qiyue-config'
SEARCH_PATHS = r'config\qiyue'
PROFILE = 'prod'
SOURCE = {
    'python': {
        'name': 'crawler',
        'format': 'yml'
    },
    'java': {
        'name': 'custom,kafka',
        'format': 'yml'
    }
}


def load_config_file(file_path):
    """
    读取配置文件
    :param file_path: 文件路径
    :return:
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def load_config_files():
    """
    读取所有配置
    :return:
    """
    config = dict()
    for (k, v) in SOURCE.items():
        fmt = v['format']
        if fmt == 'yml':
            names = v['name'].split(',')
            if names is None:
                continue
            for name in names:
                path = os.path.join(BASE_DIR, SEARCH_PATHS, k, '{}-{}.{}'.format(name, PROFILE, fmt))
                config.update(load_config_file(path))
    return config


class Config:
    _instance_lock = threading.Lock()
    config = None

    # 实现单例模式
    def __new__(cls, *args, **kwargs):
        if not hasattr(Config, "_instance"):
            with Config._instance_lock:
                if not hasattr(Config, "_instance"):
                    Config._instance = object.__new__(cls)
        return Config._instance

    def __init__(self):
        self.config = load_config_files()

    def get(self, key, default=None):
        v = self.config
        ks = key.split('.')
        for k in ks:
            v = v.get(k)
            if v is None:
                return default
        return v


def get(key, default=None):
    config = Config()
    return config.get(key, default)
