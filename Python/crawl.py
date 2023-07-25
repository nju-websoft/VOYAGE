import time
import requests
import os
from urllib.parse import unquote
import logging
from logging import handlers
from multiprocessing.pool import ThreadPool
import json

from config import DATA_PATH


class Logger(object):
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }

    def __init__(self, filename, level='warning', when='D', backCount=3,
                 fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)
        self.logger.setLevel(self.level_relations.get(level))
        sh = logging.StreamHandler()
        sh.setFormatter(format_str)
        th = logging.handlers.TimedRotatingFileHandler(filename=filename, when=when, backupCount=backCount,
                                                       encoding='utf-8')
        th.setFormatter(format_str)
        self.logger.addHandler(sh)
        self.logger.addHandler(th)


class CrawlDump(object):
    def __init__(self, save_path, log_path, pool_num=1000):
        self.save_path = save_path
        files = os.listdir(save_path)
        self.existed_set = {x for x in files if os.path.getsize(save_path + x) > 0}
        self.log = Logger(log_path, level='info')
        self.pool_num = pool_num

    def get_response(self, url, retry=5):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3371.0 Safari/537.36',
            'Accept': 'application/rdf+xml; q=1, application/xhtml+xml; q=0.8, text/turtle; q=0.7, application/xml; q=0.4, text/xml; q=0.3'
        }
        proxies = {"http": None, "https": None}
        timeout = 20
        try:
            r = requests.get(url, stream=True, headers=headers, verify=False, proxies=proxies, timeout=timeout)
            return r
        except Exception as e:
            if retry > 0:
                return self.get_response(url, retry - 1)
            else:
                return None

    def url_response(self, args):
        path = '_'.join(str(x) for x in args)
        url = args[0], args[-1]
        if path in self.existed_set:
            return 1, path  # 1: file is existed
        try:
            r = self.get_response(url)
            if r is None:
                return 2, path  # 2: response is None
            if r.status_code // 100 == 3:
                r = self.get_response(r.url)
            if r.status_code == 200:
                url = r.url
                headers = r.headers
                filename = ''
                if 'Content-Disposition' in headers and headers['Content-Disposition']:
                    disposition_split = headers['Content-Disposition'].split(';')
                    if len(disposition_split) > 1:
                        if disposition_split[1].strip().lower().startswith('filename='):
                            file_name = disposition_split[1].split('=')
                            if len(file_name) > 1:
                                filename = unquote(file_name[1])
                if not filename and os.path.basename(url):
                    filename = os.path.basename(url).split("?")[0]
                path = self.save_path + path + filename[filename.find('.'):]
                with open(path, "wb") as f:
                    f.write(r.content)
                self.log.logger.info('save file: {}({})'.format(path, filename))
                return 0, path  # 0: successfully
        except Exception as e:
            # print(e)
            return 3, path  # 3: error, catch exception
        return 4, path  # 4: status code != 200

    def multi_crawl_url(self):
        start = time.time()
        self.delete_empty_file()
        with open(DATA_PATH + 'datasets.json', 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        data = []
        for it in json_data:
            for dp in it['dump file URLs']:
                data.append((it['ID'], dp))
        with ThreadPool(1000) as pool:
            ret_iter = pool.imap(self.url_response, data)
            for ret in ret_iter:
                self.log.logger.debug(ret)
        self.log.logger.info(f"Time to download: {time.time() - start}")
        self.log.logger.debug('----')

    def delete_empty_file(self):
        files = os.listdir(self.save_path)
        for x in files:
            file = self.save_path + x
            if os.path.isfile(file) and os.path.getsize(file) == 0:
                os.remove(file)
                self.log.logger.info('delete empty file: ' + file)


if __name__ == '__main__':
    cd = CrawlDump('dumps/', 'logs/')
    cd.multi_crawl_url()
