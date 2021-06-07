from Odyssey.interface import Session
from Odyssey.db_conf import DBConf
import os
import time
import warnings
import configparser
import fcntl

class Handler:
    def __init__(self, dbname, load_config=None):
        self.dbname = dbname
        log_len, idx_len, head_size, page_size, version = self.load_config(config_path=load_config)
        if os.path.exists(dbname):
            if load_config is not None:
                warnings.warn('The db is exist. Ignored the input configs.')
            self.db, status = Session.connect(db_name=self.dbname, head_size=None)
        else:
            self.db, status = Session.connect(db_name=self.dbname, head_size=head_size)
        if status:
            self.session = Session(self.db)
            self.session.load_config_from_head()
        else:
            self.session = Session(self.db, _HEAD_SIZE=head_size, _PAGE_SIZE=page_size, _IDX_LEN=idx_len, _LOG_LEN=log_len)
            self.session.compile_head(idx_len=idx_len, log_len=log_len, head_size=head_size, page_size=page_size, version=version, info=None)
    def load_config(self, config_path=None):
        if not config_path:
            log_len = DBConf.log_len
            idx_len = DBConf.idx_len
            head_size = DBConf.head_size
            page_size = DBConf.page_size
            version = DBConf.version
        else:
            cfg = configparser.ConfigParser()
            cfg.read(config_path)
            # print(cfg_dic)
            log_len = int(cfg.get('DB', 'log_len'))
            idx_len = int(cfg.get('DB', 'idx_len'))
            head_size = int(cfg.get('DB', 'head_size'))
            page_size = int(cfg.get('DB', 'page_size'))
            version = cfg.get('DB', 'version')
            # quit()
        return log_len, idx_len, head_size, page_size, version

    def get(self, key):
        index = self.session.get_index()
        if key in index:
            data_idx = index[key]
            data_dic = self.session.get_data(data_idx)
            if data_dic['exp'] is None or data_dic['exp'] >= (time.time() - data_dic['set_time']):
                return data_dic['data']
            # raise Exception('KeyError', 'Key is out of date.')
        raise Exception('KeyError', 'Key Not In Database Or Out Of Date.')
        # return None

    def set(self, key, value, exp=None): # 多进程不安全
        self.lock_db()
        index = self.session.get_index()
        if key in index:
            last_idx = index[key]
            time_stamp = time.time()
            index[key] = self.session.set_data({'data':value, 'last_idx':last_idx, 'exp':exp, 'set_time':time_stamp}, key)
        else:
            time_stamp = time.time()
            index[key] = self.session.set_data({'data':value, 'last_idx':None, 'exp':exp, 'set_time':time_stamp}, key)
        self.session.update_index(index)
        self.unlock_db()

    def get_history(self, key, deep=None, back_time=None):
        index = self.session.get_index()
        hist_dic = {}
        count = 0
        if key in index:
            capsule = self.session.get_data(index[key])
            while capsule['last_idx'] is not None:
                count += 1
                if deep and count > deep:
                    break
                if back_time and back_time < time.time() - capsule['set_time']:
                    break
                hist_dic[capsule['set_time']] = capsule['data']
                capsule = self.session.get_data(capsule['last_idx'])
        return hist_dic

    def check_log(self, desc=True, limit=None):
        logs = self.session.check_log()
        return logs

    def set_head_info(self, info):
        self.lock_db()
        head = self.session.get_head()
        head['info'] =info
        self.session.set_head(head)
        self.unlock_db()

    def get_head(self):
        return self.session.get_head()

    def del_key(self, key):
        self.lock_db()
        index = self.session.get_index()
        if key in index:
            last_idx = index[key]
            time_stamp = time.time()
            index[key] = self.session.set_data(
                {'data': None, 'last_idx': last_idx, 'exp': 0, 'set_time': time_stamp}, key)
            self.session.update_index(index)
        else:
            pass
        self.unlock_db()

    def get_all_dic(self):
        index = self.session.get_index()
        dic = {}
        for k in index:
            try:
                dic[k] = self.get(k)
            except:
                pass
        return dic

    def lock_db(self):
        fcntl.flock(self.db.fileno(), fcntl.LOCK_EX)

    def unlock_db(self):
        fcntl.flock(self.db.fileno(), fcntl.LOCK_UN)

if __name__ == '__main__':

    h = Handler('test5.db')
    # h.set('im', 'lyx')
    # print(h.get('im'))
    # print(h.set('u', 12222222222222222222222222, exp=10))
    # print(h.session.get_index())
    # print(h.get('u'))
    print(h.get_history('u', back_time=100000000, deep=3)) # ok