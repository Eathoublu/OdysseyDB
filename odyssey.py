import os
from Odyssey.handler import Handler
# from Odyssey.interface import Session

def connect(dbname, use_config=None):
    db = Odyssey(dbname, use_config)
    return db

class Odyssey(object):
    def __init__(self, db_name, use_config=None):
        self.db_path = db_name
        self.handler = Handler(db_name, load_config=use_config)

    def set(self, key, value, exp=None):
        self.handler.set(key, value, exp)

    def get(self, key):
        return self.handler.get(key)

    def get_history(self, key, deep=None, limit_time=None):
        return self.handler.get_history(key, deep=deep, back_time=limit_time)

    def get_log(self, desc=True, limit=None):
        return self.handler.check_log(desc=desc, limit=limit)

    def __setitem__(self, key, value):
        self.handler.set(key, value)

    def __getitem__(self, key):
        return self.handler.get(key)

    def __call__(self, info):
        self.handler.set_head_info(info)
        pass

    def __delitem__(self, key):
        self.del_key(key)

    def summery(self): # 换成.summery,打印就打印一个dict
        head_info = self.handler.get_head()
        disk_use = os.path.getsize(self.db_path)//1024
        print("""
    -*- Odyssey Database Version {} -*-
     |                               |
     |   ```                         |
     |       INFO:{}               |
     |                               |
     |       LOG_LEN:{}              |
     |       IDX_LEN:{}              |   
     |       HEAD_SIZE:{}          |
     |       PAGE_SIZE:{}            |
     |                               |
     |       Disk Occupy:{} KB        |   
     |   ```                         |
     ---------------------------------
        """.format(head_info['version'], head_info['info'], head_info['LOG_LEN'], head_info['IDX_LEN'], head_info['HEAD_SIZE'], head_info['PAGE_SIZE'], disk_use))
        return {'version':head_info['version'],
                'info':head_info['info'],
                'log_len':head_info['LOG_LEN'],
                'idx_len':head_info['IDX_LEN'],
                'head_size':head_info['HEAD_SIZE'],
                'page_size':head_info['PAGE_SIZE'],
                'disk_use': disk_use
                }

    def get_info(self):
        return self.handler.get_head()['info']

    def del_key(self, key):
        self.handler.del_key(key)

    def get_all(self):
        return self.handler.get_all_dic()

    def __str__(self):
        return '{}'.format(self.get_all())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.handler.session.close()

    def __iter__(self):
        keys = self.handler.get_all_dic().keys()
        yield from keys

    def __len__(self):
        return len(self.handler.get_all_dic())

    def __sizeof__(self):
        return os.path.getsize(self.db_path)

    def __eq__(self, other):
        if self.handler.get_all_dic() == other.get_all():
            return True
        return False

    def __contains__(self, item):
        if item in self.handler.get_all_dic():
            return True

    def keys(self):
        return self.handler.get_all_dic().keys()

    def values(self):
        return self.handler.get_all_dic().values()

    def pop(self, key):
        last_val = self.get(key)
        self.del_key(key)
        return last_val

    def __bool__(self):
        return not self.handler.db.closed

    def close(self):
        self.handler.session.close()










