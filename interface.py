from Odyssey.file_utils import _CONTENT_MULTIPAGE, _CONTENT_SET, _WRITE_HEAD, _READ_HEAD, _CONN_DATABASE
import struct
import pickle
import time

class Session:
    """
    Odyssey（提供redis-like api）
    |
    Handler（日志对其透明，但必要时可以访问；）
    |
    Session（封装底层文件处理函数；封装日志系统，日志自动记录；） Session
    |
    FILE BASEMENT

    Interface相当于打开一个session，是面向一个连接的接口，传入的是连接，可以调用底层文件系统进行接近底层的读写封装


    """
    def __init__(self, _DB, _HEAD_SIZE=None, _PAGE_SIZE=None, _IDX_LEN=None, _LOG_LEN=None):
        self.db = _DB
        self.head_size = _HEAD_SIZE
        self.page_size = _PAGE_SIZE
        self.idx_len = _IDX_LEN
        self.log_len = _LOG_LEN


    def load_config_from_head(self):
        """
        如果没有在初始化阶段对齐，则需要从数据库中读取对齐配置并对齐
        :return:
        """
        head_content = self.get_head()
        self.head_size = head_content['HEAD_SIZE']
        self.page_size = head_content['PAGE_SIZE']
        self.idx_len = head_content['IDX_LEN']
        self.log_len = head_content['LOG_LEN']
        return

    def __log_hook(self, hook_type=0, user=None, time=None, info=None): # 0:data 1:index 2:head(at build)
        self.append_log(log_content={'type':hook_type, 'user':user, 'time':time.time() if time is None else time, 'info':info})
        pass

    def get_head(self):
        head_content = _READ_HEAD(self.db, self.head_size)
        length = struct.unpack('<i', head_content[:4])[0]
        dic_content = head_content[4:4+length]
        return pickle.loads(dic_content)

    def compile_head(self, idx_len, log_len, head_size, page_size, version, info=None):
        self.set_head({'version':version,
                       'info':info,
                       'log_idx':0,
                       'idx_len':0,
                       'data_idx':0,
                       'LOG_LEN':log_len,
                       'IDX_LEN':idx_len,
                       'HEAD_SIZE':head_size,
                       'PAGE_SIZE':page_size})
        self.__log_hook(hook_type=2, time=time.time())
        return

    def set_head(self, dic):
        dic_bytes = pickle.dumps(dic)
        len_dic_bytes = len(dic_bytes)
        # print(len_dic_bytes)
        length = struct.pack('<i', len_dic_bytes)
        content = length + dic_bytes
        _WRITE_HEAD(_DB=self.db, _HEAD_CONTENT=content, _HEAD_SIZE=self.head_size)
        return

    def append_log(self, log_content):
        log_content = pickle.dumps(log_content)
        log_content_length = len(log_content)
        log_bytes = struct.pack('<i', log_content_length)+log_content
        head_info = self.get_head()
        log_idx = head_info['log_idx']
        _CONTENT_SET(_CONTENT=log_bytes,
                     _DB=self.db,
                     _PAGE_SIZE=self.page_size,
                     _TYPE=0x01,
                     _INDEX_LEN=self.idx_len,
                     _LOG_LEN=self.log_len,
                     _HEAD_SIZE=self.head_size,
                     _LOG_OFFSET=log_idx,)
        head_info['log_idx'] += len(log_bytes)
        self.set_head(head_info)
        return 0

    def check_log(self):
        log_lst = []
        head_info = self.get_head()
        log_max = head_info['log_idx']

        all_log_content = _CONTENT_MULTIPAGE(_DB=self.db,
                                     _CONTENT_LEN=log_max,
                                     _HEAD_SIZE=self.head_size,
                                     _PAGE_SIZE=self.page_size,
                                     _RET=0x01,
                                     _INDEX_LEN=self.idx_len,
                                     _LOG_LEN=self.log_len,
                                     _START_OFFSET=0)
        offset = 0
        while offset<len(all_log_content):
            log_len = struct.unpack('<i', all_log_content[offset:offset+4])[0]
            # print('log_len', log_len)
            log_content = pickle.loads(all_log_content[offset+4:offset+4+log_len])

            # print('log_len', log_content)
            # log_content = pickle.loads(log_content)
            log_lst.append(log_content)
            offset += 4+log_len
        return log_lst


    def update_index(self, idx_dic):
        idx_bytes = pickle.dumps(idx_dic)
        _CONTENT_SET(_CONTENT=idx_bytes,
                     _DB=self.db,
                     _PAGE_SIZE=self.page_size,
                     _TYPE=0x00,
                     _INDEX_LEN=self.idx_len,
                     _LOG_LEN=self.log_len,
                     _HEAD_SIZE=self.head_size,)
        head_info = self.get_head()
        head_info['idx_len'] = len(idx_bytes)
        self.set_head(head_info)
        self.__log_hook(hook_type=1, time=time.time())
        pass

    def get_index(self):
        head_info = self.get_head()
        idx_len = head_info['idx_len']
        if idx_len > 0:
            index_content = _CONTENT_MULTIPAGE(_DB=self.db,
                               _CONTENT_LEN=idx_len,
                               _HEAD_SIZE=self.head_size,
                               _PAGE_SIZE=self.page_size,
                               _RET=0x00,
                               _INDEX_LEN=self.idx_len,
                               _LOG_LEN=self.log_len,
                               _START_OFFSET=0)
            return pickle.loads(index_content)
        else:
            self.update_index({})
            return {}


    def get_data(self, offset):
        data_length = _CONTENT_MULTIPAGE(_DB=self.db,
                                           _CONTENT_LEN=4,
                                           _HEAD_SIZE=self.head_size,
                                           _PAGE_SIZE=self.page_size,
                                           _RET=0x02,
                                           _INDEX_LEN=self.idx_len,
                                           _LOG_LEN=self.log_len,
                                           _START_OFFSET=offset)
        data_length = pickle.unpack('<i', data_length)[0]
        # print(data_length)
        data_content = _CONTENT_MULTIPAGE(_DB=self.db,
                           _CONTENT_LEN=data_length,
                           _HEAD_SIZE=self.head_size,
                           _PAGE_SIZE=self.page_size,
                           _RET=0x02,
                           _INDEX_LEN=self.idx_len,
                           _LOG_LEN=self.log_len,
                           _START_OFFSET=offset+4)
        # print(len(data_content))
        return pickle.loads(data_content)

    def set_data(self, data, key=None):
        data_bytes = pickle.dumps(data)
        data_bytes_length = struct.pack('<i', len(data_bytes))
        head_info = self.get_head()
        data_idx = head_info['data_idx']
        data_content= data_bytes_length+data_bytes

        _CONTENT_SET(_CONTENT=data_content,
                     _DB=self.db,
                     _PAGE_SIZE=self.page_size,
                     _TYPE=0x02,
                     _INDEX_LEN=self.idx_len,
                     _LOG_LEN=self.log_len,
                     _HEAD_SIZE=self.head_size,
                     _DATA_OFFSET=data_idx)
        head_info['data_idx'] += len(data_content)
        self.set_head(head_info)
        self.__log_hook(hook_type=0, time=time.time(), info={key:hash(data_bytes)} if key is not None else None)
        return data_idx

    def get_current_position(self):
        return self.db.tell()

    @staticmethod
    def connect(db_name, head_size):
        db, status = _CONN_DATABASE(_NAME=db_name, _HEAD_SIZE=head_size)
        return db, status

    def close(self):
        self.db.close()


if __name__ == '__main__':

    # print(struct.unpack('<i', struct.pack('<i', 1))[0])
    db, status = Session.connect('test5.db', head_size=1000, )
    interface = Session(db, 1000, 50, 10, 10)
    # interface.set_head(dic={'version':'0.0.1beta', 'info':'qe', 'log_idx':0, 'idx_len':0, 'data_idx':0, 'LOG_LEN':10,'IDX_LEN':10,'HEAD_SIZE':1000, 'PAGE_SIZE':50})
    # print(interface.get_head()) # write-read test ok
    # interface.update_index({'a':'ggggggggg'}) # index 读写测试成功
    print(interface.get_index())
    # print(interface.get_head())
    # print(interface.get_head())
    # interface.set_data({'data1':23908450})
    # print(interface.set_data('hhhhhhhh23'))
    # print(interface.set_data('hh2gbvfopopopopopopophh23'))
    # print(interface.get_data(offset=0)) # data 读写调试成功
    # print(interface.get_data(offset=204))
    # print(interface.get_head())

