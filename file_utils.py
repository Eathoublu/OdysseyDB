import fcntl
import os

def _APPEND_PAGE(_DB, _PAGE_SIZE):
    _DB.write(b'0'*_PAGE_SIZE)
    _DB.flush()
    return


def _DELETE_PAGE(_DB, _PAGE_SIZE):
    _DB.seek(0, 2) # 移到文件末尾
    _LENGTH = _DB.tell()
    _DB.truncate(_LENGTH-_PAGE_SIZE) # 往前截断一个page的大小,如果文件原本大小不足page_size的话则会报错
    _DB.flush()
    return


def _PARSE_SINGLE_PAGE(_DB, _OFFSET, _PAGE_SIZE, _RET, _INDEX_LEN, _LOG_LEN): # 0:_INDEX 1:_LOG 2:_DATA
    """
    用于解析当前页面的内容，页面的框选通过跨页面偏移offset实现
    :param _DB:
    :param _OFFSET:
    :param _PAGE_SIZE:
    :param _RET:
    :param _INDEX_LEN:
    :param _LOG_LEN:
    :return:
    """
    if _RET == 0x00:
        _DB.seek(_OFFSET)
        _INDEX_SLICE = _DB.read(_INDEX_LEN)
        return _INDEX_SLICE
    elif _RET == 0x01:
        # print('seek here', _OFFSET+_INDEX_LEN)
        _DB.seek(_OFFSET+_INDEX_LEN)
        _LOG_SLICE = _DB.read(_LOG_LEN)
        return _LOG_SLICE
    elif _RET == 0x02:
        # print(_OFFSET+_INDEX_LEN+_LOG_LEN)
        _DB.seek(_OFFSET+_INDEX_LEN+_LOG_LEN)
        _CONTENT = _DB.read(_PAGE_SIZE-_INDEX_LEN-_LOG_LEN)
        return _CONTENT

def _CONTENT_MULTIPAGE(_DB, _CONTENT_LEN, _HEAD_SIZE=1024, _PAGE_SIZE=1024, _RET=0, _INDEX_LEN=0x100, _LOG_LEN=0x100, _START_OFFSET=None):
    """
    调用parse single page 实现对于某一内容在所有页面上的读取，需告知内容的总长度
    :param _DB:
    :param _CONTENT_LEN:
    :param _HEAD_SIZE:
    :param _PAGE_SIZE:
    :param _RET:
    :param _INDEX_LEN:
    :param _LOG_LEN:
    :return:
    """
    _CONTNET = b''
    _OVER = False
    _START_PAGE = 0
    if _RET == 0x00:
        _PAGE_NUMS = _CONTENT_LEN // _INDEX_LEN
        if _CONTENT_LEN % _INDEX_LEN != 0:
            _OVER = True

    elif _RET == 0x01:
        _PAGE_NUMS = _CONTENT_LEN // _LOG_LEN
        if _CONTENT_LEN % _LOG_LEN != 0:
            _OVER = True


    elif _RET == 0x02:
        _PAGE_NUMS = _CONTENT_LEN // (_PAGE_SIZE-_INDEX_LEN-_LOG_LEN)
        if _CONTENT_LEN % (_PAGE_SIZE-_INDEX_LEN-_LOG_LEN) != 0:
            _OVER = True
        if _START_OFFSET is None:
            raise Exception('StartOffestError', '_START_OFFSET is needed when requesting data field.')
        if _START_OFFSET > 0:
            _START_PAGE = _START_OFFSET // (_PAGE_SIZE-_INDEX_LEN-_LOG_LEN)
            _PAGE_INNER_OFFSET = _START_OFFSET % (_PAGE_SIZE-_INDEX_LEN-_LOG_LEN)
            _DB.seek(_HEAD_SIZE+_START_PAGE*_PAGE_SIZE+_LOG_LEN+_INDEX_LEN+_PAGE_INNER_OFFSET)
            _READ_LEN = min(_CONTENT_LEN, (_PAGE_SIZE-_LOG_LEN-_INDEX_LEN-_PAGE_INNER_OFFSET))
            _CONTNET += _DB.read(_READ_LEN)
            _START_PAGE += 1
            # print('prefix', _CONTNET)
            # quit()

    # print('pn', _PAGE_NUMS)
    # print('sp', _START_PAGE)

    if _OVER:
        _PAGE_NUMS += 1
    for _PAGE_NUM in range(_START_PAGE, _START_PAGE+_PAGE_NUMS, 0x01):
        _CURR_OFFSET = _HEAD_SIZE + _PAGE_NUM * _PAGE_SIZE
        # print(_CURR_OFFSET)
        _CONTENT_BUFFER = _PARSE_SINGLE_PAGE(_DB, _OFFSET=_CURR_OFFSET, _PAGE_SIZE=_PAGE_SIZE, _RET=_RET, _INDEX_LEN=_INDEX_LEN, _LOG_LEN=_LOG_LEN)
        # print(_CONTENT_BUFFER)
        _CONTNET += _CONTENT_BUFFER
    return _CONTNET[:_CONTENT_LEN]



def _DB_WRITE_BYTES(_DB, _OFFSET, _CONTENT, _PAGE_LIMIT):
    """
    直接向文件写入一串字节，通过page_limit进行页面溢出检查
    :param _DB:
    :param _OFFSET:
    :param _CONTENT:
    :param _PAGE_LIMIT:
    :return:
    """
    # print('file_offset', _OFFSET)
    if _OFFSET+len(_CONTENT) > _PAGE_LIMIT:
        raise Exception('PageOverflow')
    _DB.seek(_OFFSET)
    # fcntl.flock(_DB.fileno(), fcntl.LOCK_EX)
    _DB.write(_CONTENT)
    # fcntl.flock(_DB.fileno(), fcntl.LOCK_EX)
    _DB.flush()
    return _DB.tell()

def _GET_TOTAL_SIZE(_DB):
    _DB.seek(0, 2)
    return _DB.tell()

def _WRITE_IN_PAGE(_DB, _CONTENT_BUFFER, _PAGE_NUM,_PAGE_OFFSET, _PAGE_SIZE, _HEAD_SIZE,_INDEX_LEN, _LOG_LEN, _TYPE):
    """
    在指定页面写内容，做安全检查，最终通过db_write_content实现，在本函数中需要计算当前页面末尾位置以及contnet buffer的大小从而确认安全
    传入当前页面号，页面偏移便可知道
    该函数计算页面偏移和页内偏移并相加作为总偏移量
    :param _CONTNET_BUFFER:
    :param _PAGE_NUM:
    :param __PAGE_SIZE:
    :param _HEAD_SIZE:
    :param _INDEX_LEN:
    :param _LOG_LEN:
    :return:
    """

    if _TYPE == 0x00:
        if not _PAGE_OFFSET == 0x00:
            raise Exception('PageInnerOffsetError')
        if len(_CONTENT_BUFFER) > _INDEX_LEN:
            raise Exception('INDEXContentOverflow')
        _PAGE_NUM_OFFSET = _PAGE_NUM * _PAGE_SIZE + _HEAD_SIZE
        _CURR_SIZE = _GET_TOTAL_SIZE(_DB)
        _CURR_TOTAL_PAGE = (_CURR_SIZE - _HEAD_SIZE) // _PAGE_SIZE
        if _CURR_TOTAL_PAGE <= _PAGE_NUM:
            _APPEND_PAGE(_DB, _PAGE_SIZE=_PAGE_SIZE)
        return _DB_WRITE_BYTES(_DB, _PAGE_NUM_OFFSET, _CONTENT=_CONTENT_BUFFER, _PAGE_LIMIT=_PAGE_NUM_OFFSET+_PAGE_SIZE)

    if _TYPE == 0x01:
        if not _PAGE_OFFSET == _INDEX_LEN:
            raise Exception('PageInnerOffsetError')
        if len(_CONTENT_BUFFER) > _LOG_LEN:
            raise Exception('LOGContentOverflow')
        _CURR_SIZE = _GET_TOTAL_SIZE(_DB)
        _CURR_TOTAL_PAGE = (_CURR_SIZE - _HEAD_SIZE) // _PAGE_SIZE
        if _CURR_TOTAL_PAGE <= _PAGE_NUM:
            _APPEND_PAGE(_DB, _PAGE_SIZE=_PAGE_SIZE)
        _PAGE_NUM_OFFSET = _PAGE_NUM * _PAGE_SIZE + _HEAD_SIZE + _INDEX_LEN
        return _DB_WRITE_BYTES(_DB, _PAGE_NUM_OFFSET, _CONTENT=_CONTENT_BUFFER, _PAGE_LIMIT=_PAGE_NUM_OFFSET+_LOG_LEN)

    if _TYPE == 0x02:
        if not _PAGE_OFFSET == _INDEX_LEN + _LOG_LEN:
            raise Exception('PageInnerOffsetError')
        if len(_CONTENT_BUFFER) > _PAGE_SIZE - _LOG_LEN - _INDEX_LEN:
            raise Exception('DataContentOverflow')
        _CURR_SIZE = _GET_TOTAL_SIZE(_DB)
        _CURR_TOTAL_PAGE = (_CURR_SIZE - _HEAD_SIZE) // _PAGE_SIZE
        if _CURR_TOTAL_PAGE <= _PAGE_NUM:
            _APPEND_PAGE(_DB, _PAGE_SIZE=_PAGE_SIZE)
        _PAGE_NUM_OFFSET = _PAGE_NUM * _PAGE_SIZE + _HEAD_SIZE + _INDEX_LEN + _LOG_LEN
        # print('page_num_offset', _PAGE_NUM_OFFSET, _PAGE_NUM_OFFSET+len(_CONTENT_BUFFER))
        return _DB_WRITE_BYTES(_DB, _PAGE_NUM_OFFSET, _CONTENT=_CONTENT_BUFFER, _PAGE_LIMIT=_PAGE_NUM_OFFSET+(_PAGE_SIZE-_LOG_LEN-_INDEX_LEN))


def _LOCK_FILE(_DB):
    fcntl.flock(_DB.fileno(), fcntl.LOCK_EX)
    return


def _UNLOCK_FILE(_DB):
    fcntl.flock(_DB.fileno(), fcntl.LOCK_UN)
    return


def _CONTENT_SET(_CONTENT, _DB, _PAGE_SIZE=1024, _TYPE=0x00, _INDEX_LEN=0x100, _LOG_LEN=0x100, _HEAD_SIZE=1024, _LOG_OFFSET=None, _DATA_OFFSET=None):
    """
    将传入的字节流持久化到数据库中。可以持久化INDEX、LOG、DATA，对于较长字节流，支持自动判断、换页存储，循环写；并在循环外加互斥锁（调用write_in_page进而调用DBwritebytes实现写功能）

    LOG_OFFSET:原有的log长度
    :param _CONTENT:
    :param _DB:
    :param _PAGE_SIZE:
    :param _TYPE:
    :param _INDEX_LEN:
    :param _LOG_LEN:
    :return:
    """
    assert type(_CONTENT) == bytes
    _CONTENT_LEN = len(_CONTENT)
    # print(_CONTENT_LEN)
    if _TYPE == 0x00:
        _PAGE_NUM = 0x00
        # fcntl.flock(_DB.fileno(), fcntl.LOCK_EX)
        for _CONTENT_OFFSET in range(0, _CONTENT_LEN, _INDEX_LEN):
            _CONTENT_BUFFER = _CONTENT[_CONTENT_OFFSET:_CONTENT_OFFSET+_INDEX_LEN]
            # print(_PAGE_NUM, _CONTENT_BUFFER)
            _WRITE_IN_PAGE(_DB=_DB,
                           _CONTENT_BUFFER=_CONTENT_BUFFER,
                           _PAGE_NUM=_PAGE_NUM,
                           _PAGE_OFFSET=0x00,
                           _PAGE_SIZE=_PAGE_SIZE,
                           _INDEX_LEN=_INDEX_LEN,
                           _LOG_LEN=_LOG_LEN,
                           _HEAD_SIZE=_HEAD_SIZE,
                           _TYPE=_TYPE
                           )
            _PAGE_NUM += 0x01
        fcntl.flock(_DB.fileno(), fcntl.LOCK_UN)

    if _TYPE == 0x01:
        if _LOG_OFFSET is None:
            raise Exception('LOG_OFFSET is needed.')
        # 计算从第几页开始
        _START_PAGE = _LOG_OFFSET // _LOG_LEN
        _OVER_OFFSET = _LOG_OFFSET % _LOG_LEN
        _PAGE_NUM = _START_PAGE
        # fcntl.flock(_DB.fileno(), fcntl.LOCK_EX)
        _CURR_INDEX = None

        # print(_PAGE_NUM, _OVER_OFFSET, _START_PAGE)

        if _OVER_OFFSET > 0x00:
            _DB_WRITE_BYTES(_DB,
                            _OFFSET=_PAGE_NUM*_PAGE_SIZE+_OVER_OFFSET+_INDEX_LEN+_HEAD_SIZE,
                            _CONTENT=_CONTENT[:_LOG_LEN-_OVER_OFFSET],
                            _PAGE_LIMIT=_PAGE_NUM*_PAGE_SIZE+_LOG_LEN+_INDEX_LEN+_HEAD_SIZE)
            _PAGE_NUM += 0x01
            for _LOG_CONTENT_IDX in range(_LOG_LEN-_OVER_OFFSET, len(_CONTENT), _LOG_LEN):
                _CURR_INDEX = _WRITE_IN_PAGE(_DB,
                               _CONTENT_BUFFER=_CONTENT[_LOG_CONTENT_IDX:_LOG_CONTENT_IDX+_LOG_LEN],
                               _PAGE_NUM=_PAGE_NUM,
                               _PAGE_OFFSET=_INDEX_LEN,
                               _PAGE_SIZE=_PAGE_SIZE,
                               _HEAD_SIZE=_HEAD_SIZE,
                               _INDEX_LEN=_INDEX_LEN,
                               _LOG_LEN=_LOG_LEN,
                               _TYPE=0x01)
                _PAGE_NUM += 0x01
        else:
            for _LOG_CONTENT_IDX in range(0, len(_CONTENT), _LOG_LEN):
                _CURR_INDEX = _WRITE_IN_PAGE(_DB,
                               _CONTENT_BUFFER=_CONTENT[_LOG_CONTENT_IDX:_LOG_CONTENT_IDX+_LOG_LEN],
                               _PAGE_NUM=_PAGE_NUM,
                               _PAGE_OFFSET=_INDEX_LEN,
                               _PAGE_SIZE=_PAGE_SIZE,
                               _HEAD_SIZE=_HEAD_SIZE,
                               _INDEX_LEN=_INDEX_LEN,
                               _LOG_LEN=_LOG_LEN,
                               _TYPE=0x01)
                _PAGE_NUM += 0x01
        fcntl.flock(_DB.fileno(), fcntl.LOCK_UN)
        return _LOG_OFFSET+len(_CONTENT), _CURR_INDEX  # 当前log总产度，当前指针位置

    if _TYPE == 0x02:
        if _DATA_OFFSET is None:
            raise Exception('DATA_OFFSET is needed.')
        # 计算从第几页开始
        _START_PAGE = _DATA_OFFSET // (_PAGE_SIZE-_LOG_LEN-_INDEX_LEN)
        _OVER_OFFSET = _DATA_OFFSET % (_PAGE_SIZE-_LOG_LEN-_INDEX_LEN)
        _PAGE_NUM = _START_PAGE
        # fcntl.flock(_DB.fileno(), fcntl.LOCK_EX)
        _CURR_INDEX = None
        # print(_PAGE_NUM, _OVER_OFFSET, _START_PAGE)
        # print(_PAGE_NUM, _OVER_OFFSET, _START_PAGE)

        if _OVER_OFFSET > 0x00:
            _DB_WRITE_BYTES(_DB,
                            _OFFSET=_PAGE_NUM * _PAGE_SIZE + _OVER_OFFSET + _INDEX_LEN + _HEAD_SIZE + _LOG_LEN,
                            _CONTENT=_CONTENT[:(_PAGE_SIZE - _LOG_LEN - _INDEX_LEN) - _OVER_OFFSET],
                            _PAGE_LIMIT=(_PAGE_NUM+1) * _PAGE_SIZE + _HEAD_SIZE)
            _PAGE_NUM += 0x01
            for _DATA_CONTENT_IDX in range((_PAGE_SIZE - _LOG_LEN - _INDEX_LEN) - _OVER_OFFSET, len(_CONTENT), _PAGE_SIZE - _LOG_LEN - _INDEX_LEN):
                _CURR_INDEX = _WRITE_IN_PAGE(_DB,
                                             _CONTENT_BUFFER=_CONTENT[_DATA_CONTENT_IDX:_DATA_CONTENT_IDX + (_PAGE_SIZE - _LOG_LEN - _INDEX_LEN)],
                                             _PAGE_NUM=_PAGE_NUM,
                                             _PAGE_OFFSET=_INDEX_LEN+_LOG_LEN,
                                             _PAGE_SIZE=_PAGE_SIZE,
                                             _HEAD_SIZE=_HEAD_SIZE,
                                             _INDEX_LEN=_INDEX_LEN,
                                             _LOG_LEN=_LOG_LEN,
                                             _TYPE=0x02)
                _PAGE_NUM += 0x01
        else:
            for _DATA_CONTENT_IDX in range(0, len(_CONTENT), _PAGE_SIZE - _LOG_LEN - _INDEX_LEN):
                _CURR_INDEX = _WRITE_IN_PAGE(_DB,
                                             _CONTENT_BUFFER=_CONTENT[_DATA_CONTENT_IDX:_DATA_CONTENT_IDX + (_PAGE_SIZE - _LOG_LEN - _INDEX_LEN)],
                                             _PAGE_NUM=_PAGE_NUM,
                                             _PAGE_OFFSET=_INDEX_LEN+_LOG_LEN,
                                             _PAGE_SIZE=_PAGE_SIZE,
                                             _HEAD_SIZE=_HEAD_SIZE,
                                             _INDEX_LEN=_INDEX_LEN,
                                             _LOG_LEN=_LOG_LEN,
                                             _TYPE=0x02)
                _PAGE_NUM += 0x01
        # fcntl.flock(_DB.fileno(), fcntl.LOCK_UN)
        return _DATA_OFFSET + len(_CONTENT), _CURR_INDEX  # 当前log总产度，当前指针位置






    """
    TODO:
    计算页面号
    将传入的字节流切片并一块一块通过调用write_in_page存放入对应的页面号中,循环之前加锁，循环后释放
    """

def _WRITE_EMPTY_HEAD(_DB, _HEAD_SIZE):
    _DB.write(b'0'*_HEAD_SIZE)
    _DB.flush()
    return 0x00

def _CONN_DATABASE(_NAME, _HEAD_SIZE=None):
    if os.path.exists(_NAME):
        _DB = open(_NAME, 'r+b')
        return _DB, True

    else:
        if _HEAD_SIZE is None:
            raise Exception('DBInitError', 'DBHeadSizeNotAssign')
        _DB = open(_NAME, 'wb')
        fcntl.flock(_DB.fileno(), fcntl.LOCK_EX)
        _WRITE_EMPTY_HEAD(_DB, _HEAD_SIZE)
        fcntl.flock(_DB.fileno(), fcntl.LOCK_UN)
        _DB.close()
        _DB = open(_NAME, 'r+b')
        return _DB, False

def _CLOSE_DB(_DB):
    _DB.flush()
    _DB.close()
    return None

def _READ_HEAD(_DB, _HEAD_SIZE):
    _DB.seek(0)
    _HEAD_CONTENT = _DB.read(_HEAD_SIZE)
    return _HEAD_CONTENT

def _WRITE_HEAD(_DB, _HEAD_CONTENT, _HEAD_SIZE):
    _DB.seek(0)
    _LOCK_FILE(_DB)
    if len(_HEAD_CONTENT) >= _HEAD_SIZE:
        raise Exception('HeadContentOVerflow')
    _DB.write(b'0'*_HEAD_SIZE)
    _DB.seek(0)
    _DB.write(_HEAD_CONTENT)
    _DB.flush()
    _UNLOCK_FILE(_DB)
    pass


# HEAD STRUCTURE: 4 _HEAD-SIZE:DICT




if __name__ == '__main__':

   pass
