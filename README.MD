# OdysseyDB

OdysseyDB:A lightweight migratable file based key-value database with redis-like api and more features, in memory of 2001:SpaceOdyssey by Stanley Kubrick.
奥德赛：一个轻量级的可移植的提供redis-like api和更多新奇特性的对象存储键值对数据库，命名纪念斯坦利·库布里克的《2001：太空奥德赛》。

## 为什么选择OdysseyDB
1. 可迁移：类似sqlite3，即开即用，每一个数据库都是一个文件，简单的复制粘贴即可完成数据库的跨平台迁移，但有别于sqlite的关系型数据库，OdysseyDB为键值对数据库。可以轻易在不同环境下部署，在不同环境下接续运行同一程序时可动态载入持久化的运行时变量，支持热插拔，记录变量历史值和日志。不需要开启端口，但可以同时服务多个进程，并满足数据库的ACID特性。

2. 便捷：极易上手。实例化后的Odyssey对象提供了几乎所有字典的特性，可以作为一个字典使用，区别是该字典会实时持久化在文件中，记录运行状态和历史更新记录。与此同时，Odyssey还提供类似redis的api，方便开发。

3. 灵活：有别于redis只能记录list、str和int等有限数据结构，OdysseyDB底层将对象字节流序列化且对用户透明，几乎可以记录任何的python数据结构（包括自定义的那些），为开发者提供极大便利。

4. 详细：可以记录每一个变量的历史修改日期、历史值，提供日志。

## 使用方法
### 实例化一个Odyssey数据库
```python
import Odyssey 
db = Odyssey.connect('test.db') # 如果没有该数据库，则将创建一个空数据库
```
或者：
```python
from Odyssey import Odyssey
db = Odyssey('test.db')
```
或者：
```python
from Odyssey import Odyssey
with Odyssey('test.db') as db:
	pass
```
以上三种写法等价，都会得到一个db对象用于键值对的存取管理。

与此同时，当新建立一个数据库时，还可以传入一个config文件作为数据库的配置；如果该数据库已经存在，则Odyssey会弹出一条警告并忽略该传入的配置文件，数据库的原有配置不会变更。
```python
from Odyssey import Odyssey
db = Odyssey('test.db', use_config='custom.conf')
```
custum.conf文件有以下内容：
```config
[DB]
log_len = 10
idx_len = 10
head_size = 1000
page_size = 50
version = '0.0.1'
```
可以自己定制其中的参数。


### 对键值对的存取
可以使用简单的.set .get方法：
```python
db.set('hello', 'Odyssey')
db.set('hi', 'Odyssey', exp=100) # hi键100秒后过期
print(db.get('hello'))
print(db.get('hi'))
```
键可以是任何hashable type。

也可以使用按键取值的方法：
```python
db['hello'] = 'Odyssey'
print(db['hello'])
```

### 键的删除
以下三种方法等价：
```python
del db['hello']
db.del_key('hello')
db.pop('hello') # 会返回被删除的键的最后一个值
```
键删除后只是无法正常按键取值，而其历史值仍保留在数据库中，需要时用下面的方法可以查询到。
尝试取一个不存在的键、被删除的键或者过期的键都会导致报错。

### 键的历史值查看
使用get_history方法可以查看一个制定键的历史值，可以限制查看的个数和时间范围。
```python
db['hello'] = 'Odyssey'
db['hello'] = 'is'
db['hello'] = 'good'
print(db.get_history('hello', limit_time=100, deep=3)) # 限制只搜寻最近100秒内的前三个
```

### 查看数据库中所有键值
1. get_all
```python
print(db.get_all())
```

2. 循环遍历查看
```python
for k in db:
	print('key:{} value:{}'.format(k, db[k]))
```

3. 直接打印出来
```python
print(db) # 对，就是这么简单
```
以上三种方法完全等价。

### 查看一个键是否在数据库中
```python
if 'hello' in db:
	print("It's here!")
```

### 查看数据库是否已经关闭
```python
if bool(db):
	print('db is opened.')
else:
	print('db is closed.')
```

### 给数据库头部写一段info并查看
```python
db('OdysseyDB is good.') # 该方法允许开发者可以记录一些额外的内容在数据库的info字段中，如记录备忘录、TODO等等。写入时会擦除之前的info内容。

print(db.get_info())
```

### 查看数据库日志
```python
print(db.get_log())
```

### 查看数据库统计信息
```python
db.summery() # 自带print
```

### 查看数据库所有键
```python
print(db.keys())
```

### 查看数据库所有值
```python
print(db.values())
```

### 查看有效键的个数
```python
print(len(db))
```

### 查看两个数据库的键值是否完全相同（不检查历史值）
```python
print(db1 == db2)
```

### 关闭数据库
```python
db.close()
```

## 设计模式
OdysseyDB是一个基于文件的数据库，完全使用python提供的标准库实现，不使用第三方库；每一个数据库的即为一个独立的文件。文件以头部+数据页的形式存储数据，其结构如下。
```
|--------------------|
|		 HEAD        | 
|--------------------|
|		 PAGE 1      |
|					 |
|	Index|Log|Data   |
|					 |
|					 |
|--------------------|
|		 PAGE 2      |
|					 |
|	Index|Log|Data   |
|					 |
|					 |
|--------------------|
|		 PAGE 3      |
|					 |
|	Index|Log|Data   |
|					 |
|					 |
|--------------------|
|	    ......       |

```
头部储存了数据库的一些基本信息，包括版本号、Index区域的页内大小、Log区域的页内大小、HEAD大小、PAGE大小、当前log指针、当前data指针以及Index的长度，同时允许存储一段附加信息；头部的前四个字节代表头部的大小，程序可以根据头部的大小读取头部内容，进而加载该数据库的配置。OdysseyDB允许使用不同配置初始化数据库。
数据存储在数据页中，存储信息通过Index寻址，通过页面和页内偏移找到对应的数据块，并读取该位置前四个字节中的数据长度信息，并以此长度读入数据。
整个数据库操作机制分为四级，分别是底层文件utils、Session、Handler、Odyssey。上层所有的操作最终都是由底层的文件操作utils来完成对数据库的更改的；Session一定程度上封装了各种面向文件的数据库文件操作，数据库文件的分页对其透明；Handler则封装了面向对象的存储服务；最终由Odyssey提供用户API。




## 未来特性
1. 支持多用户隔离访问
2. AES数据安全加密
3. 清除太过久远的历史值、日志信息。