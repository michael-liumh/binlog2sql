使用说明请参考：https://github.com/danfengcao/binlog2sql

安装
==============
```
git clone https://github.com/Michaelsky0913/binlog2sql.git && cd binlog2sql && \
pip install -r requirements.txt
```
git与pip的安装问题请自行搜索解决。

修改：
    1. 添加日志异常日志输出（主要输出blob数据类型的异常：哪些表存在blob数据类型，无法进行解码，对应的十六进制数据是什么）
    2. 修改requirement.txt文件
    3. 添加一些注释
