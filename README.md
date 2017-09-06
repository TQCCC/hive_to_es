通过Hive查询语句查出结果集，并向Elasticsearch导入的小工具

方便从hive导查询结果到ES的python脚本，采用分页查询机制，结果集合过多时不会撑爆内存


公司的数据分析经常需要看各种报表，多是分析统计类需求，类SQL语言适合做具有筛选逻辑的数据（有时候有的数据无法从业务主库中查出来，只能直接前端埋点），Elasticsearch适合做统计，而且结合Kibana可以直接生成报表！

耗时查询不宜直接在线上查，还好公司已经实现每天在访问低谷期同步线上数据到Hadoop大数据中心。
    
对这类常有的统计类需求，我的做法是先用HQL做筛选逻辑，ES拿到数据再进行聚合统计，如每天、每月、某人的数据。
结合ES其实更多是因为需求方喜欢Kibana的图表。


<br>
脚本使用说明 命令 #python hive_to_es.py <配置文件目录><br>
配置文件使用说明： 使用.ini后缀的配置文件<br>

[es]<br>
-- Elasticserch地址、用户名、密码<br>
hosts = 192.168.2.100:9200<br>
username = elastic<br>
password = 888888<br>

[hive]<br>
-- Hive地址、端口、数据库名、用户等配置<br>

host = 127.0.0.1<br>
port = 10000<br>
authMechanism = PLAIN<br>
user = sa_cluster<br>
database = julanling_g<br>

-- HiveQL文件位置<br>
hql_path = ./hql_test.sql<br>

-- 存入ES时的分页大小<br>
-- 为了防止结果集过大，导致查询时内存吃不消，建议配置此项，无分页配置时默认分页大小3000<br>
page_size = 2<br>

[es_bulk]<br>
-- 存入ES时，定义一个文档中的各个字段名称，注意与查询结果的各个字段按顺序对应，才能得到对应正确的数据值<br>
-- 如该例的HQL为select r_name, r_id from user_role<br>
-- ES文档：{"role_name": "xxx", "role_id":123}<br>
columns = role_name,role_id<br>

-- 存入ES的目标index和type<br>
index = tqc_test<br>
type = tqc_test_type<br>