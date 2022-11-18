# SparkSQLOnHBase

&nbsp; &nbsp; 该项目基于`Spark SQL`开发,实现了在`HBase`上进行sql查询的功能,支持标准sql查询操作.
物理逻辑层通过Spark SQL本身的谓词下推,剪枝等对HBase Scan的范围进行了优化,
后续有空闲时间会增加支持插入、删除、建表相关功能的实现.

 [最新代码中已经实现了通过sql语句插入数据的功能,`rowkey`生成策略可以通过配置文件手动指定]。

&nbsp; &nbsp; 注：项目的spark hbase由2022.11.15于github上的master分支手动编译得到,
理论上来讲能兼容Spark3.x版本

&nbsp; &nbsp; &nbsp; &nbsp; 1. HBase的`Namespace`与SQL中的`Database`对应,
在未指定`Database`的情况下, 可以使用`namespace.table`进行表查询, 
例如`select * from namespace.table`

&nbsp; &nbsp; &nbsp; &nbsp; 2. 列名部分采用了HBase的`cf:qualifier`形式, 
`ColumnFamily`与`Qualifier`之间以`:`分隔, 
因此想要查询HBase表`A`中的列`cf:1`时，需要在`cf:1`的首尾添加反引号\`cf:1\`

&nbsp; &nbsp; &nbsp; &nbsp; 3. HBase表以及Namespace的操作默认通过`org.apache.spark.sql.hbase.client.HBaseClientImpl`实现,
其中包含了从文件中读取hbase schema信息的功能,可以根据自身需求
修改spark_hbase.properties中`spark.hbase.client.impl`值

&nbsp; &nbsp; &nbsp; &nbsp; 4. yaml文件中为每个表增加了一个generator选项,
generator的值需要继承trait `org.apache.spark.hbase.execution.RowKeyGenerator`,
且必须有一个无参数的构造方法,通过实现`genRowKey`方法可以为插入的数据生成RowKey


# GetStarted
&nbsp; &nbsp; 1. 替换resource目录下hbase-site.xml，修改spark_hbase.properties的各属性值

&nbsp; &nbsp; 2. 根据需要,运行项目下test/scala中的 `org.apache.spark.sql.hbase.client.TestHBase`的`createUserNamespaceAndTable()`, 
`insertData()`以及
`scan()`方法新建表以及生成数据

&nbsp; &nbsp; 3. 执行`org.apache.spark.sql.hbase.HBaseSQLClient`的main方法启动程序,
输入sql语句检查程序是否正常运行

# 部分截图如下(字太小看不清楚可以手动放大查看)
## 程序查询语句：
&nbsp; &nbsp; show databases;

&nbsp; &nbsp; show tables;

&nbsp; &nbsp; select * from hbase.meta;

&nbsp; &nbsp; select * from pw.test where \`A:A_00\` like "%24%";

![Image text](https://github.com/wangpy1995/Spark-SQL-HBase/blob/master/src/main/resources/show/SELECT_QUERY.png)
## 插入语句截图如下:

&nbsp; &nbsp; use pw;

&nbsp; &nbsp; insert into test_insert  values('0000', 'TestSql');

&nbsp; &nbsp; select * from test_insert;

&nbsp; &nbsp; insert into test_insert  values('0000', 'TestSql');

&nbsp; &nbsp; insert into test_insert  values('0000', 'TestSql');

&nbsp; &nbsp; select * from test_insert;

![Image text](https://github.com/wangpy1995/Spark-SQL-HBase/blob/master/src/main/resources/show/INSERT_QUERY.png)

&nbsp; &nbsp;注:默认的RowKeyGenerator的rowKey规则是每次插入增加1,因此values中的'0000'不生效符合预期

####由于环境限制，程序还有许多部分未来得及测试，诸多不完善之处，还请大家多多提出宝贵意见
