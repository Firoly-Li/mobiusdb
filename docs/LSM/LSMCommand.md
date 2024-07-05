## LSMCommand

LSM本身内部是高度聚合的，对外只提供了一系列接口，细节功能并不对外暴露。这一系列接口是：

**1、Append**

向LSM系统中添加数据

((*Vec*<*FlightData*>, *oneshot*::*Sender*<*bool*>)),

**2、OffsetList**

返回指定数据表的索引

((*String*, *oneshot*::*Sender*<*Vec*<*Offset*>>)),

**3、Table**

查询指定表的数据，

((*String*, *oneshot*::*Sender*<*Option*<*RecordBatch*>>)),

 **4、Query**

通用查询接口，支持SQL

((*String*, *oneshot*::*Sender*<*Option*<*RecordBatch*>>)),

**5、TableList**

查询LSM系统维护的所有的表

(*oneshot*::*Sender*<*Option*<*Vec*<*TableName*>>>),