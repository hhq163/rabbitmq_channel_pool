# rabbitmq_channel_pool
rabbitmq连接池golang实现

采用单条连接，IO多路复用方式，rabbitmq连接池和我们普通连接池的实现方式稍有差别，不是建立多个tcp连接，而是使用channel pool的方式实现，性能很好，分享出来

# 性能测试数据

序号 | 消息大小（byte） |  测试结果（s）  
-|-|-
1 | 85 | 41250 |
2 | 168 | 31250 |
