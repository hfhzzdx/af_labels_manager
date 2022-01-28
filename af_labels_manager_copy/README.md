## 部署说明
### 数据源
目前2.0版本通过mysql数据库tbl_labels数据表获取所以需要打标签的任务表和标签算法
### 配置文件
全局配置文件路径: resource/global.properties
任务配置文件路径：通过读取全局配置文件中job.config.path配置项获得，如：job.config.path=jobConfig/HN/job.yaml
### 核心任务
核心任务有两个,com.aofeng.label.job.BaseLabelsJob && com.aofeng.label.job.SaveToESWithMarketsJob     
BaseLabelsJob,读取hives数据,进行打标签操作,最终结果入hive表.配置文件在resource/jobConfig/HN/job.yaml    
SaveToESWithMarketsJob,读取hive数据,同步至ES.配置文件在resource/jobConfig/HN/job.yaml    
任务拆分,原因有二. 
- 任务调度每月一次,为避免客户高峰期使用,选择入ES时间为半夜
- 受河南移动集群环境影响,hadoop集群节点和mysql、redis、es节点网络不互通。故有关入es、redis、mysql的所有操作，需以local模式运行。
   BaseLabelsJob则可以以yarn模式运行。
### 其他任务
- com.aofeng.label.job.GetBaseStation:转换计算基站对应user_id标签人数
- com.aofeng.label.job.LoadHiveToESJob:同步hive数据到ES集群
- com.aofeng.label.job.LoadHiveToMysqlJob:同步hive数据到Mysql数据库。默认append。结果数据保留一个月
- com.aofeng.label.job.LoadHiveToRedisJob:同步hive数据到Redis数据库。    
每个任务的配置文件和job名字一致，配置文件路径：resource/jobConfig/任务名.properties
