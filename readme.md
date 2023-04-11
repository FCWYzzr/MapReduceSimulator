# MapReduce Java模拟
## 目录
- Quick Start
- 包/类路径
- 类概述
- 工作流程
## Quick Start
    修改 map_reduce.Launcher 类中静态字段 filename 为目标文件名（无拓展名）
    
    修改 map_reduce.MapViewer 类中静态字段 filename 为目标文件名（无拓展名）
    
    构建项目

    以Launcher为入口点运行项目（Java版本为19）

    以MapViewer为入口点运行项目（Java版本为19）


## 包/类路径
### __map_reduce__
- ### __fake_net__
- - **Connection**
- - **IntermediateMap**
- - **MappingChannel**
- - **ReducingChannel**
- ### __logging__ 
- - **Logger**
- - - **Level**
- ### __work__
- - **Master**
- - **Worker**
- **Launcher**
- **MapViewer**

## 类概述
### Connection
&emsp;&emsp;网络连接模拟类，通过队列和阻塞队列模拟网络通讯
### IntermediateMap
&emsp;&emsp;reduce操作传输的可序列化对象
### MappingChannel
&emsp;&emsp;模拟mapping网络连接的通道
### ReducingChannel
&emsp;&emsp;模拟reducing网络连接的操作
### Logger
&emsp;&emsp;日志类
### Logger.Level
&emsp;&emsp;日志级别
### Master
&emsp;&emsp;Master机器模拟类
### Worker
&emsp;&emsp;Worker机器模拟类
### Launcher
&emsp;&emsp;测试入口
### MapViewer
&emsp;&emsp;结果文件查看器

## 工作流程
- ### Launcher


    
    启动Worker
    向Master提交任务
    启动Master

    等待result队列消息
    将结果序列化写入文件
- ### Master
    

    分发任务（Mapping和Reducing） 间隔10 ms
    收集结果（Mapping和Reducing） 间隔10 ms
    检查Worker存活和重分配任务     间隔5 s

- ### Worker


    从任务队列读取任务
    按照自身角色执行任务
    
