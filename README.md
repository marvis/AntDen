                         AntDen 说明


===================================简介=====================================

AntDen(蚂蚁窝)是一个开源的通用计算平台


AntDen包括如下四部分：

    I. 调度器(scheduler)
    II. 控制器(controller)
    III. 客户端(slave)
    IV. 执行器(executer)

===================================安装=====================================

    1. 确定已经安装了MYDan工具, 项目地址 https://github.com/MYDan/mayi.git

    2. clone 本项目到/opt目录下

    3. 添加调用插件到mydan中

        cp /opt/AntDen/slave/code/antden  /opt/mydan/dan/agent/code/antden

    4. 添加工具到系统环境

        cp /opt/AntDen/bin/antden /bin/

    5. 运行AntDen命令(注: 命令是全小写)

        antden

===================================使用=====================================

1.在所有客户端上启动slave:

    antden slave start

2.在控制机上启动调度器和控制器

    antden scheduler start
    antden controller start

3.在控制机上添加slave到集群

    antden scheduler addmachine 127.0.0.1

4.提交一个作业

    antden scheduler submitjob --file /opt/AntDen/example/job.yaml
