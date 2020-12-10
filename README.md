[TOC]



# 1、项目需求

数据类型:csv,orc,excel,word,txt,log

数据存储位置:不是在一台机器上，第三方，公司内部多个系统，

因为历史积累原因。公司的数据集存储在了不同的引擎：redis、mysql、hbase、hdfs、elasticsearch、kafka等

因为早集团没有统一规划数据源，导致数据源多达十几种；所以不好规划统一 ，导致查询关联及其麻烦（要各种预先抽取多个数据源到同一个地方，然后在做统一处理，最后出报表 ，而且查询及其缓慢）



# 2、项目架构说明

1、前后端任务交换指令：Akka

2、计算引擎：sparkSQL

3、二次定义sparkSQL语法：Antlr

4、服务自动发现：zookeeper

【逻辑架构】

![image-20200226101743131](image/image-20200226101743131.png)

【项目架构】

![image-20200229121357540](image/image-20200229121357540.png)





# 3、最终目标



## 3.1、项目的实现细节

![image-20200226205823252](image/image-20200226205823252.png)



# 4、Actor入门

通讯原理图:

![img](assets/421906-20190303184058021-556718746.png)

## 4.1、java和scala在并发编程模型对比：

| Java内置线程模型                                             | Scala Actor模型                                      |
| ------------------------------------------------------------ | ---------------------------------------------------- |
| “共享数据锁模型”                                             | share nothing                                        |
| 每个object有一个monitor，监视多线程对共享数据的访问【线程内部】 | 不共享数据，actor之间通过message传递（基于事件驱动） |
| 加锁的代码通过synchronized标志                               |                                                      |
| 死锁的问题                                                   |                                                      |
| 每个线程内部是顺序执行的                                     | 每个actor内部是顺序执行的                            |



## 4.2、Actor的执行顺序

1、调用start（）方法启动Actor

2、调用start（）方法后其act()方法会被执行

3、向Actor发送消息



## 4.3、发送消息的方式

| !    | 发送异步消息，没有返回值。           |
| ---- | ------------------------------------ |
| !?   | 发送同步消息，等待返回值。           |
| !!   | 发送异步消息，返回值是 Future[Any]。 |

## 4.4、Actor例子

```xml
 <dependency>
     <groupId>org.scala-lang</groupId>
     <artifactId>scala-actors</artifactId>
     <version>2.11.8</version>
 </dependency>
```



### 4.1.1、Actor可以不断的接收消息

![image-20200226112623582](image/image-20200226112623582.png)



驱动程序

![image-20200226112642299](image/image-20200226112642299.png)

说明：在act()方法中加入了while (true) 循环，就可以不停的接收消息

注意：发送start消息和stop的消息是异步的，但是Actor接收到消息执行的过程是同步的按顺序执行



### 4.1.2：结合case class发送消息

#### 1）：写3个case class

![image-20200226132545991](image/image-20200226132545991.png)



#### 2）：接收消息的actor

![image-20200226132614679](image/image-20200226132614679.png)



#### 3）：驱动

![image-20200226132638126](image/image-20200226132638126.png)



# 5：Akka入门

akka，一款高性能，高容错，分布式的并行框架

特点:

1.并行与并发

2.异步非阻塞

3.容错

4.持久化

5.轻量级，每个actor占用内存比较小 300byte,1G 内存容纳300w个actor



场景:

分布式计算中的分布式通讯，解决的是高并发场景的问题，（消息体比较小），吞吐量不是很高，零拷贝（）

密集型计算场景

总结：对高并发和密集型的计算场景，akka都可以使用



## 5.1、使用Akka来进行消息传递

![image-20200226150503739](image/image-20200226150503739.png)



驱动：

![image-20200226150523120](image/image-20200226150523120.png)



pom里面需要引入akka的配置:

```xml
   <dependency>
            <groupId>com.typesafe.akka</groupId>
            <artifactId>akka-remote_2.11</artifactId>
            <version>2.5.9</version>
        </dependency>
```



# 6、工程搭建



## 6.1、工程模块的创建

1）：创建模块名称和工程包名

![image-20200226225147512](image/image-20200226225147512.png)



## 6.2、给工程添加依赖关系

各个模块配置工程以来pom.xml文件

## 6.3：编写驱动程序

本节的目的：

把驱动程序编写好，并启动起来；

但是让驱动能够顺利启动，我们需要完成如下操作：

![image-20200229212621597](image/image-20200229212621597.png)



### 6.3.1、编写参数的获取和校验操作

![image-20200226230550952](image/image-20200226230550952.png)



### 6.3.2、构建解释器基类

![image-20200226231204688](image/image-20200226231204688.png)



然后写任务状态的基类：

![image-20200226165224753](image/image-20200226165224753.png)

在提供对外的解释器接口

![image-20200227010740434](image/image-20200227010740434.png)



### 6.3.3、创建文本解析标识的代码包

![image-20200226231501546](image/image-20200226231501546.png)





### 6.3.4、对解释器基类的增强

![image-20200226233012070](image/image-20200226233012070.png)



#### 第一步：在伴生类中提供换行符匹配操作（就是个正则表达式）

![image-20200227010827958](image/image-20200227010827958.png)





#### 第二步：对接口进行功能增强

问题:对spark-shell绑定变量的作用是什么?

![image-20200226233516692](image/image-20200226233516692.png)



#### 第三步：spark-shell绑定变量

spark-shell绑定变量，第一个要绑定的就是sparkSession

以及：sparkContext下面的内容、隐士转换、sparkSQL、udf函数等内容



但是现在我们还没有sparkSession ， 所以我们先实现一个sparkSession的构建

【com.kkb.engine下面构建EngineSession】



![image-20200226234148574](image/image-20200226234148574.png)

因为执行了enableHiveSupport，所以需要加入服务器的hive-site.xml文件

![image-20200226234416770](image/image-20200226234416770.png)



然后启动metastore服务

![image-20200226234614813](image/image-20200226234614813.png)



因为我们参照的是livy代码，本身就是一个rest 服务，用来做spark和web端的一种交互；

所以livy很好的帮我们解决了，spark-shell从初始化的绑定、到绑定变量错误的处理；

都已经帮我们解决好了

![image-20200227001748520](image/image-20200227001748520.png)

![image-20200227001921833](image/image-20200227001921833.png)

![image-20200227002054594](image/image-20200227002054594.png)

![image-20200227002145504](image/image-20200227002145504.png)



### 6.3.5、实现spark解释器

![image-20200227002831717](image/image-20200227002831717.png)

![image-20200227003136968](image/image-20200227003136968.png)

![image-20200227003619514](image/image-20200227003619514.png)

![image-20200227003731607](image/image-20200227003731607.png)



这样，我们构建好了spark的解释器，实际就是为了构建一个属于自己的spark-shell；

好在其他框架实现了，我们只需要把其他框架的源码拿来即可

所以我们在回到 驱动程序App类：

【App类】

![image-20200227004109191](image/image-20200227004109191.png)





### 6.3.6：获取zk的客户端

这样，我们继续按照流程往下走 ， 那么此时就要构建zk的客户端了

因为，我们后面会把 引擎注册到zk里面，并且依赖于zk进行服务的自动发现

【在common工程下，创建zk的工具类】

#### 第一步：导包

我们采用第三方的zk工具，尽量帮我们封装代码

```xml
<dependency>
    <groupId>com.101tec</groupId>
    <artifactId>zkclient</artifactId>
    <version>0.7</version>
</dependency>
```

然后创建ZKUtils工具类

![image-20200227004951874](image/image-20200227004951874.png)

#### 第二步：编写获取zk客户端代码

1）：

![image-20200227005235931](image/image-20200227005235931.png)



2）：

![image-20200227005617623](image/image-20200227005617623.png)



3）：

![image-20200227005931483](image/image-20200227005931483.png)



4）：

把之前写的加载配置文件的工具类，放入common工程下

![image-20200227012355971](image/image-20200227012355971.png)



然后把common工程打入engine工程里面

就是在engine的pom文件里面添加common工程包

```xml
<dependency>
    <groupId>com.kkb.platform</groupId>
    <artifactId>common</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```



![image-20200227012602948](image/image-20200227012602948.png)



5）：

![image-20200227012759219](image/image-20200227012759219.png)



### 6.3.7、注册Akka并发变成模型

#### 第一步：把基本信息类导入

![image-20200227014728761](image/image-20200227014728761.png)



#### 第二步：考虑引擎在zk中的情况

那么，我们如果注册引擎，其实就是把PlatEngine这个类注册到zk里面去；

那么问题来了：

最开始运行的时候 ，zk里面肯定没有引擎信息(所谓引擎信息 ， 我们认为其实就是id --> ip:port)；

1 --> node1:3001

2--->node2:3002



但是如果不是最初启动时候，那么引擎肯定是存在的，那么我们就要确保，再次注册引擎的时候 ，

id--->ip:port

这里面的id和端口，绝对不能重复

![image-20200227015836730](image/image-20200227015836730.png)



所以我们需要让id和端口绝对不一致，那么最好的方式就是用zk来维护状态 ， 如果这个状态存在，那么就将id和端口递增



![image-20200227021538703](image/image-20200227021538703.png)



#### 第三步：编写引擎注册到zk的代码

注册引擎，大概分成4个步骤：

1、准备好目录信息

2、创建引擎的父目录

3、创建引擎的临时节点（数据写入节点信息）

4、把前3步合并



那么接下来，我们来实现这个功能：

##### 1、准备好目录信息

![image-20200228110702511](image/image-20200228110702511.png)



##### 2、创建引擎的父目录

如果上面这个路径在zk里面是不存在的，那么直接创建一个永久节点路径；作为引擎的存储路径

![image-20200228111049584](image/image-20200228111049584.png)



##### 3、创建引擎的临时节点（数据写入节点信息）

这样，有了父目录之后，我们就可以把数据信息写入；

![image-20200228121027512](image/image-20200228121027512.png)



所以健壮的写法是，必须考虑，如果父目录不存在的情况：

![image-20200228123230934](image/image-20200228123230934.png)



##### 4、合并前3步，完成引擎的注册

![image-20200228130021725](image/image-20200228130021725.png)

‘



#### 第四步：封装注册引擎代码，并返回akka配置信息

##### 1、创建包和类

![image-20200228152051356](image/image-20200228152051356.png)



##### 2、

![image-20200228152619122](image/image-20200228152619122.png)



##### 3、利用我们刚刚写的zk方法，进行注册

![image-20200228152827421](image/image-20200228152827421.png)

#### 第五步：需要考虑，如果后续需要注册多个引擎，怎么办

##### 1、去zk里面查看是否有已经存在的引擎

###### 1.1）：在zk里面添加查询子节点的功能

![image-20200228163034784](image/image-20200228163034784.png)

获取完子节点后，把里面的数据拿出来，就是IP:port

###### 1.2）：获取子节点里面的数据

![image-20200228171702935](image/image-20200228171702935.png)

###### 1.3）：将返回的元组：(Option[String , Stat]) ， 封装成引擎信息

【将1.2代码利用到1.3】

![image-20200228172106022](image/image-20200228172106022.png)



###### 1.4）：合并前三步

我们第一步获取子节点名称（id：1、2、3）

然后根据子节点的名称，来通过第第三步，来获取具体的引擎信息

所以，我们这一步做个合并处理

![image-20200228182805326](image/image-20200228182805326.png)



这样我们封装好了，如何获取zookeeper里面的引擎信息，那么按照步骤，我们接下来要顺序注册引擎

比如：

最开始

id = 1 ， port = 3000

顺序增长

id = 2 ， port = 3001

##### 2、顺序增长id和port

那么顺序增长的前提是，当前zookeeper里面已经存在这个引擎了，所以才会顺序增长

![image-20200228184339213](image/image-20200228184339213.png)





最后在把我们刚刚写好的，注册引擎拿到最下面，顺序增长完port和id以后，开始注册

##### 3、把注册代码拿下来

![image-20200228184540919](image/image-20200228184540919.png)



### 6.3.8、回到驱动类，注册Akka信息

这样，我们在回到驱动类，注册Akka信息

![image-20200228184831942](image/image-20200228184831942.png)



### 6.3.9：测试上面的代码

然后我们在测试下，上面写的代码是不是达到预期的效果

![image-20200228191315865](image/image-20200228191315865.png)

然后去zookeeper里面查看

![image-20200228191413561](image/image-20200228191413561.png)



### 6.3.10、获取当前akka的参数

经过测试，我们上面写的代码没有任何问题；

那么接下来，我们要获取当前akka的地址（也就是引擎的地址）

![image-20200228195113493](image/image-20200228195113493.png)



### 6.3.11、把引擎信息，维护到EngineSession里面

我们在构建spark-shell功能时候，把sparkSession维护到了EngineSession里面了

那么干脆 ， 我们就把这个类作为任务状态的统计类；

那么我们把引擎信息，也维护进来

![image-20200228200531271](image/image-20200228200531271.png)



然后我们回到驱动程序，实例化一个EngineSession

![image-20200228200733678](image/image-20200228200733678.png)



### 6.3.12、设置并行度

那么接下来，我们就要启动Akka了 ；

![image-20200229101416642](image/image-20200229101416642.png)





![image-20200229104149185](image/image-20200229104149185.png)





这样，我们有了内部参数的维护，那么我们在返回驱动程序 ， 把并行度设置上

![image-20200229104215439](image/image-20200229104215439.png)



### 6.3.13、创建Akka模型

我们之所以设置并行度，最终目标就是要并行的启动Akka任务模型

所以接下来，我们要创建 一个Akka的任务模型

![image-20200229110101464](image/image-20200229110101464.png)



### 6.3.14：启动Akka模型

现在我们有了akka模型 ， 而且我们也拿到了并行度

接下来启动所有模型

![image-20200229110527970](image/image-20200229110527970.png)



这样我们就正常的启动了；

但是有这样一种可能，就是很可能主线程提前结束了，子线程还在继续运行；

就是可能会出现僵尸进程！



### 6.3.15：避免出现僵尸进程

所以我们需要让主线程等待子线程结束后，在执行关闭回收操作

所以我们在EngineSession里面，添加一些功能，让主线程等待子线程

![image-20200229114026613](image/image-20200229114026613.png)



![image-20200229114459570](image/image-20200229114459570.png)



![image-20200229150815199](image/image-20200229150815199.png)



然后让JobActor继承这个日志功能

![image-20200229150935521](image/image-20200229150935521.png)







# 7、编写JobActor

## 7.1、编写jobActor的初始化preStart阶段

### 7.1.1、将jobActor注册到zookeeper中

#### 第一步，在ZKUtiils里面添加引擎路径

![image-20200301150403124](image/image-20200301150403124.png)



#### 第二步：在jobActor里拼接引擎路径

![image-20200301151133784](image/image-20200301151133784.png)



#### 第三步：初始化zk客户端

![image-20200301152207863](image/image-20200301152207863.png)



因为我们来初始化zk的客户端

![image-20200301152512876](image/image-20200301152512876.png)



#### 第四步：将actor的引擎，注册到zk

首先我们在ZKUtils里面封装个方法，专门来对接jobActor的注册

![image-20200301154915611](image/image-20200301154915611.png)



然后在jobActor的初始化里面，进行注册jobActor的引擎

![image-20200301155245818](image/image-20200301155245818.png)



然后启动测试，查看zk里面是否注册进去

![image-20200301155925856](image/image-20200301155925856.png)

测试结果

![image-20200301155944791](image/image-20200301155944791.png)



## 7.2、编写jobActor的结束阶段代码

首先我们把后续需要的一些变量提前初始化好：

1、spark的解释器

2、sparkSession

上面这俩变量，会在actor的生命周期结束时候进行回收

### 第一步：定义成员变量

![image-20200301164223196](image/image-20200301164223196.png)



### 第二步：初始化阶段给上面两个变量赋值

![image-20200301164328130](image/image-20200301164328130.png)



### 第三步：在postStop，actor的生命周期结束阶段对这俩变量进行回收

![image-20200301164416595](image/image-20200301164416595.png)



## 7.3 、编写jobActor的receive

首先我们要开始编写一个actor的钩子，目的很简单，万一出现了错误，我们可以 catch住这个错误，然后把错误回显给客户端（web端）

### 7.3.1、编写一个 actor的钩子

![image-20200301180055957](image/image-20200301180055957.png)



### 7.3.2、在receive里匹配指令

#### 1）：匹配指令 ， 添加actorHook，并初始化变量

![image-20200302101454086](image/image-20200302101454086.png)



#### 2）：编写一个获取全局唯一的任务组ID ， 因为后续会基于任务组ID来获取引擎信息



![image-20200302102948757](image/image-20200302102948757.png)

#### 3）：将job信息（包含任务组ID） ，返回给前端

![image-20200302103118286](image/image-20200302103118286.png)



#### 4）：更新线程副本里面的作业描述

之所以这样做：

1、可以对任务作出描述，方便任务的web端定位

2、有了任务的描述，那么后续是可以取消已经提交的任务

![image-20200302103752873](image/image-20200302103752873.png)



#### 5）：基于commandMode进行匹配具体操作

接下来，我们要匹配具体的操作，就是看传递过来的指令是代码还是SQL；

然后根据指令的不同，选择不同的操作方式

![image-20200302141525959](image/image-20200302141525959.png)



### 7.3.3：接收CODE，然后处理

定义一个变量assemble_instruction，来组装命令

![image-20200307152001994](image/image-20200307152001994.png)

接受命令：

![image-20200302154945358](image/image-20200302154945358.png)



![image-20200302161956423](image/image-20200302161956423.png)



#### 最后处理完后，把结果回显给客户端

上面处理后，会返回一个response的结果 ， 我们需要把结果回显给客户端

因此，我们需要接收这个response，然后解析他 ， 然后收集他



##### 第一步：去EngineSession添加一个记录任务的map集合

去EngineSession添加一个记录任务的map集合 ， 主要是为了保存批处理的作业信息



![image-20200302163508474](image/image-20200302163508474.png)



##### 第二步：根据执行的结果，存储job的状态

![image-20200302170312756](image/image-20200302170312756.png)



##### 第三步：响应任务状态

![image-20200302171027572](image/image-20200302171027572.png)



### 7.3.4：接收SQL，然后处理

SQL：select name from person where age > 18

sql执行流程(sql生命周期):

![image-20200302182348637](image/image-20200302182348637.png)



不管解析被划分为几步，在Spark 执行环境中，都要转化成RDD的调用代码，才能被spark core所执行

![image-20200302192314945](image/image-20200302192314945.png)



那么这里面有个关键的点，就是查询的SQL ， 怎么转化成Unresolved LogicalPlan；

Unresolved LogicalPlan 这个阶段接收的是抽象的语法树，所以我们需要知道的就是，这个SQL语句是怎么转成抽象语法树的；

答案就是：antlr4（spark是在2.0以后，开始使用antlr4解析的sql语法）

spark通过antlr4去解析SQL语句，形成抽象语法树AST；

也就是说，详细的流程是这样的：

![image-20200303140649183](image/image-20200303140649183.png)



#### 7.3.4.1：antlr的入门

##### 1）语法

- `grammar` 名称和文件名要一致
- Parser 规则（即 non-terminal）以小写字母开始
- Lexer 规则（即 terminal）以大写字母开始
- 用 `'string'` 单引号引出字符串



##### 2）：配置antlr的环境变量

首先你要有配置antlr的环境变量

```shell
export CLASSPATH=".:/usr/local/lib/antlr-4.5.3-complete.jar:$CLASSPATH"
alias antlr4='java -Xmx500M -cp "/usr/local/lib/antlr-4.5.3-complete.jar:$CLASSPATH" org.antlr.v4.Tool'
alias grun='java org.antlr.v4.runtime.misc.TestRig'
```



然后我们按照语法格式来写一个hello word的代码；

【随便打开 一个maven工程，然后导包】

```xml
<dependency>
    <groupId>org.antlr</groupId>
    <artifactId>antlr4-runtime</artifactId>
    <version>4.5.3</version>
</dependency>
```

###### 第一步：编写antlr文件

![image-20200303121223051](image/image-20200303121223051.png)



通过上面，我们大概了解，antlr其实主要就是写正则



###### 第二步：生成java代码

接下来，通过命令生成java代码

```
antlr4 learnAntlr.g4 
```

![image-20200303121837606](image/image-20200303121837606.png)



###### 第三步：重写learnAntlrBaseListener

![image-20200303122615079](image/image-20200303122615079.png)



###### 第四步：词法和语法解析

![image-20200303123221642](image/image-20200303123221642.png)



###### 第五步：运行测试

![image-20200303123335519](image/image-20200303123335519.png)





#### 7.3.4.2：在代码中编写antlr文件

![image-20200303141447185](image/image-20200303141447185.png)



#### 7.3.4.3：讲解antlr文件里面的意思

##### 1）：load的列子

假如我想实现加载文件操作，形成一个表，然后在基于这个表做查询操作；

以上的动作是：数据源--->load（加载）——>select这个表

比如：

那么我们在Engine.g4文件中就是：

![image-20200303144307204](image/image-20200303144307204.png)



他们的对应关系就是：

![image-20200303144216429](image/image-20200303144216429.png)



##### 2）：save的列子

比如我们从kafka记载了数据，形成tb表 ， 然后将数据写入mysql

简单说就是: kafka -->load -->save --->mysql



```sql
load kafka.veche4
where `spark.job.mode`="stream" 
and `kafka.bootstrap.servers`="39.100.249.234:9092" 
as tb; 


save tb as jdbc.`t3`
where 
driver="com.mysql.jdbc.Driver"      
and url="jdbc:mysql://cdh2:3306/test?useUnicode=true&characterEncoding=utf8"
and user="root"
and password="root"
and `failOnDataLoss`="false" 
and `outputMode`="Append" 
and `streamName`="Stream" 
and `duration`="2"
and `checkpointLocation`="/Users/angel/Desktop/data/S1_2020080120"
and coalesce="1";
```



那么对应我们antlr文件就是：

![image-20200303153620249](image/image-20200303153620249.png)


load操作，我们刚刚讲过了，接下来在说下，怎么save的

【看图做对比】

![image-20200303155109413](image/image-20200303155109413.png)



#### 7.3.4.4：最后把剩余的功能统一说下

![image-20200303160555795](image/image-20200303160555795.png)



#### 7.3.4.5：生成antlr的代码

命令：

```
antlr4 Engine.g4
```

![image-20200303162739086](image/image-20200303162739086.png)



### 7.3.5：重写EngineBaseListener

就像我们自己写的例子一样，此时我们要重写一下EngineBaseListener，对这个类的功能做增量，来满足我们的需求

#### 第一步：创建EngineSQLExecListener类【不带参数】

![image-20200303164047018](image/image-20200303164047018.png)



#### 第二步：重写EngineSQLExecListener里面的exitSql方法

重写这个方法的依据：

antlr4在离开sql的时候，会触发exitSql

![image-20200303164252153](image/image-20200303164252153.png)



所以我们要重写这块儿，来触发我们的业务逻辑

![image-20200303164639965](image/image-20200303164639965.png)



#### 第三步：测试上面的步骤

##### 1）：首先我们要先确保当前的流程一定是通的，所以我们要测试下，确保当前流程绝对没问题

所以我们写个方法，专门来执行词法 和语法的解析

![image-20200303165604622](image/image-20200303165604622.png)

##### 2）：然后我们测试下，看看操作是否通过

![image-20200303165702911](image/image-20200303165702911.png)

##### 3）：然后集成一下接收到命令后，怎么对接这块儿

![image-20200303173906741](image/image-20200303173906741.png)





##### 4）：启动App驱动，然后在test里面发送命令

![image-20200303172308184](image/image-20200303172308184.png)

#### 第四步：编写模式匹配的load操作

接下来，我们来完善load操作：

![image-20200303175143737](image/image-20200303175143737.png)



##### 1）：创建包

![image-20200303183125754](image/image-20200303183125754.png)





##### 2）：规范化sql里面的方法

编写trait来规范化操作，方便后期维护，也是一种面向接口的编程思想



![image-20200303184320426](image/image-20200303184320426.png)



##### 3）：编写load操作，LoadAdaptor

###### 第一步：编写整体的大框

![image-20200303191438474](image/image-20200303191438474.png)



###### 第二步：确定要解析的内容和成员变量

在这个里面，我们要解析语法：

```
LOAD format POINT? path WHERE? expression? booleanExpression* AS tablename
```



![image-20200303192306205](image/image-20200303192306205.png)



###### 第三步：遍历语法树的孩子节点

![image-20200303193309702](image/image-20200303193309702.png)



```
spark.job.mode = stream 就代表是流处理 ， 否则就是离线处理
```

![image-20200303194249408](image/image-20200303194249408.png)



##### 4）：EngineSQLExecListener这个类的模式匹配，将LoadAdaptor做关联

![image-20200303195002420](image/image-20200303195002420.png)



##### 5）：测试当前的LoadAdaptor

测试LoadAdaptor是否对当前节点树做了解析，那么直接打印一下这个option信息

![image-20200303201523924](image/image-20200303201523924.png)



在测试类，编写测试命令

离线：

```sql
val instruction = "load jdbc.testTableName " +
    "where driver=\"com.mysql.jdbc.Driver\" " +
    "      and url=\"jdbc:mysql://cdh1:3306/hive?characterEncoding=utf8\" " +
    "      and user=\"root\" " +
    "      and password=\"root\" " +
    "as tb; " +
    "SELECT * FROM tb LIMIT 100;"
```

流：

```sql
val instruction = "load kafka.`veche4`" +
      "where `spark.job.mode`=\"stream\" " +
      "and `kafka.bootstrap.servers`=\"39.100.249.234:9092\" " +
      "as tb; " +
      "save tb as hbase.`TES6`" +
      "where `hbase.zookeeper.quorum`=\"angel_rsong1:2181\" " +
      "and `hbase.table.rowkey.field`=\"id_\" " +
      "and `hbase.table.family`=\"MM\" " +
      "and `failOnDataLoss`=\"false\" " +
      "and `hbase.table.numReg`=\"10\" " +
      "and `hbase.check_table`=\"true\" " +
      "and `outputMode`=\"Append\" " +
      "and `streamName`=\"Stream\" " +
      "and `duration`=\"10\" " +
      "and `sendDingDingOnTerminated`=\"true\" " +
      "and `checkpointLocation`=\"/Users/angel/Desktop/data/ess312ldd\";"
```



#### 第五步：处理匹配离线处理的数据源

这一步，我们要根据刚刚load操作出来的结果，然后去匹配操作。然后去查询出数据

也就是;

load   —>匹配到离线 --->select结果

##### 1）：构建离线匹配数据源的类（BatchJobLoadAdaptor）

![image-20200306111220821](image/image-20200306111220821.png)



因为我们对接数据源是通过spark sql去处理的，所以我们先在EngineSQLExecListener这个监听者实现类里面添加sparkSession

这样我们的BatchJobLoadAdaptor（匹配离线数据源）就可以使用sparkSQL，去查询数据源的数据了

##### 2）：监听者实现类（EngineSQLExecListener），添加sparkSession

![image-20200306112742980](image/image-20200306112742980.png)





##### 3）：离线匹配数据源的类（BatchJobLoadAdaptor）开始根据sparkSession查询数据

![image-20200306112549529](image/image-20200306112549529.png)



###### 匹配数据源

![image-20200306113928729](image/image-20200306113928729.png)



![image-20200306114045495](image/image-20200306114045495.png)



最后回到JobActor ， 给EngineSQLExecListener添加sparkSession的参数

![image-20200306145634422](image/image-20200306145634422.png)



然后在LoadAdaptor里面添加上BatchJobLoadAdaptor匹配数据源的操作

![image-20200306150202828](image/image-20200306150202828.png)



##### 4）：接下来，做个简单的测试，看看能不能查出来数据

在BatchJobLoadAdaptor里面打印一下结果



![image-20200306150830229](/image/image-20200306150830229.png)

###### 

最后在测试类执行如下代码

![image-20200306150936703](image/image-20200306150936703.png)

结果：

![image-20200306150955088](image/image-20200306150955088.png)

出现如上结果，此时说明流程已经走通



#### 第六步：完成EngineSQLExecListener里面的select操作

sparkSession.sql（查询sql）的操作

![image-20200306154440394](image/image-20200306154440394.png)



所以 我们创建一个selectAdaptor类，来匹配select操作



![image-20200306171806403](image/image-20200306171806403.png)



##### 1）：然后编写selectAdaptor里面的内容：

![image-20200306173235155](image/image-20200306173235155.png)



##### 2）：执行sql语句，创建临时表，然后在把处理结果进行保存

![image-20200306181256650](image/image-20200306181256650.png)



加载这个结果的原因说明：

我们后面要将处理的结果返回给客户端 ， 并且还要将处理的结果保存起来；

比如说，后续公司需要开发一些功能，将处理后的数据结果，可以下载下来，保存成csv文件；

那么此时对处理结果进行保存，显然是必要的

![image-20200306174118213](image/image-20200306174118213.png)



###### 最后：

我们在JobActor里面封装一下解析SQL的操作，让我们解析并执行完SQL以后，可以拿到结果



#### 第七步：封装处理结果操作

我们返回的处理结果是dataFrame ， 所以我们需要将dataFrame转成json

这样在将结果返回给客户端(因为客户端没法直接解析dataFrame)

##### 1）：将dataFrame结果，解析成json

在adaptor工程下，写个方法，专门来将dataFrame转成json

方法：

使用SparkSQL内置函数接口开发StructType/Row转Json函数，我们以前肯定使用过：

【列子】

```scala
def main(args: Array[String]): Unit = {
  val spark = getSparkSession(getSparkConf)
  val df = spark.createDataFrame(Seq(
    ("ming", 20, 15552211521L),
    ("hong", 19, 13287994007L),
    ("zhi", 21, 15552211523L)
  )) toDF("name", "age", "phone")

  val data: Dataset[String] = df.toJSON
  println(data.collect())
}
```

跟踪上述，发现最终都会调用Spark源码中的org.apache.spark.sql.execution.datasources.json.JacksonGenerator类，使用Jackson，根据传入的StructType、JsonGenerator和InternalRow，生成Json字符串。

![image-20200306184128194](image/image-20200306184128194.png)



JacksonGenerator类

![image-20200306183801547](image/image-20200306183801547.png)



【我们在adaptor工程下使用】

![image-20200306185941917](image/image-20200306185941917.png)



##### 2）：编写结果的报告处理和结果落地保存

![image-20200306190750386](image/image-20200306190750386.png)



##### 3）：把解析SQL操作和汇报结果操作进行封装

![image-20200306191612342](image/image-20200306191612342.png)

##### 4）：最后在匹配SQL处理处 ， 使用parseSQL方法



![image-20200306191838030](image/image-20200306191838030.png)



##### 5）：测试上面的所有操作

![image-20200306174244108](image/image-20200306174244108.png)

###### 在汇报结果的方法reportResult里面，对处理的结果展示一下

![image-20200306192441584](image/image-20200306192441584.png)



###### 【最后结果展示】

sql的结果

![image-20200306192540469](image/image-20200306192540469.png)

hdfs上的数据

![image-20200306192605230](image/image-20200306192605230.png)



### 7.3.6、编写存储数据SaveAdaptor

![image-20200307101105874](image/image-20200307101105874.png)



在存储数据时，我们需要考虑两个场景：

```
1：考虑场景问题：离线、流式
2：考虑数据格式问题：json、text、hbase、mysql....
```

#### 7.3.6.1、创建SaveAdaptor类

![image-20200307100950614](image/image-20200307100950614.png)



#### 7.3.6.2、分析antlr里面的save操作

```sql
('save'|'SAVE') (overwrite | append | errorIfExists | ignore | update)* tableName 'as' format '.' path ('where' | 'WHERE')? expression? booleanExpression* ('partitionBy' col)? ('coalesce' numPartition)?
```

上面的语法解析，实际就是解析类似如下的SQL

![image-20200307101919402](image/image-20200307101919402.png)



#### 7.3.6.3、写好局部变量，对应解析后的值

接下来要提供一些局部变量，我们解析出来值后，赋值给这些局部变量

![image-20200307134832183](image/image-20200307134832183.png)



#### 7.3.6.4、遍历节点树，解析每一个节点

接下来遍历节点，按照我们自己写的antlr语法规则，开始遍历语法规则

![image-20200307141654330](image/image-20200307141654330.png)



#### 7.3.6.5、判断当前的save操作是流处理还是批处理

##### 1）：在EngineSQLExecListener里面添加加载流的配置操作

![image-20200307144448650](image/image-20200307144448650.png)



##### 2）：在load操作的时候LoadAdaptor，添加流的标志

![image-20200307144652111](image/image-20200307144652111.png)



##### 3）：在save操作的时候，判断是流处理还是批处理

![image-20200307145006039](image/image-20200307145006039.png)



#### 7.3.6.6、开发save操作的批处理的sink源匹配

##### 1）：创建BatchJobSaveAdaptor类

![image-20200307150035441](image/image-20200307150035441.png)



##### 2）：对format（数据源）进行匹配，根据需求落地到不同的源

![image-20200307151423629](image/image-20200307151423629.png)





##### 3）：编写一个最简单的数据落地操作text格式数据落地

###### 第一步：封装写入操作

![image-20200307151551096](image/image-20200307151551096.png)



###### 第二步：实现text的写操作

![image-20200307152325271](image/image-20200307152325271.png)



##### 4）：SaveAdaptor中添加批处理的落地流程

![image-20200307171844892](image/image-20200307171844892.png)



##### 5）：EngineSQLExecListener监听器实现者里面调用SelectAdaptor

![image-20200307172014619](image/image-20200307172014619.png)



##### 6）：测试

![image-20200307174113415](image/image-20200307174113415.png)



![image-20200307174129100](image/image-20200307174129100.png)



