package com.xyz.engine

import java.io.{ByteArrayOutputStream, PrintStream}

import akka.actor.{Actor, Props}
import com.xyz.domain.{CommandMode, ResultDataType}
import com.xyz.domain.engine.{Instruction, Job}
import com.xyz.engine.interpreter.SparkInterpreter
import com.xyz.logging.Logging
import com.xyz.utils.{GlobalConfigUtils, ZKUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * akk模型
 *
 * @param _interpreter  解析器
 * @param engineSession 引擎session
 * @param sparkConf     sparkConf
 */
class JobActor(
                _interpreter: SparkInterpreter,
                engineSession: EngineSession,
                sparkConf: SparkConf) extends Actor with Logging {
  //组装jobActor的有效路径，存储到zookeeper（前端通过这个路径，可以获取akka引擎，然后来做前后端对接的操作）
  val valid_engine_path = s"${ZKUtils.valid_engine_path}/${engineSession.engineInfo}_${context.self.path.name}"
  var zkClient: ZkClient = _
  var token: String = _
  var job: Job = _

  var sparkSession: SparkSession = _
  var interpreter: SparkInterpreter = _

  //初始化
  override def preStart(): Unit = {
    warn(
      """
        |               __                           __                 __
        |_____    _____/  |_  ___________    _______/  |______ ________/  |_
        |\__  \ _/ ___\   __\/  _ \_  __ \  /  ___/\   __\__  \\_  __ \   __\
        | / __ \\  \___|  | (  <_> )  | \/  \___ \  |  |  / __ \|  | \/|  |
        |(____  /\___  >__|  \____/|__|    /____  > |__| (____  /__|   |__|
        |     \/     \/                         \/            \/
      """.stripMargin)
    zkClient = ZKUtils.getZkClient(GlobalConfigUtils.getProp("zk.servers"))
    interpreter = _interpreter
    sparkSession = EngineSession.createSpark(sparkConf).newSession()

    ZKUtils.registerActorInPlatEngine(zkClient, valid_engine_path, engineSession.tag.getOrElse("default"))
  }

  /**
   * 每接收一条消息调用一次
   *
   * @return
   */
  //处理业务
  override def receive: Receive = {
    //匹配指令
    case Instruction(commandMode, instruction, variables, _token) => {
      actorHook() {
        () =>
          //组装命令
          var assemble_instruction = instruction
          token = _token
          //封装job
          job = Job(instruction = instruction, variables = variables)
          var globalGroupId = GlobalGroupId.groupId
          job.engineInfoAndGroup = s"${engineSession.engineInfo}_${globalGroupId}"
          //将job发送到客户端
          sender ! job

          //清理spark的信息
          //提前清理可能存在的线程副本
          sparkSession.sparkContext.clearJobGroup()
          //设置作业的描述
          sparkSession.sparkContext.setJobDescription(s"running job:${instruction}")
          //设置线程组ID，后面的作业，由这个线程启动
          sparkSession.sparkContext.setJobGroup("groupId:" + globalGroupId, "instruction:" + instruction)
          //匹配命令
          commandMode match {
            case CommandMode.SQL =>
            case CommandMode.CODE =>
              //打印命令
              //   info("\n" + ("*" * 80) + "\n" +assemble_instruction + "\n" + ("*" * 80))
              //job执行模式
              // job.mode = CommandMode.CODE
              //替换字符串
              assemble_instruction = assemble_instruction.replaceAll("'", "\"").replaceAll("\n", " ")
              //执行代码
              val response = interpreter.execute(assemble_instruction)
            case _ =>
          }

      }
    }
  }

  //结束
  override def postStop(): Unit = {
    warn(
      """
        |             _                       _
        |  ____  ____| |_  ___   ____     ___| |_  ___  ____
        | / _  |/ ___)  _)/ _ \ / ___)   /___)  _)/ _ \|  _ \
        |( ( | ( (___| |_| |_| | |      |___ | |_| |_| | | | |
        | \_||_|\____)\___)___/|_|      (___/ \___)___/| ||_/
        |                                              |_|
      """.stripMargin)
    interpreter.close()
    sparkSession.stop()
  }

  /**
   * 钩子：通过钩子能够找到操作的对象,类似引用
   * 作用:捕获业务处理异常，方便发送到客户端进行跟踪
   *
   * @param func
   */
  def actorHook()(func: () => Unit): Unit = {
    try {
      func()
    } catch {
      case e: Exception => {
        val out = new ByteArrayOutputStream();
        e.printStackTrace(new PrintStream(out))
        val job = Job(data = out.toString(), dataType = ResultDataType.ERROR)
        //通过sender 把job对象发送到客户端
        sender ! job
      }
    }
  }
}


object JobActor {
  def apply(sparkInterpreter: SparkInterpreter, engineSession: EngineSession, sparkConf: SparkConf): Props = {
    Props(new JobActor(sparkInterpreter, engineSession, sparkConf))
  }
}