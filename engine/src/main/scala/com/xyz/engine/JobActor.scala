package com.xyz.engine

import akka.actor.{Actor, Props}
import com.xyz.domain.engine.Job
import com.xyz.engine.intepreter.SparkInterpreter
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

  //处理业务
  override def receive: Receive = {
    null
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

}

object JobActor {
  def apply(
             sparkInterpreter: SparkInterpreter,
             engineSession: EngineSession,
             sparkConf: SparkConf): Props = {
    Props(new JobActor(sparkInterpreter, engineSession, sparkConf))
  }
}