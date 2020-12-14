package com.xyz.engine

import akka.actor.ActorSystem
import com.typesafe.config.Config
import com.xyz.common.AkkaUtils
import com.xyz.engine.intepreter.SparkInterpreter
import com.xyz.utils.{GlobalConfigUtils, ZKUtils}

/**
 * 平台服务启动的入口
 */
object App {

  /**
   *
   * 所有我们要对传入的参数做一些解析成自己想要的结构
   * @param args
   * @return
   */
  def parseArgs(args: Array[String]): Map[String, String] = {
    var argsMap: Map[String, String] = Map()
    //("-engine.zkServers",2,3)
    //("-engine.tag",3,5)
    var argv:List[String] = args.toList
    while (argv.nonEmpty) {
      argv match {
        /**
         * fun(a,b,c){
         *
         * }
         * fun(1,2,3)
         */
        //()->(tail)-(value,tail)->("-engine.zkServers",value,tail)
        //()->("-engine.zkServers",value,tail)
        // a ++;

        case "-engine.zkServers" :: value :: tail => {
          argsMap += ("zkServers" -> value)
          argv = tail
        }
        case "-engine.tag" :: value :: tail => {
          argsMap+=("engine.tag"->value)
          argv = tail
        }
        case Nil=>
        case tail=>{
          println(s"对不起，无法识别：${tail.mkString(" ")}")
        }

      }
    }

    argsMap
  }

  def main(args: Array[String]): Unit = {

    //测试字符串解析
    //val tpmArgs = Array("-engine.zkServers","node01:2181")
    //val tpmArgs2 = Array("-engine.tag","tag_1","tag_2")
    //parseArgs(tpmArgs2)
    //构建spark的解析器
    //    val interpreter = new SparkInterpreter
    //    val sparkConf = interpreter.start()
    //    sparkConf.set("spark.driver.host","localhost")
    //测试zk注册
    val argszk = parseArgs(args)
    val zkServer = argszk.getOrElse("zkServers", GlobalConfigUtils.getProp("zk.servers"))
    val zkClient = ZKUtils.getZkClient(zkServer)
    println(zkServer)
    println(zkClient)
    //测试engineSession的创建
    //获取akk服务信息
    val actorConf: Config = AkkaUtils.getConfig(zkClient)
    //注册akka信息
    val actorSystem = ActorSystem("system", actorConf)

    //获取当前akka的参数
    val hostname = actorConf.getString("akka.remote.netty.tcp.hostname")
    val port = actorConf.getString("akka.remote.netty.tcp.port")

    val engineSession = new EngineSession(s"${hostname}:${port}", null)
    println(engineSession)
  }
}
