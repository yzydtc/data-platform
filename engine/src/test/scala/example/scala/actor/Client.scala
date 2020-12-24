package example.scala.actor

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.pattern.Patterns
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.xyz.domain.CommandMode
import com.xyz.domain.engine.Instruction

import scala.concurrent.duration.Duration

object Client extends App {
  //客户端地址
  val host = InetAddress.getLocalHost.getHostAddress
  //客户端端口号
  val port = 3001

  val conf = ConfigFactory.parseString(
    s"""
       |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
       |akka.remote.netty.tcp.hostname = ${host}
       |akk.remote.netty.tcp.port = ${port}
       |""".stripMargin
  )
  //词频统计命令
  val instruction =
    "val textFile = spark.sparkContext.textFile(\"hdfs://HDFS80933/data/test/words\");" +
      "val counts = textFile.flatMap(line=>line.split(\" \")).map(word=>(word,1)).reduceByKey(_+_);" +
      "val result = counts.collect();" +
      "result.foreach(println(_))"
  //"counts.repartition(1).saveAsTextFile(\"hdfs://HDFS80933/data/test/words_count\")"

  val comandMode = CommandMode.CODE
  //系统预留的参数变量
  val variables = "[]"
  val token = ""

  //创建客户端的actorsystem系统
  val clientSystem = ActorSystem("client", conf)

  /**
   * 服务端配置信息
   */
  val ip = InetAddress.getLocalHost().getHostAddress
  val actorAddr = s"${ip}:3000"
  val actorName = "actor_1"
  val selection = clientSystem.actorSelection("akka.tcp://system@" + actorAddr + "/user/" + actorName)
  Patterns.ask(selection, Instruction(comandMode, instruction, variables, token), new Timeout(Duration.create(10, "s")))

}
