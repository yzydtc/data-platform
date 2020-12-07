package example.scala.actor

import scala.actors.Actor

class HelloActor extends Actor {

  //类似于java线程中的run，用于线程启动
  override def act(): Unit = {
    while (true) {
      receive {//偏函数
        case name: String => println("Hello," + name)
        case _ => println("无法处理信息")
      }
    }
  }
}

object HelloActor {
  def main(args: Array[String]): Unit = {
    val helloActor = new HelloActor
    helloActor.start()
    helloActor ! "tom"
  }
}
