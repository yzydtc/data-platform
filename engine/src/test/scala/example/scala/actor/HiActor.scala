package example.scala.actor

import scala.actors.Actor

/**
 * 1.实现简单简单字符的通信
 * 2.发送case类型的消息
 * 3.actor与actor之间的通信
 * @param msg
 * @param targetActor
 */
case class sayHello(msg: String, targetActor: Actor)

class HiActor extends Actor {
  override def act(): Unit = {
    while (true) {
      receive{
        case "hello" => println("hello")
        case "Hi" => println("hi")
        case sayHello(msg,targetActor) => {
          println(msg)
          targetActor ! msg
        }
        case _ => println("发送异常")
      }
    }

  }
}

class SayHelloActor extends Actor {
  override def act(): Unit = {
    while (true) {
      receive {
        case "sayHello" => println("SayHelloActor:sayHello")
        case "sayHi" => println("SayHelloActor:sayHi")
        case _ => println("无法接收异常")
      }
    }
  }
}

object HiActor extends App {
  val hiActor = new HiActor
  val sayHelloActor  = new SayHelloActor
  sayHelloActor.start()
  hiActor.start()
  hiActor ! "hello"
  hiActor ! sayHello("sayHi",sayHelloActor)
}