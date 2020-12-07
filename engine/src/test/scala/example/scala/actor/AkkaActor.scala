package example.scala.actor

import akka.actor.{Actor, ActorSystem, Props}

trait Action {
  val message: String
  val time: Int
}

case class TurnOffLight(time: Int) extends Action {
  val message: String = "关灯"
}

case class TurnOnLight(time: Int) extends Action {
  val message: String = "开灯"
}


class AkkaActor extends Actor {
  override def receive: Receive = {
    case t: TurnOffLight => {
      println(s"${t.message} : ${t.time}")
    }
    case b: TurnOnLight => {
      println(s"${b.message} : ${b.time}")
    }
    case _ => {
      println("无法处理信息")
    }
  }
}

object AkkaActor extends App {
  val actorSystem = ActorSystem("xiaoAiStudent")
  val akkaActor = actorSystem.actorOf(Props(new AkkaActor), "akkaActor")
  akkaActor ! TurnOnLight(1)
  akkaActor ! TurnOnLight(2)
  akkaActor ! "exception"
}
