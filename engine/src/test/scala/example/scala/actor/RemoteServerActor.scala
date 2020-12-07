package example.scala.actor

import scala.actors.remote.RemoteActor._
import scala.actors.Actor


class RemoteServerActor extends Actor{
  override def act(): Unit = {
    alive(9010)
    register('myName, this)
    while (true) {
      receive {
        case "hello" => println("hello")
        case _ => println("异常")
      }
    }
  }
}


object RemoteServerActor extends App {
  val remoteServerActor = new RemoteServerActor
  remoteServerActor.start()
}