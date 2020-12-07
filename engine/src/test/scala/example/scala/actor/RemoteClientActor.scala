package example.scala.actor

import scala.actors.remote.RemoteActor._
import scala.actors.remote.Node



object RemoteClientActor extends App {
  val remoteClientActor = select(Node("127.0.0.1", 9010), 'myName)
  remoteClientActor ! "hello"
}