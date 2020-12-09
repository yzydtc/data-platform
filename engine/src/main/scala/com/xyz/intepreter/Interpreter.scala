package com.xyz.intepreter

import org.json4s.JsonAST.JObject
import org.apache.spark.SparkConf

/**
 * 解析引擎(顶层的解析任务状态管理)
 */
object Interpreter {

  //目的:把引擎的所有的响应统一管理，上层的抽象
  abstract class ExcuteResponse

  //执行成功
  case class ExcuteSuccess(Content: JObject) extends ExcuteResponse

  //执行失败
  case class ExcuteError(
                          //任务名称
                          excuteName: String,
                          //任务失败原因
                          excuteValue: String,
                          //失败的堆栈信息(类似于java中的异常信息)
                          trackback: Seq[String]
                        )

  //未完成任务 TODO
  case class ExcuteInComplete() extends ExcuteResponse

  //终止任务 TODO
  case class ExcuteAborted(message: String) extends ExcuteResponse

  /**
   * 对外提供引擎的方法
   */

  trait Interpreter {

    import Interpreter._

    /**
     * 对外提供启动方法
     *
     * @return
     */
    def start(): SparkConf

    /**
     * 对外提供关闭方法
     */
    def close(): Unit

    /**
     * 命令执行方法(代码/SQL),为了保证安全，包内可见
     *
     * @param order
     * @return
     */
    private[intepreter] def excute(order: String): ExcuteResponse
  }

}
