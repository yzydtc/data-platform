package com.xyz.intepreter

import java.io.ByteArrayOutputStream

import com.xyz.intepreter.Interpreter.Interpreter

import scala.tools.nsc.interpreter.Results


abstract class AbstractSparkInterpreter extends Interpreter {

  import AbstractSparkInterpreter._

  //操作流,返回客户端响应
  protected val outputStream = new ByteArrayOutputStream()

  //方便底层实现，用于判断是否已经启动引擎(类似命令中执行spark-shell)
  protected def isSteated(): Boolean

  //中断引擎,命令执行的结果
  protected def interpreter(order: String): Results.Result

  //对spark-shell绑定初始化的变量值
  protected def bind(
                      name: String,
                      className: String,
                      value: Object,
                      modifier: List[String]
                    )

  //关闭spark-shell
  protected def close(): Unit
}

object AbstractSparkInterpreter {
  //反向肯定预测，匹配命令当中的换行符
  //命令：就是要执行的代码中的换行符
  private[intepreter] val Keep_NEWLINE_REGEX = """(?<=\n)""".r
}

