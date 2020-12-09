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
    //val tpmArgs = Array("-engine.zkServers","node01:2181")
    val tpmArgs2 = Array("-engine.tag","tag_1","tag_2")
    parseArgs(tpmArgs2)

  }
}
