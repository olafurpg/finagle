package com.twitter.finagle.ssl

import strawman.collection.stringToStringOps
object Shell {
  def run(args: Array[String]): Unit = {
    val process = Runtime.getRuntime.exec(args)
    process.waitFor()
    require(process.exitValue == 0, "Failed to run command '%s'".format(args.mkString(" ")))
  }
}
