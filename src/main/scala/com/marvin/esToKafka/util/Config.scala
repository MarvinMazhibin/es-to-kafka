package com.marvin.esToKafka.util

import java.util.Properties
import org.slf4j.LoggerFactory
哈哈哈新增一行
/*
* @author: marvin
* @software: IntelliJ IDEA
* @file: Config.scala
* @time: 2020-03-11 17:27
*/

object Config {


  /**
    * 加载配置文件
    */
  private val logger = LoggerFactory.getLogger(this.getClass)
  logger.info("当前加载资源路径:"+this.getClass.getResource("/").getPath)

  private val properties = new Properties()
  def load(fileName:String):Unit = properties.load(Config.getClass.getResourceAsStream("/"+fileName))

  def getString(key:String): String = properties.getProperty(key)
  def getInt(key:String):Int = properties.getProperty(key).toInt

  def main(args: Array[String]): Unit = {
    load("config.properties")
    println(getString("aa"))
  }

}
