/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package tech.mlsql.test

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.TimeZone
import java.util.regex.Pattern

import com.google.common.io.Files
import org.apache.spark.{TaskContext, WowRowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, Row}
import os.CommandResult
import tech.mlsql.arrow.python.ispark.SparkContextImp
import tech.mlsql.arrow.python.runner.SparkSocketRunner
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.net.NetTool
import tech.mlsql.common.utils.network.NetUtils
import tech.mlsql.common.utils.shell.ShellCommand
import tech.mlsql.test.RayEnv.ServerInfo

class RayEnv extends Logging with Serializable {

  @transient var rayAddress: String = _
  @transient var rayOptions: Map[String, String] = Map.empty

  @transient var dataServers: Seq[ServerInfo] = _

  def startRay(envName: String): Unit = {

    val rayVersionRes = runWithProfile(envName, "ray --version")
    val rayVersion = rayVersionRes.out.lines.toList.head.split("version").last.trim

    val result = if (rayVersion >= "1.0.0" || rayVersion == "0.8.7") {
      runWithProfile(envName, "ray start --head")
    } else {
      runWithProfile(envName, "ray start --head --include-webui")
    }
    val initRegex = Pattern.compile(".*ray.init\\((.*)\\).*")
    val errResultLines = result.err.lines.toList
    val outResultLines = result.out.lines.toList

    errResultLines.foreach(logInfo(_))
    outResultLines.foreach(logInfo(_))

    (errResultLines ++ outResultLines).foreach(line => {
      val matcher = initRegex.matcher(line)
      if (matcher.matches()) {
        val params = matcher.group(1)


        val options = params.split(",")
          .filter(_.contains("=")).map(param => {
          val words = param.trim.split("=")
          (words.head, words(1).substring(1, words(1).length - 1))
        }).toMap


        if (rayAddress == null) {
          rayAddress = options.getOrElse("address", options.getOrElse("redis_address", null))
          rayOptions = options - "address" - "redis_address"
          logInfo(s"Start Ray:${rayAddress}")
        }
      }
    })
    
    if (rayVersion >= "1.0.0") {
      val initRegex2 = Pattern.compile(".*--address='(.*?)'.*")
      (errResultLines ++ outResultLines).foreach(line => {
        val matcher = initRegex2.matcher(line)
        if (matcher.matches()) {
          val params = matcher.group(1)

          val host = params.split(":").head
          rayAddress = host + ":10001"
          rayOptions = rayOptions
          new Thread(new Runnable {
            override def run(): Unit = {
              runWithProfile(envName,
                s"""
                   |python -m ray.util.client.server --host ${host} --port 10001
                   |""".stripMargin)
            }
          }).start()
          Thread.sleep(3000)
          logInfo(s"Start Ray:${rayAddress}")
        }
      })
    }


    if (rayAddress == null) {
      throw new RuntimeException("Fail to start ray")
    }


  }

  def stopRay(envName: String): Unit = {
    val result = runWithProfile(envName, "ray stop")
    result
  }

  def startDataServer(df: DataFrame): Unit = {
    val dataSchema = df.schema
    dataServers = df.repartition(1).rdd.mapPartitions(iter => {
      val socketServer = new SparkSocketRunner("serve-runner-for-ut", NetTool.localHostName(), TimeZone.getDefault.getID)
      val commonTaskContext = new SparkContextImp(TaskContext.get(), null)
      val rab = WowRowEncoder.fromRow(dataSchema) //RowEncoder.apply(dataSchema).resolveAndBind()
      val newIter = iter.map(row => {
        rab(row)
      })
      val Array(_server, _host: String, _port: Int) = socketServer.serveToStreamWithArrow(newIter, dataSchema, 10, commonTaskContext)
      Seq(ServerInfo(_host, _port, TimeZone.getDefault.getID)).iterator
    }).collect().toSeq
  }

  def collectResult(rdd: RDD[Row]): RDD[InternalRow] = {
    rdd.flatMap { row =>
      val socketRunner = new SparkSocketRunner("read-runner-for-ut", NetUtils.getHost, TimeZone.getDefault.getID)
      val commonTaskContext = new SparkContextImp(TaskContext.get(), null)
      val pythonWorkerHost = row.getAs[String]("host")
      val pythonWorkerPort = row.getAs[Long]("port").toInt
      logInfo(s" Ray On Data Mode: connect python worker[${pythonWorkerHost}:${pythonWorkerPort}] ")
      val iter = socketRunner.readFromStreamWithArrow(pythonWorkerHost, pythonWorkerPort, commonTaskContext)
      iter.map(f => f.copy())
    }
  }


  private def runWithProfile(envName: String, command: String): CommandResult = {
    val tmpShellFile = File.createTempFile("shell", ".sh")
    val setupEnv = if (envName.trim.startsWith("conda") || envName.trim.startsWith("source")) envName else s"""conda activate ${envName}"""
    try {
      Files.write(
        s"""
           |#!/bin/bash
           |export LC_ALL=en_US.utf-8
           |export LANG=en_US.utf-8
           |
           |# source ~/.bash_profile
           |${setupEnv}
           |${command}
           |""".stripMargin, tmpShellFile, StandardCharsets.UTF_8)
      val cmdResult = ShellCommand.execCmdV2("/bin/bash", tmpShellFile.getAbsolutePath)
      //      if (cmdResult.exitCode != 0) {
      //        throw new RuntimeException(s"run command failed ${cmdResult.toString()}")
      //      }
      cmdResult
    } finally {
      tmpShellFile.delete()
    }
  }

}

object RayEnv {

  case class ServerInfo(host: String, port: Long, timezone: String)

}