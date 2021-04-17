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

import com.google.common.io.Files
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row}
import os.CommandResult
import tech.mlsql.arrow.python.ispark.SparkContextImp
import tech.mlsql.arrow.python.runner.SparkSocketRunner
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.network.NetUtils
import tech.mlsql.common.utils.shell.ShellCommand
import tech.mlsql.test.RayEnv.ServerInfo

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.TimeZone
import java.util.regex.Pattern

class RayEnv extends Logging with Serializable {

  @transient var rayAddress: String = _
  @transient var rayOptions: Map[String, String] = Map.empty

  @transient var dataServers: Seq[ServerInfo] = _

  def startRay(): Unit = {
    val result = runWithProfile("ray start --head --num-cpus 2")
    val initRegex = Pattern.compile(".*ray.init\\((.*)\\).*")
    result.out.lines.foreach(line => {
      val matcher = initRegex.matcher(line)
      if (matcher.matches()) {
        val params = matcher.group(1)
        val options = params.split(",")
          .map(param => {
            val words = param.trim.split("=")
            (words.head, words(1).substring(1, words(1).length - 1))
          }).toMap
        rayAddress = options("address")
        rayOptions = options - "address"
      }
    })
  }

  def stopRay(): Unit = {
    val result = runWithProfile("ray stop")
    result
  }

  def startDataServer(df: DataFrame): Unit = {
    val dataSchema = df.schema
    dataServers = df.repartition(1).rdd.mapPartitions(iter => {
      val socketServer = new SparkSocketRunner("serve-runner-for-ut", NetUtils.getHost, TimeZone.getDefault.getID)
      val commonTaskContext = new SparkContextImp(TaskContext.get(), null)
      val rab = RowEncoder.apply(dataSchema).resolveAndBind()
      val newIter = iter.map(row => {
        rab.toRow(row)
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


  private def runWithProfile(command: String): CommandResult = {
    val tmpShellFile = File.createTempFile("shell", ".sh")
    try {
      Files.write(
        s"""
           |#!/bin/bash
           |export LC_ALL=en_US.utf-8
           |export LANG=en_US.utf-8
           |
           |source ~/.bash_profile
           |conda activate dev
           |${command}
           |""".stripMargin, tmpShellFile, StandardCharsets.UTF_8)
      val cmdResult = ShellCommand.execCmdV2("/bin/bash", tmpShellFile.getAbsolutePath)
      if (cmdResult.exitCode != 0) {
        throw new RuntimeException(s"run command failed ${cmdResult.toString()}")
      }
      cmdResult
    } finally {
      tmpShellFile.delete()
    }
  }

}

object RayEnv {

  case class ServerInfo(host: String, port: Long, timezone: String)

}