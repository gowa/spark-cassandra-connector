package com.datastax.spark.connector.ccm.mode

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicBoolean
import com.datastax.oss.driver.api.core.Version
import com.datastax.spark.connector.ccm.CcmConfig
import com.datastax.spark.connector.ccm.CcmConfig.V6_8_5
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try
import scala.util.control.NonFatal

private[mode] trait DefaultExecutor extends ClusterModeExecutor {
  private val logger: Logger = LoggerFactory.getLogger(classOf[StandardModeExecutor])

  private val created = new AtomicBoolean()

  override def start(nodeNo: Int): Unit = {
    val formattedJvmArgs = config.jvmArgs.map(arg => s" --jvm_arg=$arg").mkString(" ")
    try {
      execute(s"node$nodeNo", "start", formattedJvmArgs + "--wait-for-binary-proto")
    } catch {
      case NonFatal(e) =>
        val linesCount = 1000

        val startupErrors = s"${dir}/ccm_1/node${nodeNo}/logs/startup-stderr*"
        Try(logger.error(s"Start command failed, here is the last $linesCount lines of stderr: \n" +
          getLastLogLines(startupErrors, linesCount).mkString("\n")))

        val startupOut = s"${dir}/ccm_1/node${nodeNo}/logs/startup-stdout*"
        Try(logger.error(s"Start command failed, here is the last $linesCount lines of stdout: \n" +
          getLastLogLines(startupOut, linesCount).mkString("\n")))

        val debug = s"${dir}/ccm_1/node${nodeNo}/logs/debug.log"
        Try(logger.error(s"Start command failed, here is the last $linesCount lines of debug log \n" +
          getLastLogLines(debug, linesCount).mkString("\n")))

        val systemlog = s"${dir}/ccm_1/node${nodeNo}/logs/system.log"
        Try(logger.error(s"Start command failed, here is the last $linesCount lines of debug log \n" +
          getLastLogLines(systemlog, linesCount).mkString("\n")))

        throw e
    }
  }

  private def eventually[T](hint: String = "", f: =>T ): T = {
    val start = System.currentTimeMillis()
    val timeoutInSeconds = 20
    val intervalInMs = 500
    val end = start + timeoutInSeconds * 1000

    while ( System.currentTimeMillis() < end) {
      try {
        return f
      } catch { case e: Throwable =>
        logger.warn(s"Tried to execute code, will retry : ${e.getMessage}")
        Thread.sleep(intervalInMs)
      }
    }
    throw new IllegalStateException(s"Unable to complete function in $timeoutInSeconds: $hint")
  }

  /**
    * Remove this once C* 4.0.0 is released.
    *
    * This is a workaround that allows running it:test against 4.0.0-betaX and 4.0.0-rcX. These C* versions are
    * published as 4.0-betaX and 4.0-rcX, lack of patch version breaks versioning convention used in integration tests.
    */
  private def adjustCassandraBetaVersion(version: String): String = {
    val beta = "4.0.0-beta(\\d+)".r
    val rc = "4.0.0-rc(\\d+)".r
    version match {
      case beta(betaNo) => s"4.0-beta$betaNo"
      case rc(rcNo) => s"4.0-rc$rcNo"
      case other => other
    }
  }

  override def create(clusterName: String): Unit = {
    if (created.compareAndSet(false, true)) {
      val options = config.installDirectory
        .map(dir => config.createOptions :+ s"--install-dir=${new File(dir).getAbsolutePath}")
        .orElse(config.installBranch.map(branch => config.createOptions :+ s"-v git:${branch.trim().replaceAll("\"", "")}"))
        .getOrElse(config.createOptions :+ s"-v ${adjustCassandraBetaVersion(config.version.toString)}")

      val dseFlag = if (config.dseEnabled) Some("--dse") else None

      val createArgs = Seq("create", clusterName, "-i", config.ipPrefix, (options ++ dseFlag).mkString(" "))

      // Check installed Directory
      val repositoryDir = Paths.get(
        sys.props.get("user.home").get,
        ".ccm",
        "repository",
        adjustCassandraBetaVersion(config.getDseVersion.getOrElse(config.getCassandraVersion).toString))

      if (Files.exists(repositoryDir)) {
        logger.info(s"Found cached repository dir: $repositoryDir")
        logger.info("Checking for appropriate bin dir")
        eventually(f = Files.exists(repositoryDir.resolve("bin")))
      }

      try {
        execute(createArgs: _*)
      } catch {
        case NonFatal(e) =>
          Try(logger.error("Create command failed, here is the last 500 lines of ccm repository log: \n" +
              getLastRepositoryLogLines(500).mkString("\n")))
          throw e
      }

      eventually("Checking to make sure repository was correctly expanded", {
        Files.exists(repositoryDir.resolve("bin"))
      })

      config.nodes.foreach { i =>
        val node = s"node$i"
        val addArgs = Seq ("add",
          "-s", // every node is a seed node
          "-b", // autobootstrap is enabled
          "-j", config.jmxPort(i).toString,
          "-i", config.ipOfNode(i),
          "--remote-debug-port=0") ++
          dseFlag :+
          node

        execute(addArgs: _*)

        if (config.dseEnabled && config.getDseVersion.exists(_.compareTo(V6_8_5) >= 0)) {
          execute(node, "updateconf", s"metadata_directory:${dir.toFile.getAbsolutePath}/metadata$i")
        }
      }

      config.cassandraConfiguration.foreach { case (key, value) =>
        execute("updateconf", s"$key:$value")
      }
      if (config.getCassandraVersion.compareTo(Version.V2_2_0) >= 0) {
        execute("updateconf", "enable_user_defined_functions:true")
      }
      if (config.dseEnabled) {
        config.dseConfiguration.foreach { case (key, value) =>
          execute("updatedseconf", s"$key:$value")
        }
        config.dseRawYaml.foreach { yaml =>
          executeUnsanitized("updatedseconf", "-y", yaml)
        }
        if (config.dseWorkloads.nonEmpty) {
          execute("setworkload", config.dseWorkloads.mkString(","))
        }
      } else {
        // C* 4.0.0 has materialized views disabled by default
        if (config.getCassandraVersion.compareTo(Version.parse("4.0-beta1")) >= 0) {
          execute("updateconf", "enable_materialized_views:true")
        }
      }
    }
  }
}

private[ccm] class StandardModeExecutor(val config: CcmConfig) extends DefaultExecutor {
  override val dir: Path = Files.createTempDirectory("ccm")
  // remove config directory on shutdown
  dir.toFile.deleteOnExit()
  // remove db artifacts
  override def remove(): Unit = {
    execute("remove")
  }
}
