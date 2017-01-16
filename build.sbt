// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html


sparkVersion := "2.0.0"

scalaVersion := "2.11.7"

spName := "krapsh/kraps-server"

// Don't forget to set the version
version := s"0.1.9"

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

spShortDescription := "Experimental REST API to run Spark computation graphs"

spDescription := """This is the server part for the Kraps project. It provides a simple REST API to execute
                   |data pipelines with Spark in a language independent manner. It complements (on the JVM side) the
                   |Haskell bindings available in Krapsh.
                   |
                   |This project is only a technological preview. The API may change in the future.
                   |""".stripMargin

spHomepage := "https://github.com/krapsh/kraps-server"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

spAppendScalaVersion := true

// Add Spark components this package depends on, e.g, "mllib", ....
sparkComponents ++= Seq("sql")

// uncomment and change the value below to change the directory where your zip artifact will be created
// spDistDirectory := target.value

// add any Spark Package dependencies using spDependencies.
// e.g. spDependencies += "databricks/spark-avro:0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

// These versions are ancient, but they cross-compile around scala 2.10 and 2.11.
// Update them when dropping support for scala 2.10
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-api" % "2.1.2"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.9"

libraryDependencies ++= {
  val akkaV = "2.3.9"
  val sprayV = "1.3.3"
  Seq(
    "io.spray"            %%  "spray-can"     % sprayV,
    "io.spray"            %%  "spray-routing" % sprayV,
    "com.typesafe.akka"   %%  "akka-actor"    % akkaV
  )
}

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.2"

parallelExecution := false


scalacOptions in (Compile, doc) ++= Seq(
  "-groups",
  "-implicits",
  "-skip-packages", Seq("org.apache.spark").mkString(":"))

scalacOptions in (Test, doc) ++= Seq("-groups", "-implicits")

// This fixes a class loader problem with scala.Tuple2 class, scala-2.11, Spark 2.x
fork in Test := true


concurrentRestrictions in Global := Seq(
  Tags.limitAll(1))

autoAPIMappings := true
