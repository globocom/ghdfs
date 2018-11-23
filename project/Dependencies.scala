import sbt._
object Dependencies {

  val projectDependencies = Seq(
    "org.apache.hadoop" % "hadoop-client" % "2.8.3" excludeAll(
      ExclusionRule("javax.servlet"),
      ExclusionRule("javax.servlet.jsp"),
      ExclusionRule("org.mortbay.jetty"),
      ExclusionRule("io.netty")
    )
  )

  val testsDependencies = Seq(
    "org.scalatest" %% "scalatest" % "3.0.1",
    "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0"
  )

  val rootDependencies = projectDependencies ++ testsDependencies.map(_ % Test)
}
