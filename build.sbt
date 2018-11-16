val compilerVersion = "2.11.11"

val buildSettings = Seq(
  name := "ghdfs",
  scalaVersion := compilerVersion,
  description := "ghdfs",
  organization := "com.globo.bigdata",
  organizationName := "Globo.com",
  homepage := None,
  resolvers := Resolvers.defaultResoulvers,
  libraryDependencies ++= Dependencies.rootDependencies,
  run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)).evaluated,
  publishTo := Some(
    if (isSnapshot.value) {
      Opts.resolver.sonatypeSnapshots
    }
    else {
      Opts.resolver.sonatypeStaging
    }
  ),
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
  publishMavenStyle := true
)

lazy val root = (project in file(".")).settings(buildSettings: _*)

scapegoatVersion in ThisBuild := "1.3.2"
scalaBinaryVersion in ThisBuild := "2.11"
scapegoatDisabledInspections := Seq("ExpressionAsStatement", "OptionGet")
