import sbt._

object Resolvers {

  val defaultResoulvers = Seq(
    DefaultMavenRepository,
    "Globo Artifactory" at "https://artifactory.globoi.com/artifactory/libs-release",
    "Globo Plugin" at "https://artifactory.globoi.com/artifactory/plugins-release",
    "Globo Ivy" at "https://artifactory.globoi.com/artifactory/remote-ivy-repos"
  )

}
