val commonName = "db-async-common"
val commonVersion = "0.2.22-SNAPSHOT"
val postgresqlName = "postgresql-async"
val mysqlName = "mysql-async"
val projectScalaVersion = "2.13.5"
val specs2Version = "3.8.6"
val specs2Dependency = "org.specs2" %% "specs2-core" % specs2Version % "test"
val specs2JunitDependency = "org.specs2" %% "specs2-junit" % specs2Version % "test"
val specs2MockDependency = "org.specs2" %% "specs2-mock" % specs2Version % "test"
val logbackDependency = "ch.qos.logback" % "logback-classic" % "1.1.8" % "test"
val baseSettings = Seq(
  scalacOptions :=
    Opts.compile.encoding("UTF8")
      :+ Opts.compile.deprecation
      :+ Opts.compile.unchecked
      :+ "-feature"
  ,
  testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "sequential"),
  scalacOptions in doc := Seq("-doc-external-doc:scala=http://www.scala-lang.org/archives/downloads/distrib/files/nightly/docs/library/"),
  crossScalaVersions := Seq(projectScalaVersion),
  javacOptions := Seq("-source", "11", "-target", "11", "-encoding", "UTF8"),
  organization := "io.github.mavenrain",
  version := commonVersion,
  parallelExecution := false,
  publishArtifact in Test := false,
  publishMavenStyle := true,
  pomIncludeRepository := {
    _ => false
  },
  publishTo := version {
    v: String =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }.value,
  pomExtra := (
    <url>https://github.com/mauricio/postgresql-async</url>
      <licenses>
        <license>
          <name>APACHE-2.0</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:mauricio/postgresql-netty.git</url>
        <connection>scm:git:git@github.com:mauricio/postgresql-netty.git</connection>
      </scm>
      <developers>
        <developer>
          <id>mauricio</id>
          <name>Onyekachukwu Obi</name>
          <url>https://github.com/mauricio</url>
        </developer>
      </developers>
    )
)
val commonDependencies = Seq(
  "org.slf4j" % "slf4j-api" % "1.7.22",
  "joda-time" % "joda-time" % "2.9.7",
  "org.joda" % "joda-convert" % "1.8.1",
  "io.netty" % "netty-all" % "4.1.6.Final",
  "org.javassist" % "javassist" % "3.21.0-GA",
  specs2Dependency,
  specs2JunitDependency,
  specs2MockDependency,
  logbackDependency
)
val implementationDependencies = Seq(
  specs2Dependency,
  logbackDependency
)
lazy val root =
  (project in file("."))
    .settings(
      baseSettings ++ Seq(
        publish := (),
        publishLocal := (),
        publishArtifact := false
      )
    )
    .aggregate(common, postgresql, mysql)
lazy val common =
  (project in file(commonName))
    .settings(
      baseSettings ++ Seq(
        name := commonName,
        libraryDependencies ++= commonDependencies
      )
    )
lazy val postgresql =
  (project in file(postgresqlName))
    .settings(
      baseSettings ++ Seq(
        name := postgresqlName,
        libraryDependencies ++= implementationDependencies
      )
    )
    .dependsOn(common)
lazy val mysql =
  (project in file(mysqlName))
    .settings(
      baseSettings ++ Seq(
        name := mysqlName,
        libraryDependencies ++= implementationDependencies
      )
    )
    .dependsOn(common)
