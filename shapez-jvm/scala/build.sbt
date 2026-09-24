ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "org.shapez"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val cargoTargetDir = settingKey[File]("Path to the cargo target/debug dir holding libshapez_jvm")

lazy val cargoBuild = taskKey[Unit]("Build the shapez-jvm Rust cdylib via cargo")

lazy val protoSource = settingKey[File]("Path to the shapez proto source directory")

lazy val root = (project in file("."))
  .settings(
    name := "shapez-jvm-scala",

    libraryDependencies ++= Seq(
      "net.java.dev.jna" % "jna" % "5.14.0",
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion,
      "org.scalameta" %% "munit" % "1.0.0" % Test
    ),

    // shapez-jvm/scala/   <- baseDirectory
    // shapez-jvm/         <- ..
    // shapez/             <- ../..  (cargo workspace root)
    // target/debug/       <- ../../target/debug
    // shapez/proto/       <- ../../shapez/proto
    cargoTargetDir := baseDirectory.value / ".." / ".." / "target" / "debug",
    protoSource := baseDirectory.value / ".." / ".." / "shapez" / "proto",

    // ScalaPB: compile shapez/proto/*.proto into Scala case classes under
    // package `shapez.report`. Generated sources land in target/scala-2.13/src_managed.
    // flatPackage drops the .report-from-filename sub-package so the
    // generated case classes live at `shapez.report.AnalysisReport`
    // (matching the proto package directly) instead of
    // `shapez.report.report.AnalysisReport`.
    Compile / PB.targets := Seq(
      scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value / "scalapb"
    ),
    Compile / PB.protoSources := Seq(protoSource.value),

    cargoBuild := {
      import scala.sys.process._
      val log = streams.value.log
      val workspaceRoot = baseDirectory.value / ".." / ".."
      log.info(s"[shapez-jvm] cargo build -p shapez-jvm (cwd=${workspaceRoot.getAbsolutePath})")
      val rc = Process(Seq("cargo", "build", "-p", "shapez-jvm"), workspaceRoot).!
      if (rc != 0) sys.error(s"cargo build -p shapez-jvm failed with exit code $rc")
    },

    Test / fork := true,
    Test / javaOptions := Seq(
      s"-Djava.library.path=${cargoTargetDir.value.getAbsolutePath}",
      s"-Djna.library.path=${cargoTargetDir.value.getAbsolutePath}"
    ),

    Test / test := (Test / test).dependsOn(cargoBuild).value,
    Test / testOnly := (Test / testOnly).dependsOn(cargoBuild).evaluated,
    Test / testQuick := (Test / testQuick).dependsOn(cargoBuild).evaluated
  )
