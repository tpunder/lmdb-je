
//
// For sbt-sonatype
//
organization := "com.eluvio"

publishMavenStyle := true

licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("eluvio", "lmdb-je", "Tim Underwood", "timunderwood@gmail.com"))

//
// For sbt-pgp
//
usePgpKeyHex("AB8A8ACD374B4E2FF823BA35553D700D8BD8EF54")

//
// For sbt-release
//
import ReleaseTransformations._

releaseCrossBuild := false // This is *not* a cross-built Scala project. It is Java only.
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommand("publishSigned"),
  releaseStepCommand("sonatypeBundleRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)
