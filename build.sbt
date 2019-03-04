import com.typesafe.sbt.SbtNativePackager._
import NativePackagerKeys._

FMPublic

name := "lmdb-je"

organization := "com.eluvio"

description := "LMDB Java Edition"

homepage := Some(url("https://github.com/eluvio/lmdb-je"))

// For native packager
enablePlugins(JavaAppPackaging)

//javacOptions in Compile ++= Seq("-source", "1.8", "-target", "1.8")

javacOptions in doc ++= Seq("-windowtitle", "lmdb-je", "-sourcepath", "/Users/tim/java-src", "-tag", "implSpec:x", "-tag", "implNote:x", "-tag", "jls:x", "-linkoffline", "http://docs.oracle.com/javase/8/docs/api/", "http://docs.oracle.com/javase/8/docs/api/")

// Fork for tests so we can enable assertions
fork in Test := true

// Enable assertions
javaOptions in Test += "-ea"

autoScalaLibrary := false

crossPaths := false

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")

// Java Dependencies
libraryDependencies ++= Seq(
  "com.github.jnr" % "jnr-ffi" % "2.0.9",
  "com.novocode" % "junit-interface" % "0.11" % "test", // For running Junit tests from SBT
  "junit" % "junit" % "4.12" % "test"
)
