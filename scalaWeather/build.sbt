name := "scalaWeather"

version := "1.0"

scalaVersion :="2.10.3"


// | sbt can take a load time checking dependencies. This avoids re-checking the dependencies.
// | Comment this line if error "Skipping update requested, but update has not previously run successfully."
// skip in update := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-hive" % "1.6.0"
) 

mainClass in (Compile, packageBin) := Some("com.scalaWeather.MaxTemperature")