mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case x => old(x)
  }
}

target in assembly := file("target/")

jarName in assembly := {
  name.value + ".jar"
}
