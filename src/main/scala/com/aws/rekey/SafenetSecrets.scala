package com.aws.rekey

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast

import org.json4s._
import org.json4s.native.JsonMethods._

/**
  * A simple secrets object to broadcast secrets into the spark env.
  */
object SafenetSecrets {

  implicit val formats = DefaultFormats

  /** Extract JSON keks from secret bucket and broadcast the json
    * @param secretsBucket bucket name where safenet keys are uploaded
    * @param secretsFile prefix containing the encoded json.
    * @param sc Spark context to broadcast to.
    * @return Broadcast[JValue] broadcast variable.
    */
  def broadcastKeks(  secretsBucket : String,
                      secretsFile : String,
                      sc : SparkContext ) : Broadcast[JValue] = {
    val kekContent = S3Utils.s3ObjectString(secretsBucket, secretsFile)
    println( "Adding broadcast content:" + parse(kekContent))
    sc.broadcast(parse(kekContent))
  }

  /**
    * parse secrets json to extract the secret key using the label and version
    * @param bc broadcast variable
    * @param label label for key mapping
    * @param version specific version for the label.
    * @return Array[Byte] byte array of the secret key
    */
  def getAppEncryptionKey( bc : Broadcast[JValue],
                           label :String,
                           version :String) : Array[Byte] = {
    val secretsJson = bc.value
    (secretsJson \ label \ version).extract[String].getBytes
  }
}
