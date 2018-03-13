package com.aws.rekey

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._

import java.io.InputStream

import org.apache.spark._
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * The main Controller object. Takes in a mapping s3 file as a argument.
  * The mapping file contains several lines of json objects.
  * The json object contains the following keys
  * bucket : The bucket whose objects need to be rotated.
  * dpKekLabel : label to identify the encryption key to use
  * dpKekVersion : version of the dpKekLabel
  * prefix : The prefix within the bucket to encrypt. Use empty string for all
  *
  * delegates the rotation process to the spark job.
  */
object RotateKeys {

  implicit val formats = DefaultFormats

  /**
    * Entry function for kicking off rotation job. Simply add this object
    * as a spark step to your main job and it will start execution from
    * this method.
    * @param args args passed on commnd line - s3 path to the mapping.json
    */
  def main(args: Array[String]) : Unit = {
    if ( args.length != 1 ) {
      println("Usage: spark-submit --class com.aws.rekey.RotateKeys " + 
                      "--master yarn-cluster rotate-keys.jar <s3-mapping-file>" )
    } else {

      val sc = new SparkContext(new SparkConf().setAppName("Key Rotator"))
      
      //Broadcast the secrets json into the spark env.
      val bcKeks = SafenetSecrets.broadcastKeks("iss-key-rotation-tagged-us-west-2",
                                    "secrets-kek-v3.json",sc)

      val s3Reader = new AmazonS3Client(
                        new DefaultAWSCredentialsProviderChain())

      val mappingPath = args(0).split("/")
      val s3Object = s3Reader.getObject(mappingPath(0), mappingPath(1))

      val contents: InputStream = s3Object.getObjectContent
      val lines = Source.fromInputStream(contents).getLines()

      // Process one json object i.e. one bucket at a time from the mapping file
      // pseudo way to prevent overwhelming the aws api calls.
      lines.foreach { line =>
        val mapping = parse(line)
        val bucket = (mapping \ "bucket").extract[String]
        val newLabel = (mapping \ "dpKekLabel").extract[String]
        val newVersion = (mapping \ "dpKekVersion").extract[String]
        val prefix = (mapping \ "prefix").extract[String]
        val exclude = (mapping \ "exclude").extract[String]
        Rotator.rotate( sc, bucket, bcKeks, newLabel, newVersion, prefix, exclude)
      }
      s3Reader.shutdown
      sc.stop

    }
  }
}
