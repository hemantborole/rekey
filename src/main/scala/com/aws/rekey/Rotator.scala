package com.aws.rekey

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.JavaConversions._

/**
  * Orchestrates re-encryption of s3 objects. It is important that the object
  * be decrypted in this programs environment ( and not remote ) for security
  * concerns. This object starts the spark job and controls parallelization.
  */

object Rotator {

  implicit val formats = DefaultFormats

  /**
    * Extract dpKekLabel and dpKekVersion (just keys in hash) from s3 objects
    * userMetadata. These keys are used in the broadcast secret json to identify
    * the secret key, used to encrypt/decrypt the s3 object
    * @param item the s3 object whose userMetadata is extracted
    * @return (String, String) tuple of values of dpKekLabel and dpKekVersion
    */
  def getLabelVersion( item : S3ObjectSummary ) : ( String, String ) = {
    val objMeta = S3Utils.s3Reader.getObjectMetadata(
                item.getBucketName, item.getKey )
    val userMeta = objMeta.getUserMetadata
    try {
      val meta = parse(userMeta.get("x-amz-matdesc"))
      if( JNothing != (meta \ "dpKeKLabel") ) {
        ( (meta \ "dpKeKLabel").extract[String],
          (meta \ "dpKeKVersion").extract[String] ) 
      } else {
        ( (meta \ "dpKekLabel").extract[String],
          (meta \ "dpKekVersion").extract[String] )
      }
    } catch {
      case npe : NullPointerException => {
        println("Null Pointer for :" + item.getKey )
        (null,null)
      }
      case ex : Exception => {
        println("Other Pointer for :" + item.getKey + ":" + ex.getMessage )
        (null,null)
      }
    }
  }

  /**
    * for each object returned from s3list, is first decrypted. From each s3
    * object's userMetadata, dpKekLabel and dpKekVersion is used to identify
    * the key to be used for decryption. The object is then re-encrypted using
    * the key identified from the mapping.json by the newLabel and newVersion
    * from the broadcast secrets json
    *
    * @param sc SparkContect for starting transformations and action.
    * @param bucket Objects in this bucket are re-encrypted.
    * @param bcKeks braodcast variable to hold the secrets json
    * @param newLabel label identifier for newer key
    * @param newVersion version for the newLabel
    * @param prefix restrict re-encrytion only to objects in this prefix.
    */
  def rotate( sc : SparkContext,
              bucket : String,
              bcKeks : Broadcast[JValue],
              newLabel : String,
              newVersion : String,
              prefix : String,
              exclude : String ) : Unit = {

    val listToRotate = S3Utils.s3List(bucket, prefix)
                    .filter( m => !(m.getKey.startsWith(exclude) || m.getKey.startsWith("re-encrypted/") ) )
    listToRotate.foreach(println)

    val processed = sc.parallelize(listToRotate).map ( item => {
      val ( label, version ) = getLabelVersion( item )
      val encryptionKey = (bcKeks.value \ label \ version)
                          .extract[String].getBytes

      val newLabelKeyIndex = newLabel + "_" + label.split("_").last
      val (md5, kmsKeyId, content) = 
                S3Utils.decryptS3Object(item, encryptionKey, label, version)
      val encryptedObject = S3Utils.encryptS3Object( item,
                md5, kmsKeyId, content,
                newLabelKeyIndex, newVersion, bcKeks)
    })
    .count() // Count is just to trigger the action.
    //.collect().take(5).foreach( l => println(l) ) // for debugging
    println("Processed " + processed + " objects" )
  }

}

