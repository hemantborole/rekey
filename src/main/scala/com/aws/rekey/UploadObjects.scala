package com.aws.rekey

import com.amazonaws.auth._
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import com.amazonaws.regions._

import java.io.ByteArrayInputStream
import java.io.InputStream
import javax.crypto.spec.SecretKeySpec

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Load initial objects with encryption.
  */
object UploadObjects {

  implicit val formats = DefaultFormats

  def convertToEncrypted( bucket : String,
                          prefix : String,
                          kmsArnId : String,
                          label : String,
                          version : String ) : Unit = {

    val kmsKeyId = kmsArnId.split("/").last.replaceAll("-","")
    val amazonS3client = new AmazonS3Client(
                          new DefaultAWSCredentialsProviderChain)

    val objStream = org.apache.commons.io.IOUtils.toByteArray(
                    amazonS3client.getObject(bucket, prefix).getObjectContent)

    val keySpec = new SecretKeySpec( kmsKeyId.getBytes, "AES")
    val materialProvider = new SimpleMaterialProvider()
            .withLatest(new EncryptionMaterials(keySpec))
    val encryptionClient = new AmazonS3EncryptionClient(
            new InstanceProfileCredentialsProvider().getCredentials,
            materialProvider,
            new CryptoConfiguration()
                  .withStorageMode(CryptoStorageMode.ObjectMetadata)
        )
    encryptionClient.setRegion(Regions.getCurrentRegion)
    val metadata = new ObjectMetadata
    metadata.setContentLength(objStream.length)
    metadata.setContentType("application/octet-stream")
    metadata.addUserMetadata("dpKekLabel",label)
    metadata.addUserMetadata("dpKekVersion",version)
    //metadata.setContentMD5(md5)

    val putRequest = new PutObjectRequest(bucket,
                            "encrypted/" + prefix,
                            new ByteArrayInputStream(objStream),
                            metadata)
                   .withSSEAwsKeyManagementParams(
                          new SSEAwsKeyManagementParams(kmsArnId))
    encryptionClient.putObject(putRequest)
  }

  def extractParams( bcParams : Broadcast[JValue] ) : (String, String, String, String, String) = {
    val params = bcParams.value
    ( (params \ "loadBucket").extract[String],
      (params \ "prefix").extract[String],
      (params \ "keyArn").extract[String],
      (params \ "dpKekLabel").extract[String],
      (params \ "dpKekVersion").extract[String]
    )
  }

  def main( args : Array[String] ) : Unit = {
    if ( args.length != 1 ) {
      println("Usage: spark-submit --class com.aws.rekey.RotateKeys " + 
                      "--master yarn-cluster rotate-keys.jar <s3-laoder-file>" )
    } else {

      val sc = new SparkContext(new SparkConf().setAppName("Upload Objects"))
      val uploadDataFile = args(0).split("/")
      val s3Object = S3Utils.s3Reader
                      .getObject(uploadDataFile(0), uploadDataFile(1))

      val contents: InputStream = s3Object.getObjectContent
      val lines = Source.fromInputStream(contents).getLines()

      // Process one json object i.e. one bucket at a time from the mapping file
      // pseudo way to prevent overwhelming the aws api calls.
      lines.foreach { line =>
        val jParams = parse(line)
        val bucket = ( jParams \ "loadBucket" ).extract[String]
        val prefix = ( jParams \ "prefix" ).extract[String]
        val exclude = ( jParams \ "exclude" ).extract[String]
        val bcArgs = sc.broadcast(jParams)

        val uploadList = S3Utils.s3List(bucket,prefix)
                          .filter( m => (!m.getKey.startsWith(exclude) ) )

        val loaded = sc.parallelize(uploadList)
        .map( item => {
          val params = extractParams(bcArgs)
          val itemBucket = params._1
          val itemPrefix = item.getKey
          val kmsArnId = params._3
          val label = params._4
          val version = params._5
          println("Processing item: " + itemBucket + "/" + itemPrefix +
                                      " using params " + params)
          convertToEncrypted( itemBucket, itemPrefix, kmsArnId, label, version)
        })
        loaded.count
      }
      sc.stop
    }
  }

}
