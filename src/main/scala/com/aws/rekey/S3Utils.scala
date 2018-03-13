package com.aws.rekey

import com.amazonaws.auth._
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import com.amazonaws.regions._

import java.io.ByteArrayInputStream
import java.nio.charset.MalformedInputException
import javax.crypto.spec.SecretKeySpec

import org.apache.commons.codec.binary._
import org.apache.spark.broadcast.Broadcast

import org.json4s._
import org.json4s.native.JsonMethods._


import scala.io.Source

/**
  * Utils object to perform most s3 object related operations like
  * listing objects recursive in a bucket,
  * decrypting/encrypting s3 object
  */
object S3Utils {

  implicit val formats = DefaultFormats
  val s3Reader = new AmazonS3Client(
                    new DefaultAWSCredentialsProviderChain())

  /**
    * read a s3 object's content and return the contents of s3 object.
    * Just used for secrets json, so ok on size.
    * @param bucket
    * @param prefix prefix must point to leaf node object, cannot recurse.
    * @return Array[Byte] byte array of s3 objects content
    */
  def s3ObjectString( bucket : String, prefix : String ) : String = {
    val contents = new StringBuilder

    val contentStream = s3Reader.getObject(bucket, prefix)
                                .getObjectContent
    
    new String(org.apache.commons.io.IOUtils.toByteArray(contentStream))
  }


  /**
    * recursively list latest version of each object in a s3 bucket,
    * starting at the prefix, if passed.
    * @param bucket the bucket to list objects from
    * @param prefix optional parameter to limit the list of objects
    * @return List[S3ObjectSummary] list of summary object of s3 objects.
    */
  def s3List(bucket :String, prefix : String )
          : java.util.List[S3ObjectSummary] = {
    val s3Client = new AmazonS3Client(
                      new DefaultAWSCredentialsProviderChain)
    var current = s3Client.listObjects(bucket, prefix)
    val keyList = current.getObjectSummaries
    current = s3Client.listNextBatchOfObjects(current)
    while( current.isTruncated ) {
      keyList.addAll(current.getObjectSummaries)
      current = s3Client.listNextBatchOfObjects(current)
    }
    keyList.addAll(current.getObjectSummaries)
    keyList
  }


  /**
    * decrypt s3 object using the secret key 
    * @param objectSummary s3 file summary reference to decrypt
    * @param kmsCmkOld kms key obtained from objects userMetadata
    * @return (String, String, Array[Byte]) tuple containing
    * md5 hash of contents of the s3 object
    * kmsKeyId kms key id for kms encryption on server side for the s3 object.
    * data bytes in the decrypted s3 object
    */
  def decryptS3Object(  objectSummary : S3ObjectSummary,
                        kmsCmkOld : Array[Byte],
                        label : String,
                        version : String )
            : (String, String, Array[Byte]) = {
    val cmkDecoded = Base64.decodeBase64(kmsCmkOld)
    val keySpec = new SecretKeySpec( cmkDecoded, "AES")

    val materialProvider = new SimpleMaterialProvider()
                          .withLatest(new EncryptionMaterials(keySpec)
                                      .addDescription("dpKeKLabel",label)
                                      .addDescription("dpKeKVersion",version)
                                      )

    val encryptionClient = new AmazonS3EncryptionClient(
            new InstanceProfileCredentialsProvider().getCredentials,
            materialProvider,
            new CryptoConfiguration()
                  .withStorageMode(CryptoStorageMode.ObjectMetadata)
        )
    encryptionClient.setRegion(Regions.getCurrentRegion)
    val contentStream = encryptionClient.getObject(
                                  objectSummary.getBucketName, objectSummary.getKey)
                                .getObjectContent

    val data = org.apache.commons.io.IOUtils.toByteArray(contentStream)
    val s3Object = encryptionClient.getObject(
                          objectSummary.getBucketName, objectSummary.getKey )
    val md5 = s3Object.getObjectMetadata.getContentMD5
    val kmsKeyId = s3Object.getObjectMetadata.getSSEAwsKmsKeyId
    (md5, kmsKeyId, data)
  }

  
    * re-encrypt the object that was decrypted by decryptS3Object, using a new
    * encryption key. The new encryption key is obtained from the
    * mapping.json's label/version which is further used to extract secret from
    * the secrets json.
    * @param s3Object a reference to the summary object of the s3 file that
    *                 needs re-encryption
    * @param md5 md5 hash of content of the s3 file
    * @param kmsKeyId kms key id for kms encryption on server side for the s3
    *                 object.
    * @param content bytes in the s3 object
    * @param labelNew the new label to lookup in the secret json
    * @param versionNew version of the labelNew
    * @param bcKeks broadcast variable contains secrets
    */
  def encryptS3Object(  s3Object : S3ObjectSummary,
                        md5 : String,
                        kmsKeyId : String,
                        content : Array[Byte],
                        labelNew : String,
                        versionNew : String,
                        bcKeks : Broadcast[JValue] ) : Unit = {
    val newKey = Base64.decodeBase64(
                  (bcKeks.value \ labelNew \ versionNew)
                  .extract[String].getBytes)
    val newKeySpec = new SecretKeySpec(newKey, "AES")
    val materialProvider = new SimpleMaterialProvider()
            .withLatest(new EncryptionMaterials(newKeySpec)
                    .addDescription("dpKeKLabel",labelNew)
                    .addDescription("dpKeKVersion",versionNew)
             )
    val encryptionClient = new AmazonS3EncryptionClient(
            new InstanceProfileCredentialsProvider().getCredentials,
            materialProvider,
            new CryptoConfiguration()
                  .withStorageMode(CryptoStorageMode.ObjectMetadata)
        )
    encryptionClient.setRegion(Regions.getCurrentRegion)
    val jsonMeta = s"""{"dpKeKLabel":"${labelNew}","dpKeKVersion":"${versionNew}"}""" 

    val metadata = new ObjectMetadata
    metadata.setContentLength(content.length)
    metadata.setContentType("application/octet-stream")
    metadata.setContentMD5(md5)
    metadata.addUserMetadata("x-amz-matdesc", jsonMeta)

    val putRequest = new PutObjectRequest(s3Object.getBucketName,
                            "re-encrypted/" + s3Object.getKey,
                            new ByteArrayInputStream(content),
                            metadata)
                   .withSSEAwsKeyManagementParams(
                          new SSEAwsKeyManagementParams(kmsKeyId))
    encryptionClient.putObject(putRequest)

  }

}
