package com.aws.rekey

import com.amazonaws.auth._
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import com.amazonaws.regions._

import java.io.FileOutputStream
import javax.crypto.spec.SecretKeySpec

import org.apache.commons.codec.binary._

import scala.collection.JavaConversions._

object DownloadObject {
  def download( sourceBucket : String,
                sourcePrefix : String,
                kmsCmkOld : String,
                label : String,
                version : String,
                localDestFile : String ) : Unit = {
    val keySpec = new SecretKeySpec( 
                Base64.decodeBase64(kmsCmkOld.getBytes), "AES")

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
    val contentStream = encryptionClient.getObject( sourceBucket, sourcePrefix )
                                .getObjectContent

    val data = org.apache.commons.io.IOUtils.toByteArray(contentStream)

    val out = new FileOutputStream(localDestFile)
    out.write(data)
    out.close
  }

  def main( args : Array[String] ) : Unit = {
    //Test
    download( args(0), args(1), args(2), args(3), args(4), args(5))
  }

}

