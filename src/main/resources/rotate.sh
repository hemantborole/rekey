#!/bin/bash

## Create a emr cluster, using a cli.
## Add a spark job as a step process to execute the rotation.
## TODO configure spark cores/instances/memory based on workload. edit/hard-code until then.
## Args: s3 path to a mappings.json file. The file name does not really matter.

set -x

dir=`dirname $0`

log() {
  if [ $# -lt 1 ]; then
    echo "`date`: WARNING. log function misused."
  fi
  if [[ $2 != "0" ]]; then
    echo "`date` $1: exit code $2"
    exit $2
  fi
  echo "`date` $1: status code $2"
}


if [ $# -ne 1 ]; then
  log "Usage: $0 <local-jar-location-to-s3>" 1
fi

### AWS ENV Settings.
assets_bucket=key-rotation-emr-tagged-us-west-2
log_bucket=${assets_bucket}/logs
MAPPING=${assets_bucket}/mapping.json


JAR_PATH=$1
jarfile=`basename $JAR_PATH`
aws s3 cp data/mapping.json s3://${assets_bucket}/mapping.json
aws s3 cp $JAR_PATH s3://${assets_bucket}/${jarfile}
log "AWS S3 copy :" $?


### EMR PARAMS
inst_type=m3.2xlarge
inst_count=18
region='us-west-2'

### SPARK PARAMS
instances=7
memory=3g
cores=4


json="[{\"InstanceCount\":1,\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"${inst_type}\",\"Name\":\"Master instance group - 1\"},{\"InstanceCount\":${inst_count},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${inst_type}\",\"Name\":\"Core instance group - 2\"}] "
steps="[{\"Args\":[\"spark-submit\",\"--class\",\"com.aws.rekey.RotateKeys\",\"--deploy-mode\",\"cluster\",\"--conf\",\"spark.executor.instances=${instances}\",\"--conf\",\"spark.executor.memory=${memory}\",\"--conf\",\"spark.executor.cores=${cores}\",\"s3://${assets_bucket}/${jarfile}\",\"${MAPPING}\"],\"Type\":\"CUSTOM_JAR\",\"ActionOnFailure\":\"CONTINUE\",\"Jar\":\"command-runner.jar\",\"Properties\":\"\",\"Name\":\"Spark application\"}]"
echo ${steps}

## Cluster creation command. edit, only if you know what you are doing :)
aws emr create-cluster \
  --applications Name=Hadoop Name=Spark \
  --service-role target/emr-service \
  --enable-debugging \
  --ec2-attributes '{"InstanceProfile":"emr-ec2-key-rotation","SubnetId":"subnet-418be636","KeyName":"hborole.rekey"}' \
  --release-label emr-5.0.0 \
  --log-uri "s3://${assets_bucket}/logs/" \
  --name 'Rotate Keys - Re-encryption' \
  --instance-groups "$json" \
  --region ${region} \
  --steps "$steps" \
  --configurations file://${dir}/max_resource.json
  #--auto-terminate \

status=$?

if [ $status -ne 0 ]; then
  echo "AWS Create cluster failed"
else
  echo "Cluster started. Monitor it using the cluster id and using aws emr describe-cluster --cluster-id <clusterid>"
fi
exit $status
