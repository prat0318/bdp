#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters. One expected"
    exit
fi

export RND=$(date +%s)
export ENV=bdp
export EID=pa6768
export ASSGN=$1
export MainClass=WordStatistics
export input=dataSet2.txt

echo "Building package..."
mvn package

if [ $? -eq 0 ]; then
    echo "Uploading to S3..."
    aws s3 cp target/bdp-0.$ASSGN.jar s3://utcs378/$EID/jars/

    echo "Launching the EMR job..."
    job_id_raw=$(aws emr create-cluster --log-uri s3://utcs378/$EID/logs/ \
                           --ami-version 2.4.7 \
                           --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m1.small \
                                             InstanceGroupType=CORE,InstanceCount=1,InstanceType=m1.small  \
                           --bootstrap-actions "Path=s3://utcs378/$EID/bootstrap.sh,Name=Classpath" \
                           --steps "Type=CUSTOM_JAR,"`
                                   `"Name=CustomJAR,"`
                                   `"ActionOnFailure=CONTINUE,"`
                                   `"Jar=s3://utcs378/$EID/jars/bdp-0.$ASSGN.jar,"`
                                   `"Args=com.refactorlabs.cs378.assign$ASSGN.$MainClass,"`
                                        `"s3n://utcs378/data/$input,"`
                                        `"s3n://utcs378/$EID/output/assign${ASSGN}_$RND" \
                           --auto-terminate \
                           --no-visible-to-all-users \
                           --name ${EID}_a${ASSGN}_$RND)
    job_id=$(echo $job_id_raw | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["ClusterId"]')
    echo "Job Id created... "$job_id

    current="not_done"
    until [ "$current" = "TERMINATED" -o "$current" = "TERMINATED_WITH_ERRORS" ]; do
        sleep 30
        update=$(aws emr describe-cluster --cluster-id $job_id)
        current=$(echo $update | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["Cluster"]["Status"]["State"]')
        update_msg=$(echo $update | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["Cluster"]["Status"]["StateChangeReason"]')
        echo "Current State: $current Detail: $update_msg"
    done

    if [ "$current" = "TERMINATED" ]; then
        echo "Copying output files from S3 to ~/Downloads..."
        aws s3 cp --recursive s3://utcs378/$EID/output/assign${ASSGN}_$RND ~/Downloads/assign${ASSGN}_$RND
    fi

else
    echo "Build Failed :("
fi
