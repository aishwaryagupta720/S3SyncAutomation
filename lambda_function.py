import subprocess
import logging
import os
import boto3
from subprocess import check_output, STDOUT, CalledProcessError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Parent Bucket Details
master_buckets={'catapult':['rocade-capital-prod-landing-zone'],'sling':['rocade-capital-prod-unredacted-zone','rocade-capital-prod-staging-zone'],'keep':['rocade-capital-prod-redacted-zone']}
backup_buckets={'catapult':['rocade-capital-prod-landing-zone-backup'],'sling':['rocade-capital-prod-unredacted-zone-backup','rocade-capital-prod-staging-zone-backup'],'keep':['rocade-capital-prod-redacted-zone-backup']}

# Environment Variables
prefix = os.environ["PREFIX"]
topic_arn = os.environ["SNS"]

# to send sns notifications on error
sns_client = boto3.client('sns')


def run_command(command):
    command_list = command.split(' ')
    print(command_list)    
    
    try:
    
        logger.info("Running shell command: \"{}\"".format(command))
        result = subprocess.run(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # o = check_output(command, stderr=STDOUT, shell=True)
    
        if not result.stderr or result.stderr.isspace():
            logger.info("Command output:\n---\n{}\n---".format(result.stdout.decode('UTF-8')))
        else:
            logger.info("Command output:\n---\n{}\n---".format(result.stdout.decode('UTF-8')))
            logger.error("Command Error:\n---\n{}\n---".format(result.stderr.decode('UTF-8')))
            sns_client.publish(TopicArn=topic_arn, Message="The S3 SYNC Lambda in {} is facing Command Error:\n---\n{}\n---".format(prefix, result.stderr.decode('UTF-8')))


    except Exception as e:
        logger.error("Exception: {}".format(e))
        sns_client.publish(TopicArn=topic_arn,Message="The S3 SYNC Lambda in {} is facing Exception: {}".format(prefix,e))
        
        return False

    return True

def lambda_handler(event, context):
    
    source = master_buckets[prefix]
    destination = backup_buckets[prefix]
    print(source, destination)
    for src_bucket,dest_bucket in zip(source,destination):
        # run_command('/opt/aws --version')
        run_command('/opt/aws s3 sync s3://{} s3://{}'.format(src_bucket,dest_bucket))
