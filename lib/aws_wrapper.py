# -*- coding: utf-8 -*-
""" AWS API wrapper methods module """

import os
import json
import boto3
import botocore
import logging

import utils
import config


# For logging at AWS.
root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logging.basicConfig(level=config.LOG_LEVEL)
logger = logging.getLogger(__name__)


class AwsWrapper(object):

    def __init__(self):
        self.sns_topic = config.AWS_SNS_TOPIC
        self.sqs_queue_url = config.AWS_SQS_QUEUE_URL

    def _create_aws_client(self, client_name):
        """
            Return a client object for a given AWS resource.
        """
        return boto3.client(client_name)

    def download_video(self, video, destination):
        """
            Given a video name and a destination in disk,
            download it from the origin S3 bucket.
        """

        s3 = boto3.resource('s3')
        filepath = os.path.join(config.S3_BUCKET_ORIGIN_DIR, video)

        try:
            s3.Bucket(config.S3_BUCKET_ORIGIN).download_file(
                filepath, destination)
            utils.update_permission(destination)
            logger.info('{} downloaded.'.format(filepath))

        except botocore.exceptions.ClientError as e:
            error = 'Error attempting to download {0}: {1}'.format(filepath, e)
            logger.error(error)

    def upload_asset(self, filename, destination):
        """
            Given an asset name and a folder structure,
            uploads it to a S3 bucket.
        """

        s3 = self._create_aws_client('s3')

        try:
            s3.upload_file(filename, config.S3_BUCKET_DESTINATION, destination)
            logger.debug('{0} saved at {1}.'.format(filename, destination))

        except botocore.exceptions.ClientError as e:
            error = 'Could not upload {0}: {1}'.format(filename, e)
            logger.error(error)

        except boto3.exceptions.S3UploadFailedError as e:
            error = 'Could not auth with S3 {0}: {1}'.format(filename, e)
            logger.error(error)

        except OSError as e:
            error = 'Could not find {0}: {1}'.format(filename, e)
            logger.error(error)

    def send_sns_msg(self, clip_metadata):
        """
            Given a JSON SNS message, send it upstream to the SNS topic at AWS.
        """
        sns = self._create_aws_client('sns')

        try:
            response = sns.publish(
                TopicArn=self.sns_topic,
                Message=json.dumps({'default': json.dumps(clip_metadata)}),
                MessageStructure='json'
            )

        except botocore.exceptions.ClientError as e:
            error = 'Could not send SNS message {0}: {1}'.format(
                clip_metadata, e)
            logger.error(error)

    def send_sqs_msg(self, body, timestamp):
        """
            Given a JSON SQS body message and the timestamp
            when the event was created (in milliseconds),
            compose an SQS message, sending it back to the queue.
        """
        sqs = self._create_aws_client('sqs')

        try:
            response = sqs.send_message(
                QueueUrl=self.sqs_queue_url,
                MessageBody=json.dumps(body),
                DelaySeconds=int(config.SQS_TIMEOUT)
            )

            logger.info('SQS message ID {} sent back to the queue.'.format(
                response.get('MessageId')))

        except (botocore.exceptions.ClientError, TypeError) as e:
            error = 'Could not send SQS message {0}: {1}'.format(
                body, e)
            logger.error(error)
