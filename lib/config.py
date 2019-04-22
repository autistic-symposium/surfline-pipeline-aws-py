# -*- coding: utf-8 -*-
""" Config value constants """

from os import environ
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv(join(dirname(__file__), '..', '.env'))

# Logging
try:
    LOG_LEVEL = environ['LOG_LEVEL']
except KeyError:
    raise Exception('Please set the .env file.')


# Local config
CLIP_DOWNLOAD_DEST = environ['CLIP_DOWNLOAD_DEST']
TIMESTAMP_FORMAT = environ['TIMESTAMP_FORMAT']
SQS_TIMEOUT = environ['SQS_TIMEOUT']
SQS_RETRY_LIMIT = environ['SQS_RETRY_LIMIT']
OUT_OF_RANGE_LIMIT = environ['OUT_OF_RANGE_LIMIT']

# Cam API
CAM_SERVICES_URL = environ['CAM_SERVICES_URL']
RECORDINGS_URL = environ['RECORDINGS_URL']
CLIP_URL = environ['CLIP_URL']
THUMBNAIL_SIZES = environ['THUMBNAIL_SIZES']
VIDEO_MAX_LEN = environ['VIDEO_MAX_LEN']

# AWS
S3_BUCKET_ORIGIN = environ['S3_BUCKET_ORIGIN']
S3_BUCKET_ORIGIN_DIR = environ['S3_BUCKET_ORIGIN_DIR']
S3_BUCKET_DESTINATION = environ['S3_BUCKET_DESTINATION']
AWS_SNS_TOPIC = environ['AWS_SNS_TOPIC']
AWS_SQS_QUEUE = environ['AWS_SQS_QUEUE']
AWS_SQS_QUEUE_URL = environ['AWS_SQS_QUEUE_URL']

# LAMBDA_TASK_ROOT is part of the lambda execution environment
# https://docs.aws.amazon.com/lambda/latest/dg/current-supported-versions.html
LAMBDA_TASK_ROOT = environ.get('LAMBDA_TASK_ROOT', '')
