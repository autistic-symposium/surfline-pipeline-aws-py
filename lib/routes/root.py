# -*- coding: utf-8 -*-
""" Root service handler module for AWS Lambda function. 'METHOD_HANDLERS' """

import json
import logging

from lib.handlers import clip_generator_trigger
from lib.config import LOG_LEVEL
from lib import utils

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


def handler(event, context):

    logger.info('Event: {0}'.format(
        json.dumps(event, indent=4, sort_keys=True)))

    # run a resources report before and after processing
    utils.run_resources_report()

    response = clip_generator_trigger.handler(event, context)

    # run a resources report before and after processing
    utils.run_resources_report()

    return response
