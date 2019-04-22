# -*- coding: utf-8 -*-
""" Cam API wrapper methods module """

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


class CamWrapper(object):

    def __init__(self, session_start_ms, session_end_ms, cameraId, clipId):
        self.alias = ''
        self.clips = []

        clip_key = '/{0}/{1}.mp4'.format(cameraId, clipId)
        clip_url = utils.url_join(config.CLIP_URL, clip_key)

        thumbnail_key = '/{0}/{1}_{{size}}.jpg'.format(cameraId, clipId)
        thumbnail_url = utils.url_join(config.CLIP_URL, thumbnail_key)

        self.metadata = {
            "clipId": clipId,
            "cameraId": cameraId,
            "startTimestampInMs": session_start_ms,
            "endTimestampInMs": session_end_ms,
            "status": "CLIP_PENDING",
            "bucket": config.S3_BUCKET_DESTINATION,
            "clip": {
                "key": clip_key,
                "url": clip_url
            },
            "retryTimestamps": [],
            "thumbnail": {
                "key": thumbnail_key,
                "url": thumbnail_url,
                "sizes": list(map(int, config.THUMBNAIL_SIZES.split()))
            }
        }

    def get_alias(self):
        """
            Given a camera's cameraId string, calls the
            cameras-service API to retrieve the camera's alias.
        """
        endpoint = '/cameras/{}'.format(self.metadata['cameraId'])

        try:
            self.alias = utils.get_request(
                config.CAM_SERVICES_URL, endpoint)['alias']
            logger.debug('Cam alias retrieved: {}'.format(self.alias))

        except (KeyError, TypeError) as e:
            logger.error('{0} returned {1}'.format(endpoint, e))

    def get_clip_names(self):
        """
            Given a camera's alias string, a unix start timestamp in
            milliseconds and a unix end timestamp in milliseconds,
            return a list with clip names within that period.
        """
        endpoint = '/cameras/recording/{0}?'.format(self.alias) + \
            'startDate={0}&endDate={1}&allowPartialMatch=true'.format(
                self.metadata['startTimestampInMs'],
                self.metadata['endTimestampInMs'])

        response = utils.get_request(config.RECORDINGS_URL, endpoint)

        if response:
            for clip in response:
                try:
                    clip_name = clip['recordingUrl']
                    self.clips.append(utils.get_basename_str(clip_name))

                except KeyError as e:
                    logger.error('{0} returned {1}'.format(url, e))

            logger.info(
                'Retrieved clip(s): {0}'.format(self.clips))

        else:
            logger.info('No clips were found for {}'.format(endpoint))

    def put_clip_metadata(self):
        """
            Given the clip metadata, generate a HTTP PUT request to the
            Cameras Service.
        """
        endpoint = '/clips'
        response = utils.put_request(
            config.CAM_SERVICES_URL, endpoint, self.metadata)

    def update_clip_status(self, clip_status):
        """
            Updates the clip metadada status dictionary so that the data can be
            PUT back to camera services and/or sent to a SNS message.
        """
        self.metadata['status'] = clip_status
