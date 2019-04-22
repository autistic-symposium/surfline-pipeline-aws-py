# -*- coding: utf-8 -*-
""" Clip generator entry module """

import os
import json
import logging

import lib.utils as utils
import lib.config as config

import lib.cam_wrapper as cam_wrapper
import lib.aws_wrapper as aws_wrapper
import lib.ffmpeg_wrapper as ffmpeg_wrapper


# For logging at AWS.
root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logging.basicConfig(level=config.LOG_LEVEL)
logger = logging.getLogger(__name__)


def extract_event_data(event_raw):
    """
        Given an event that triggers the lambda function, extract the
        necessary data from its contract.
    """
    try:
        body = event_raw["Records"][0]["body"].replace("'", "\"")
        send_ts = event_raw["Records"][0]["attributes"]["SentTimestamp"]
        return json.loads(body), send_ts

    except (KeyError, IndexError) as e:
        error = 'Ill-formatted event message: {}'.format(e)
        logger.error(error)
        raise Exception(error)


def retrieve_video_list(session_start_ms, session_end_ms, cameraId, clipId):
    """
        Instantiate the cameras API wrapper, creating a list of videos
        for the given event data. Return the instance object.
    """
    if config.LOG_LEVEL <= 'INFO':
        begin = utils.epoch_to_timestamp(session_start_ms)
        end = utils.epoch_to_timestamp(session_end_ms)
        logger.info(
            'Attempting to retrieve clips between {0} and \
            {1}...'.format(begin, end))

    cw = cam_wrapper.CamWrapper(
        session_start_ms, session_end_ms, cameraId, clipId)
    cw.get_alias()
    cw.get_clip_names()

    return cw


def upload_assets(aw, cameraId, output_clip_path, thumbnail_list):
    """
        Call the AWS wrapper upload to S3 method.
    """
    logger.info('Uploading clip and thumbnail to S3...')

    clip_to_upload = os.path.join(
        cameraId, utils.get_basename_str(output_clip_path))
    aw.upload_asset(output_clip_path, clip_to_upload)

    for thumbnail in thumbnail_list:

        thumbnail_to_upload = os.path.join(
            cameraId, utils.get_basename_str(thumbnail))
        aw.upload_asset(thumbnail, thumbnail_to_upload)

        utils.remove_file(thumbnail)


def update_clip_status(cw, message):
    """
        Update the camera wrapper clip availability.
    """
    logger.info('Status for ClipId {0}: {1}'.format(
        cw.metadata['clipId'], message))
    cw.update_clip_status(message)

    logger.info('Sending generated clip metadata to Cameras Services...')
    cw.put_clip_metadata()


def get_delta_time(start_event, end_request):
    """
        Given two event timestamps (in millseconds, e.g.1538601628000),
        return their absolute time difference, in seconds.
    """
    return abs(int(start_event) - int(end_request))/1000


def is_clip_larger_than_limit(startTimestampInMs, endTimestampInMs):
    """
        Check whether the requested clip has a length larger
        than the limit, and return True if this is the case.
    """
    delta = get_delta_time(startTimestampInMs, endTimestampInMs)
    return int(config.VIDEO_MAX_LEN) < delta


def update_success_clip_creation(fw, aw, cw, cameraId):
    """
        Given a ffmpeg_wrapper object, an aws_wrapper object,
        a cam_wrapper object, and cameraID, creates the clip's
        thumbnails, upload all the assets to the S3 buket, and
        updates the status at the cameras service.
    """
    fw.create_thumbnails()

    upload_assets(aw, cameraId,
                  fw.output_clip_path, fw.thumbnails)
    update_clip_status(cw, 'CLIP_AVAILABLE')
    aw.send_sns_msg(cw.metadata)


def update_unsuccess_clip_metadata(cw):
    """
        When the clip fails to be created, remove
        URLs and storage data from its metadata.
    """
    del cw.metadata["bucket"]
    del cw.metadata["clip"]
    del cw.metadata["thumbnail"]["url"]
    del cw.metadata["thumbnail"]["key"]
    cw.metadata["thumbnail"]["sizes"] = []


def update_unsuccess_clip_creation(cw, aw, msg):
    """
        Given an aws wrapper object, a cam_wrapper object,
        and an alternate flow message, update the status in
        the cameras services and to an SNS message.
    """
    update_unsuccess_clip_metadata(cw)
    update_clip_status(cw, msg)
    aw.send_sns_msg(cw.metadata)


def set_clip_output(fw):
    """
        Given an ffmpeg wrapper object, set the output
        clip string.
    """
    output_clip = '{}.mp4'.format(fw.clipId)
    fw.output_clip_path = os.path.join(
        config.CLIP_DOWNLOAD_DEST, output_clip)


def download_video(video, aw):
    """
        Given a video name and an aws wrapper object, download
        the video, and return the path where this video is
        in disk (input_path) and an tmp output path destination
        for ffmpeg operations.
    """
    input_path = os.path.join(
        config.CLIP_DOWNLOAD_DEST, video)
    output_path = os.path.join(
        config.CLIP_DOWNLOAD_DEST, 'tmp_{}'.format(video))

    aw.download_video(video, input_path)

    return input_path, output_path


def create_session_clip(fw, aw):
    """
        Given instances of ffmpeg wrapper and aws wrapper,
        download, trim and concanate videos to create
        the session clip, returning True if successful.

        If more than 2 videos need to be downloaded to
        create the clip, it will download and decode/trim each
        video at the time (this is due AWS Lambda function's
        restrictions, i.e. 500 MB disk limit).
    """

    set_clip_output(fw)

    if len(fw.videos) < 1:
        logger.error(
            'No clips found for {}'.format(output_clip))
        return False

    elif len(fw.videos) == 1:

        video_path, out = download_video(fw.videos.pop(), aw)

        if os.path.exists(video_path):

            timestamp_video = utils.get_timestamp_str(video_path)
            epoch_video = utils.timestamp_to_epoch(timestamp_video)
            trim_start, trim_end = fw.calculate_trim_time(epoch_video)

            fw.trim_clip(video_path, fw.output_clip_path,
                         trim_start, trim_end)

        else:
            logger.error('Could not trim {}.'.format(video_path))
            return False

    else:
        videos_path = []

        # Video list starts with the last video (end), so we reverse it.
        for i, video in enumerate(reversed(fw.videos)):

            input_path, output_path = download_video(video, aw)

            if os.path.exists(input_path):

                timestamp_video = utils.get_timestamp_str(input_path)
                epoch_video = utils.timestamp_to_epoch(timestamp_video)

                trim_start, trim_end = fw.calculate_trim_time(epoch_video)

                # First video.
                if i == 0:
                    fw.trim_clip(input_path, output_path, trim_start)

                # Last video.
                elif i == len(fw.videos) - 1:
                    fw.trim_clip(input_path, output_path, '0:0', trim_end)

                # Any video in the middle.
                else:
                    fw.trim_clip(input_path, output_path, '0:0')

                utils.remove_file(input_path)
                videos_path.append(output_path)

            else:
                logger.error(
                    'Could not trim {}.'.format(output_path))
                return False

        fw.concatenate_videos(videos_path)

        for video in videos_path:
            utils.remove_file(video)

    return True


def handler(event, context=None):
    """
        Lambda function handler and entry point.
    """
    # Clean up environment from previous artifacts.
    utils.run_subprocess_with_output(['rm', '-rf', '/tmp/*'])

    # Retrieve event.
    event_body, sent_ts = extract_event_data(event)

    try:
        start_ts = int(event_body['startTimestampInMs'])
        end_ts = int(event_body['endTimestampInMs'])
        camera_id = event_body['cameraId']
        clip_id = event_body['clipId']
        retry_ts = event_body.get('retryTimestamps', [])

    except TypeError as e:
        logger.error('Ill-formatted event {0}: {1}'.format(event_body, e))

    # Retrieve the video list.
    cw = retrieve_video_list(start_ts, end_ts,
                             camera_id, clip_id)

    # Start an instance at AWS.
    aw = aws_wrapper.AwsWrapper()

    # Clips cannot be longer than a give value, so check this.
    if is_clip_larger_than_limit(start_ts, end_ts):
        logger.info('Clip length is larger than limit. Exiting.')
        update_unsuccess_clip_creation(cw, aw, 'CLIP_TOO_LONG')
        return

    # Check if clips list was successfully retrieved.
    if cw.clips:

        fw = ffmpeg_wrapper.FfmpegWrapper(
            cw.alias,
            cw.clips,
            start_ts,
            end_ts,
            clip_id)

        # Clip was successfully created.
        if create_session_clip(fw, aw):
            update_success_clip_creation(fw, aw, cw, camera_id)

        # Hard-failure, which will make lambda function retry.
        else:
            update_clip_status(cw, 'FATAL_ERROR')
            raise SystemExit('Unable to create session clip from sources.')

    # If no clips list was retrieved.
    else:

        # Verify whether this message is a retry, then calculates
        # the time difference between the timestamp in the SQS event
        # request (or in the first try), and the timestamp of the
        # request for the beginning of the clip .
        if retry_ts:

            max_retry_num = int(
                config.SQS_RETRY_LIMIT)//int(config.SQS_TIMEOUT)
            if len(retry_ts) > max_retry_num:
                logger.info('Max number of retries were reached. Exiting.')
                update_unsuccess_clip_creation(cw, aw, 'CLIP_RETRY_TIMEOUT')
                return

            else:
                logger.info('Retry number {0}: \
                    The difference of time between clip start and request \
                    is {1}s (retry timestamp is {2} and start timestamp \
                    is {3}). Timeout limit is {4}.'.format(
                    len(retry_ts),
                    delta_time, retry_ts[0],
                    start_ts,
                    config.SQS_TIMEOUT))
                delta_time = get_delta_time(
                    retry_ts[0], start_ts)

        else:

            delta_time = get_delta_time(sent_ts, start_ts)

        # If time difference is smaller than 3 days and
        # larger than 15 minutes (in secs).
        if delta_time > int(config.SQS_RETRY_LIMIT) and \
                delta_time < int(config.OUT_OF_RANGE_LIMIT):
            logger.info('Delta time is {0}, which is larger than the retry \
                limit ({1}) and smaller than the out-of-range limit \
                ({2})'.format(
                delta_time, config.SQS_RETRY_LIMIT, config.OUT_OF_RANGE_LIMIT))
            update_unsuccess_clip_creation(cw, aw, 'CLIP_NOT_AVAILABLE')

        # If time difference between the current time and the
        # requested time range is larger than 3 days (in secs).
        elif delta_time > int(config.OUT_OF_RANGE_LIMIT):
            update_unsuccess_clip_creation(cw, aw, 'CLIP_OUT_OF_RANGE')

        # If time difference is smaller than 15 minutes.
        else:
            update_clip_status(cw, 'CLIP_PENDING')
            event_body['retryTimestamps'].append(sent_ts)
            aw.send_sqs_msg(event_body, sent_ts)
