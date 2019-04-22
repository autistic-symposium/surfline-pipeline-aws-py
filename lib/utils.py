# -*- coding: utf-8 -*-
""" Utils methods module """

from __future__ import print_function

import os
import time
import calendar
import requests
import datetime
import logging
import urlparse
import subprocess
import hashlib

import config


# For logging at AWS.
root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logging.basicConfig(level=config.LOG_LEVEL)
logger = logging.getLogger(__name__)


def url_join(domain, endpoint):
    """
        Given a domain and an endpoint strings, return the full url.
    """
    return requests.compat.urljoin(domain, endpoint)


def get_request(domain, endpoint):
    """
        Given an URL, send a HTTP GET request, returning
        a JSON object.
    """
    url = url_join(domain, endpoint)
    r = requests.get(url)

    try:
        if r.status_code == 200:
            logger.debug("GET request to {} was successful.".format(url))
            return r.json()

        else:
            logger.error(r.text)

    except (requests.ConnectionError, requests.Timeout,
            requests.ConnectTimeout, requests.ReadTimeout) as e:
        logger.error(e)


def put_request(domain, endpoint, data):
    """
        Given a domain and endpoint strings, and a dict of data,
        send a HTTP PUT request, returning a JSON response.
    """
    try:
        clip_endpoint = '{0}/{1}'.format(endpoint, data['clipId'])
        url = url_join(domain, clip_endpoint)
        r = requests.put(url, json=data)
        logger.info(
            "PUT {0} with {1}: {2}".format(url, data, r.status_code))
        return True

    except TypeError:
        logger.error('PUT to /cameras/clips failed: ill-formated data.')

    except (requests.ConnectionError, requests.Timeout,
            requests.ConnectTimeout, requests.ReadTimeout) as e:
        logger.error(e)
        return False


def get_basename_str(url):
    """
        Given a URL, return the basename string.
    """
    u = urlparse.urlparse(url)
    logger.debug("Extracted basename string from filename is {}.".format(url))

    return os.path.basename(u.path)


def get_timestamp_str(clipname):
    """
        Given a video name, return the timestamp str.

        Files in the new format have the following format:
        Timestamp yyyymmddThhmmssmmm
        e.g. hbpiernscam.stream.20181031T025200051.mp4

        Files in the old format have the following format:
        e.g. hbpiernscam.20180815T140019.mp4 and need to
        have milliseconds added.
    """
    timestamp = clipname.split('.')[-2]

    try:
        if len(timestamp) < 16:
            # The file is in the old format.
            timestamp = '{}000'.format(timestamp)

        logger.debug(
            "Timestamp from filename is {}.".format(timestamp))

        return timestamp

    except KeyError as e:
        error = 'Could not get clip timestamp from {0}: {1}'.format(
            clipname, e)
        logger.error(error)
        raise Exception(error)


def get_time_now():
    """
        Return the current time in a unix epoch integer in seconds.
    """
    return int(round(time.time()))


def get_location_str(clipname):
    """
        Given a clip name string (e.g. hbpiernscam.20180815T140019.mp4),
        return its location, (e.g. hbpiernscam).
    """
    try:
        location = clipname.split('.')[0]
        logger.debug(
            "Extracted location string from filename is {}.".format(location))
        return location

    except KeyError as e:
        logger.error('Could not get clip location from {0}: \
                                        {1}'.format(clipname, e))


def timestamp_to_epoch(timestamp, timestamp_format=None):
    """
        Given a timestamp and (optionally) a timestamp format,
        return the unix time in milliseconds.
    """
    timestamp_format = timestamp_format or config.TIMESTAMP_FORMAT
    date = time.strptime(timestamp, timestamp_format)
    unix_millisec = calendar.timegm(date) * 1000.

    logger.debug("Timestamp {0} converted to epoch in ms: {1}.".format(
        timestamp, unix_millisec))

    return unix_millisec


def epoch_to_timestamp(epoch_in_ms, timestamp_format=None):
    """
        Given a unix epoch time in milliseconds and (optionally)
        a timestamp format, return a UTC timestamp.
    """
    timestamp_format = timestamp_format or config.TIMESTAMP_FORMAT

    if isinstance(epoch_in_ms, unicode):
        epoch_in_ms = time.mktime(datetime.datetime.strptime(
            epoch_in_ms, timestamp_format).timetuple())

    else:
        epoch = epoch_in_ms/1000.
        timestamp = datetime.datetime.utcfromtimestamp(
            epoch).strftime(timestamp_format)
    logger.debug(
        'Epoch {0} converted to timestamp {1}.'.format(epoch, timestamp))

    return timestamp


def humanize_delta_time(msecs):
    """
        Given a time delta in seconds, return a human (ffmpeg)
        readable string.
    """
    mins, secs = divmod(msecs/1000., 60)

    mins = '{num:02d}'.format(num=int(mins))
    secs = '{num:06.03f}'.format(num=float(secs))

    return '{0}:{1}'.format(mins, secs)


def remove_file(filepath):
    """
        Delete a given file from disk.
    """
    try:
        os.remove(filepath)

    except OSError as e:
        logger.warning('Could not delete {0}: {1}'.format(filepath, e))


def update_permission(filepath, perm='755'):
    """
        Given a path for a file, and an optional permission code,
        update the file's permission.
    """
    try:
        subprocess.check_output(["chmod", perm, filepath])
        logger.debug(
            'File {0} had permission updated to {1}'.format(filepath, perm))

    except subprocess.CalledProcessError as e:
        logger.error(
            'Could not update {0} permissions for \
            {0}: {1}'.format(perm, filepath, e))


def run_subprocess(cmd, ok_msg=None, err_msg=None):
    """
        Given a list of subprocess commands and flags,
        a success message and an error message, run a
        subprocess thread for that cmd.
    """
    ok_msg = ok_msg or 'Command {}: success.'.format(' '.join(cmd))
    err_msg = err_msg or 'Command {} unsuccess.'.format(' '.join(cmd))

    try:
        logger.info('Running subprocess: {0}'.format(' '.join(cmd)))
        with open(os.devnull) as devnull:
            subprocess.check_output(cmd, stdin=devnull)
        logger.debug(ok_msg)

    except (subprocess.CalledProcessError, OSError) as e:

        # When the lambda function is deployed at AWS,
        # every file needs explicit permissions to be modified
        try:
            logger.debug("Changing {} permission.".format(cmd[0]))

            # Moving binary to the work destination
            if os.path.isfile(cmd[0]) and config.CLIP_DOWNLOAD_DEST:
                bin_dest = os.path.join(config.CLIP_DOWNLOAD_DEST, cmd[0])
                subprocess.check_output(["cp", cmd[0], bin_dest])
                update_permission(bin_dest)

            else:
                bin_dest = cmd[0]

            # Running again the command with the new binary
            cmd[0] = bin_dest
            with open(os.devnull) as devnull:
                subprocess.check_output(cmd, stdin=devnull)

        # We add a catch all here because this block will
        # only be reached if the permission change don't work.
        except Exception as e:
            logger.error("{0}: {1}".format(err_msg, e))


def run_subprocess_with_output(cmd):
    """
        Given a list of subprocess commands and flags,
        run it capturing the STDOUT and returning
        this string.
    """
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = process.communicate()
    if err:
        logger.info('Error running {0}: {1}'.format(' '.join(cmd)))
    return out


def run_resources_report():
    """
        Run OS commands to debug the state of the disk storage
        where this app is running. This is used to understand
        lambda function's failures due to IOError and OSError
        exceptions (hence the print statement instead of logging).
    """
    for cmd in [['uname', '-a'],
                ['ls', '-Rla', '/tmp'],
                ['df', '-h']]:
        logger.info('####################################')
        logger.info('# ' + ' '.join(cmd))
        logger.info('####################################')
        logger.info(subprocess.check_output(cmd))

    # Get md5 of ffmpeg binary
    if config.LAMBDA_TASK_ROOT:
        ffmpeg = os.path.join(config.LAMBDA_TASK_ROOT, 'ffmpeg')
    else:
        ffmpeg = 'ffmpeg'

    if os.path.isfile(ffmpeg):
        logger.info('####################################')
        logger.info('# get_md5(ffmpeg)')
        logger.info('####################################')
        logger.info(get_md5(ffmpeg))


def get_md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_video_len(video_path):
    """
        Given a video path, returns its length in
        milliseconds.
    """
    cmd = ['ffprobe',
           '-v',
           'error',
           '-show_entries',
           'format=duration',
           '-of',
           'default=noprint_wrappers=1:nokey=1',
           video_path]

    out = utils.run_subprocess_with_output(cmd)
    logger.info('Video {0} has length {1}'.format(video_path, out))

    try:
        return float(out)*1000
    except ValueError:
        logger.error('Could not extract length for {}'.format(video_path))
        return 0.
