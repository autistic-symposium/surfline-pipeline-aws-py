# -*- coding: utf-8 -*-
""" Ffmpeg API wrapper methods module """

import os
import logging
import tempfile

import config
import utils


# For logging at AWS.
root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logging.basicConfig(level=config.LOG_LEVEL)
logger = logging.getLogger(__name__)


class FfmpegWrapper(object):

    def __init__(self, alias, videos, epoch_start, epoch_end, clipId):
        self.alias = alias
        self.videos = videos
        self.epoch_start = int(epoch_start)
        self.epoch_end = int(epoch_end)
        self.clipId = clipId
        self.thumbnails = []
        self.output_clip_path = ''
        self.trim_thumbnail = ''
        if config.LAMBDA_TASK_ROOT:
            self.ffmpeg = os.path.join(config.LAMBDA_TASK_ROOT, 'ffmpeg')
        else:
            self.ffmpeg = 'ffmpeg'

    def trim_clip(self, video, output, start, end=None):
        """
            Given a MP4 video, an output path, a start time,
            and an optional end time; trim the video.
        """
        utils.update_permission(video)

        cmd = [self.ffmpeg,
               '-i', video,
               '-ss', str(start),
               '-to', str(end),
               '-vf', 'format=yuv420p',
               '-preset', 'fast',
               '-movflags', '+faststart',
               '-y',
               output]

        # If no end time was specified, remove -to flag.
        if not end:
            try:
                flag_index = cmd.index('-to')
                if flag_index:
                    cmd = cmd[:flag_index] + cmd[(flag_index + 2):]
            except ValueError:
                pass

        logger.debug('Trimming {0} from {1} to {2} with command {3}'.format(
            video, start, end, ' '.join(cmd)))
        ok_msg = 'Clip saved at {}.'.format(output)
        err_msg = 'Could not trim clip {}'.format(output)
        utils.run_subprocess(cmd, ok_msg, err_msg)

    def _generate_filelist(self, videos_fullpath, f):
        """
            Given a list of MP4 video files, generate a
            txt with their location so that this can
            be used with ffmpeg concat command.
        """
        try:
            for video in videos_fullpath:
                f.write('file {0}\n'.format(video))
            return True

        except IOError as e:
            logger.error(
                'Cannot create clip filelist for concatenating: {}'.format(e))
            return False

    def concatenate_videos(self, videos_path):
        """
            Given a list of MP4 video files, concatenate
            them to an output file.
        """
        with tempfile.NamedTemporaryFile(delete=False, mode='a+r') as f:

            if self._generate_filelist(videos_path, f):

                utils.update_permission(f.name)

                f.read()

                cmd = [self.ffmpeg,
                       '-f', 'concat',
                       '-safe', '0',
                       '-i', f.name,
                       '-movflags', '+faststart',
                       '-vcodec', 'copy',
                       '-c', 'copy',
                       '-y',
                       self.output_clip_path]

                ok_msg = 'Videos {0} were concatenated to {1}'.format(
                    videos_path, self.output_clip_path)
                err_msg = 'Could not concatenate {0}'.format(videos_path)
                utils.run_subprocess(cmd, ok_msg, err_msg)

    def calculate_trim_time(self, epoch_video):
        """
            Given a video str epoch time, and two epoch time
            intervals, returns a human (ffmpeg) readable start
            and end trim time.
        """
        # Find where to trim within the video.
        clip_start_trim = abs(epoch_video - self.epoch_start)
        clip_end_trim = abs(self.epoch_end - epoch_video)

        # Create the strings for trimming time for ffmpeg.
        trim_start = utils.humanize_delta_time(clip_start_trim)
        trim_end = utils.humanize_delta_time(clip_end_trim)

        # Create a time around 25% for best match for thumbnail.
        self.trim_thumbnail = utils.humanize_delta_time(
            (abs(epoch_video - self.epoch_end) - clip_start_trim)/4)

        logger.debug("Clip's trim time is {0} to {1}".format(
            trim_start, trim_end))
        return trim_start, trim_end

    def _thumbnail_resize(self):
        """
            Resize an original thumbnail JPEG file to a list of
            resizes.
        """
        img_output_path = self.thumbnails[0]

        if not os.path.isfile(img_output_path):
            logger.error(
                'Could not find thumbnail {}.'.format(img_output_path))
            return

        for size in config.THUMBNAIL_SIZES.split():

            img_output_resized_path = os.path.join(
                config.CLIP_DOWNLOAD_DEST,
                '{0}_{1}.jpg'.format(self.clipId, size))
            self.thumbnails.append(img_output_resized_path)

            cmd = [self.ffmpeg,
                   '-i', img_output_path,
                   '-vf', 'scale={}:-1'.format(size),
                   '-y',
                   img_output_resized_path]

            ok_msg = 'Thumbnail {0} resized to {1}'.format(
                img_output_path, size)
            err_msg = 'Could not resize {0} to {1}.'.format(
                img_output_path, size)
            utils.run_subprocess(cmd, ok_msg, err_msg)

    def create_thumbnails(self):
        """
            Given a clip filename and path creates a thumbnail
            image in several resolutions, adding their paths
            to a list.
        """
        img_output = '{0}_original.jpg'.format(self.clipId)
        img_output_path = os.path.join(config.CLIP_DOWNLOAD_DEST, img_output)
        logger.info('Creating thumbnail for {}'.format(img_output_path))

        cmd = [self.ffmpeg,
               '-ss', self.trim_thumbnail,
               '-i', self.output_clip_path,
               '-vframes', '1',
               '-y',
               img_output_path]

        ok_msg = 'Thumbnail created at {}.'.format(img_output_path)
        err_msg = 'Failed to create thumbnail for {}'.format(
            self.output_clip_path)
        utils.run_subprocess(cmd, ok_msg, err_msg)

        self.thumbnails.append(img_output_path)
        self._thumbnail_resize()
