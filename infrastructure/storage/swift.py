import shutil
from typing import List

from swiftclient.service import SwiftError, SwiftService, SwiftUploadObject

from application.server.main.logger import get_logger
from config.harvester_config import config_harvester
from config.logger_config import LOGGER_LEVEL
from domain.ovh_path import OvhPath

logger = get_logger(__name__, level=LOGGER_LEVEL)

METADATA_DUMP = config_harvester['metadata_dump']
PUBLICATIONS_DUMP = config_harvester['publications_dump']


class Swift(object):

    def __init__(self, config):
        self.config = config
        options = self._init_swift_options()

        options['object_uu_threads'] = 20
        self.swift = SwiftService(options=options)
        container_names = []
        try:
            list_account_part = self.swift.list()
            for page in list_account_part:
                if page["success"]:
                    for item in page["listing"]:
                        i_name = item["name"]
                        container_names.append(i_name)
                        if i_name == METADATA_DUMP:
                            print("using input SWIFT", METADATA_DUMP, "container:", item)
                        elif i_name == PUBLICATIONS_DUMP:
                            print("using output SWIFT", PUBLICATIONS_DUMP, "container:", item)
                else:
                    logger.error("error listing SWIFT object storage containers")

        except SwiftError as e:
            logger.exception(f"error listing containers: {str(e)}")

        if PUBLICATIONS_DUMP not in container_names:
            # create the container
            try:
                self.swift.post(container=PUBLICATIONS_DUMP)
            except SwiftError:
                logger.exception(
                    "error creating SWIFT object storage container " + PUBLICATIONS_DUMP)
        else:
            logger.debug("container already exists on SWIFT object storage: " + PUBLICATIONS_DUMP)

    def _init_swift_options(self):
        options = {}
        for key in self.config["swift"]:
            if self.config["swift"][key] and len(self.config["swift"][key].strip()) > 0:
                options[key] = self.config["swift"][key]
        return options

    def upload_files_to_swift(self, container, file_path_dest_path_tuples: List):
        """
        Bulk upload of a list of files to current SWIFT object storage container
        """
        # Slightly modified to be able to upload to more than one dest_path
        objs = [SwiftUploadObject(file_path, object_name=str(dest_path)) for file_path, dest_path in
                file_path_dest_path_tuples if isinstance(dest_path, OvhPath)]
        try:
            for result in self.swift.upload(container, objs):
                if not result['success']:
                    error = result['error']
                    if result['action'] == "upload_object":
                        logger.error("Failed to upload object %s to container %s: %s" % (
                            container, result['object'], error))
                    else:
                        logger.exception("%s" % error, exc_info=True)
                else:
                    if result['action'] == "upload_object":
                        logger.debug(
                            f'Result upload : {result["object"]} succesfully uploaded on {result["container"]} (from {result["path"]})')
        except SwiftError:
            logger.exception("error uploading file to SWIFT container", exc_info=True)

    def download_files(self, container, file_path, dest_path):
        """
        Download a file given a path and returns the download destination file path.
        """
        if type(file_path) == str:
            objs = [file_path]
        elif type(file_path) == list:
            objs = file_path
        try:
            for down_res in self.swift.download(container=container, objects=objs):
                if down_res['success']:
                    local_path = down_res['path']
                    shutil.move(local_path, dest_path)
                else:
                    logger.error("'%s' download failed" % down_res['object'])
        except SwiftError:
            logger.exception("error downloading file from SWIFT container")

    def get_swift_list(self, container, dir_name=None):
        """
        Return all contents of a given dir in SWIFT object storage.
        Goes through the pagination to obtain all file names.
        """
        result = []
        try:
            options = {"prefix": dir_name} if dir_name else None
            list_parts_gen = self.swift.list(container=container, options=options)
            for page in list_parts_gen:
                if page["success"]:
                    for item in page["listing"]:
                        result.append(item["name"])
                else:
                    logger.error(page["error"])
        except SwiftError as e:
            logger.error(e.value)
        return result
