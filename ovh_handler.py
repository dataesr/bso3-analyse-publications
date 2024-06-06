import os
import shutil
from glob import glob
from types import SimpleNamespace
from typing import List

from application.server.main.logger import get_logger
from config.logger_config import LOGGER_LEVEL
from domain.ovh_path import OvhPath
from infrastructure.storage.swift import Swift

logger_console = get_logger(__name__, level=LOGGER_LEVEL)


def generateStoragePath(identifier):
    """Convert a file name into a path with file prefix as directory paths:
    123456789 -> 12/34/56/78/123456789/"""
    return OvhPath(identifier[:2], identifier[2:4], identifier[4:6], identifier[6:8], identifier)


def get_partitions(files: List[str], partition_size: int) -> List[List[str]]:
    """Return a list of partitions of files"""
    if len(files) > partition_size:
        return [files[i : i + partition_size] for i in range(0, len(files), partition_size)]
    return [files]


def download_files(_swift, dest_dir, files):
    """Make dest_dir if it does not exist and download files not already present in dest_dir"""
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    files = [file for file in files if not os.path.exists(os.path.join(dest_dir, file))]
    try:
        _swift.download_files(_swift.config["publications_dump"], files, dest_dir)
    except shutil.Error:
        logger_console.exception("File already exists", exc_info=True)



def upload_and_clean_up(_swift: Swift, service_ns: SimpleNamespace):
    files = glob(service_ns.dir + "*")
    files_to_upload = []
    for file in files:
        if file.endswith(service_ns.suffix):
            current_uuid = os.path.basename(file).split(".")[0]
            destination_path = OvhPath(service_ns.prefix+'-0.8.0-newround/publication', generateStoragePath(current_uuid), os.path.basename(file))
            files_to_upload.append((file, destination_path))
    #files_to_upload = [
    #    (file, OvhPath(service_ns.prefix, generateStoragePath(os.path.basename(file).split(".")[0]), os.path.basename(file)))
    #    for file in files if file.endswith(service_ns.suffix)
    #]
    _swift.upload_files_to_swift(_swift.config["publications_dump"], files_to_upload)
    for file in files:
        os.remove(file)
