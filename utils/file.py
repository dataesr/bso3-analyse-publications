import os
#import magic
import gzip
from config.path_config import COMPRESSION_EXT

def compress(file):
    deflated_file = file + COMPRESSION_EXT
    with open(file, "rb") as f_in:
        with gzip.open(deflated_file, "wb") as f_out:
            f_out.write(f_in.read())
    return deflated_file


def decompress(file):
    inflated_file = os.path.splitext(file)[0]
    with gzip.open(file, "rb") as f_in:
        with open(inflated_file, "wb") as f_out:
            f_out.write(f_in.read())
    return inflated_file


#def _is_valid_file(file, mime_type):
#    """Check if the file exists and is not empty and is of the correct mime_type"""
#    target_mime = []
#    if mime_type == "png":
#        target_mime.append("image/png")
#    else:
#        target_mime.append("application/" + mime_type)
#    file_type = ""
#    if os.path.isfile(file):
#        if not is_file_not_empty(file):
#            return False
#        file_type = magic.from_file(file, mime=True)
#    return file_type in target_mime

def is_valid_pdf(filename):
    try:
        file = open(filename, 'rb')
        x = file.read(1024)
        return (b'%PDF' in x[0:4])
    except:
        return False

def is_file_not_empty(filepath: str) -> bool:
    filesize = os.path.getsize(filepath)
    return filesize > 0


def write_to_file(file_content, filepath) -> None:
    with open(filepath, 'wb') as f:
        f.write(file_content)
