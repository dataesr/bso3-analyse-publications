class HarvesterException(Exception):
    pass


class PublicationDownloadFileException(HarvesterException):
    pass


class EmptyFileContentException(PublicationDownloadFileException):
    pass


class FailedRequest(PublicationDownloadFileException):
    pass

