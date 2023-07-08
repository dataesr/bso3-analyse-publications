from datetime import datetime
from time import sleep
import requests

from application.server.main.logger import get_logger
from domain.abstract_api_client import AbstractAPIClient

from config.logger_config import LOGGER_LEVEL
from harvester.exception import FailedRequest
from utils.file import write_to_file

logger = get_logger(__name__, level=LOGGER_LEVEL)


class BaseAPIClient(AbstractAPIClient):
    def __init__(self, config: dict) -> None:
        logger.info(f"Initializing the {config['name']} API client")
        self.name = config["name"]
        self.publication_base_url = config["PUBLICATION_URL"]
        self._init_throttle(config)
        self.session = self._init_session(config)

    def _init_throttle(self, config):
        self.curr_window = datetime.now()
        self.curr_window_num_requests = 0
        self.max_num_requests = config["throttle_parameters"]["max_num_requests"]
        self.window_size = config["throttle_parameters"]["window_size"]  # in seconds

    def _init_session(self, config) -> requests.Session:
        """A first request has to be made in order to have a real singleton.
        Moreover, it double as an API health check."""
        logger.info(f"Initializing a requests session for {self.name} API")
        session = requests.Session()
        session.headers.update(config["HEADERS"])
        publication_url = self._get_publication_url(config["health_check_doi"])
        response = session.get(publication_url)
        if not response.ok or not response.text[:5] == "%PDF-":
            raise FailedRequest(
                f"First request to initialize the session failed. "
                f"Make sure the publication {config['health_check_doi']} can be downloaded using the {self.name} API. "
                f"Request status code = {response.status_code}, Response content = {response.content}"
            )
        logger.debug("First request to initialize the session succeeded")
        return session

    def throttle(self, window_span_in_seconds, max_num_request_per_window):
        """Regulate the number of requests to the max number of requests per window"""
        now = datetime.now()
        if (now - self.curr_window).seconds >= window_span_in_seconds:
            self.curr_window = now
            self.curr_window_num_requests = 0

        self.curr_window_num_requests += 1
        if self.curr_window_num_requests > max_num_request_per_window:
            num_window_to_wait = self.curr_window_num_requests // max_num_request_per_window
            wait_time = num_window_to_wait * window_span_in_seconds
            logger.debug(f"Holding request for {wait_time}s")
            extra_padding_time = 0.1
            sleep(wait_time + extra_padding_time)
            self.throttle(self.window_size, self.max_num_requests)

    def download_publication(self, doi: str, filepath: str) -> (str, str):
        """
        Will raise a FailedRequest exception (_validate_downloaded_content) if the status_code
        is different from 200, this exception will be caught in the `_download_publication` function
        """
        self.throttle(self.window_size, self.max_num_requests)
        logger.debug(f"Downloading publication using {self.name} client")
        publication_url = self._get_publication_url(doi)
        response = self.session.get(publication_url)
        self._validate_downloaded_content_and_write_it(response, doi, filepath)
        return "success", self.name

    def _validate_downloaded_content_and_write_it(self, response, doi: str, filepath: str) -> None:
        if response.ok:
            if response.text[:5] == "%PDF-":
                write_to_file(response.content, filepath)
                logger.debug(
                    f"The publication with doi = {doi} was successfully downloaded via {self.name} request"
                )

            else:
                raise FailedRequest(f"Not a PDF")
        else:
            raise FailedRequest(
                f"The publication with doi = {doi} download failed via {self.name} request. Request status code = {response.status_code}"
                + f"Response content = {response.content}"
            )

    def _get_publication_url(self, doi: str) -> str:
        raise NotImplemented
