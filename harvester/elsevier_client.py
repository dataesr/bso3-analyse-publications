from harvester.base_api_client import BaseAPIClient


class ElsevierClient(BaseAPIClient):
    def _get_publication_url(self, doi: str) -> str:
        publication_url = self.publication_base_url + doi
        return publication_url
