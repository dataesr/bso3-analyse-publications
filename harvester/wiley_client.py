from harvester.base_api_client import BaseAPIClient


class WileyClient(BaseAPIClient):
    def _get_publication_url(self, doi: str) -> str:
        doi_encoded = doi.replace("/", "%2F")
        publication_url = self.publication_base_url + doi_encoded
        return publication_url
