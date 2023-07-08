from collections.abc import Sequence
from datetime import datetime


class ProcessedEntry(Sequence):
    def __init__(self,
                 doi: str,
                 uuid: str,
                 is_harvested: str,
                 softcite_version: str,
                 grobid_version: str,
                 harvester_used: str,
                 domain: str,
                 url_used: str,
                 harvesting_date: datetime,
                 datastet_version: str):
        self.doi: str = doi
        self.uuid: str = uuid
        self.is_harvested: str = is_harvested
        self.grobid_version: str = grobid_version
        self.softcite_version: str = softcite_version
        self.harvester_used: str = harvester_used
        self.domain: str = domain
        self.url_used: str = url_used
        self.harvesting_date = harvesting_date
        self.datastet_version: str = datastet_version
        self._tup = (self.doi, self.uuid, self.is_harvested, self.softcite_version, self.grobid_version,
                     self.harvester_used, self.domain, self.url_used, self.harvesting_date, self.datastet_version)

    def actualize(self):
        self._tup = (self.doi, self.uuid, self.is_harvested, self.softcite_version, self.grobid_version,
                     self.harvester_used, self.domain, self.url_used, self.harvesting_date, self.datastet_version)

    def __repr__(self) -> str:
        self.actualize()
        return str(self._tup)

    def __getitem__(self, i):
        self.actualize()
        return self._tup[i]

    def __len__(self):
        return len(self._tup)
