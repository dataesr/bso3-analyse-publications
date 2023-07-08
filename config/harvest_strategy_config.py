"""Stratégie de harvesting des publications en accès ouvert
L'objectif est de maximiser l'efficaté du harvester en privilégiant les sources
de publications facilement téléchargeables (archive vs publisher).
La liste contient des conditions sous forme de fonction lambda représentant
l'ordre de priorité des sources. Elles renvoient True ou False en fonction de
l'objet oa_location (dernière observation en date de la publication en
accès ouvert). La liste est parcourue jusqu'à qu'une condition renvoie True
(la dernière étant une catch-all)."""

oa_harvesting_strategy = [
    # arXiv
    lambda oa_location: "repository_normalized" in oa_location
    and oa_location["repository_normalized"] == "arXiv",
    # PubMed Central
    lambda oa_location: "repository_normalized" in oa_location
    and oa_location["repository_normalized"] == "PubMed Central",
    # HAL
    lambda oa_location: "repository_normalized" in oa_location
    and oa_location["repository_normalized"] == "HAL",
    # repo
    lambda oa_location: "repository_normalized" in oa_location
    and oa_location["host_type"] == "repository",
    # Wiley
    lambda oa_location: "wiley" in oa_location["url_for_pdf"],
    # Else
    lambda oa_location: True,
]
