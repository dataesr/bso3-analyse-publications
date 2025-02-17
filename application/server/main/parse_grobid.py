import re
from bs4 import BeautifulSoup
from application.server.main.logger import get_logger

logger = get_logger(__name__)

def json_grobid(filename, GROBID_VERSIONS):
    logger.debug(filename)
    try:
        grobid = BeautifulSoup(open(filename, 'r'), 'lxml')
    except:
        logger.debug(f'error reading grobid {filename}')
        return {}
    version = '0.0'
    try:
        version = grobid.find('application', {'ident': 'GROBID'}).attrs['version']
    except:
        logger.debug(f'error reading grobid {filename}')
        return {}
    if version not in GROBID_VERSIONS:
        return {}
    return parse_grobid(grobid, version)


def parse_grobid(grobid, version):
    res = {}
    res['grobid_version'] = version
    authors, affiliations = [], []
    source_elt = grobid.find('sourcedesc')
    if source_elt:
        for a in source_elt.find_all('author'):
            author = parse_author(a)
            if author:
                authors.append(author)
                for aff in author.get('affiliations', []):
                    if aff not in affiliations:
                        affiliations.append(aff)
    if authors:
        res['authors'] = authors
    if affiliations:
        res['affiliations'] = affiliations
    keywords, abstract = [], []
    profile_elt = grobid.find('profiledesc')
    if profile_elt:
        keywords_elt = profile_elt.find('keywords')
        if keywords_elt:
            for k in keywords_elt.find_all('term'):
                keywords.append({'keyword': k.get_text().strip()})
        abstract_elt = profile_elt.find('abstract')
        if abstract_elt:
            abstract.append({'abstract': abstract_elt.get_text(' ').strip()})
    if keywords:
        res['keywords'] = keywords
    if abstract:
        res['abstract'] = abstract
        
    acknowledgement_elt = grobid.find('div',{'type': 'acknowledgement'})
    if acknowledgement_elt:
        res['acknowledgments'] = [{'acknowledgments': acknowledgement_elt.get_text(' ').strip()}]
    references=[]
    ref_elt = grobid.find('div', {'type': 'references'})
    if ref_elt:
        for ref in ref_elt.find_all('biblstruct'):
            #reference = {'reference': ref.get_text(' ').replace('\n',' ').strip()}
            reference = {}
            doi_elt = ref.find('idno', {'type': 'DOI'})
            if doi_elt:
                reference['doi'] = doi_elt.get_text(' ').lower().strip()
                references.append(reference)
    if references:
        res['references'] = references
    res['has_availability_statement'] = False
    availability_elt = grobid.find('div', {'type': 'availability'})
    if availability_elt:
        res['has_availability_statement'] = True
    grants = parse_funding(grobid)
    if grants:
        res['has_grant'] = True
        res['grants'] = grants
    return res


def parse_funding(soup):
    funding_map = {}
    for ix, e in enumerate(soup.find_all('org', {'type':'funding'})+soup.find_all('org', {'type':'funded-project'})):
        local_id, grant_id, program = None, None, None
        current_funding={}
        if 'xml:id' in e.attrs:
            local_id = '#'+e.attrs['xml:id']
        try:
            grant_id = e.find('idno', {'type': 'grant-number'}).get_text(' ')
            current_funding['grantid'] = grant_id
        except:
            pass
        try:
            program = e.find('orgname', {'type': 'program'}).get_text(' ')
            current_funding['sub_agency'] = program
        except:
            pass
        if local_id:
            funding_map[local_id] = current_funding
    grants = []
    for ix, e in enumerate(soup.find_all('funder')):
        if 'ref' in e.attrs:
            refs = e.attrs['ref'].split(' ')
            for ref in refs:
                current_funding = funding_map[ref]
        else:
            current_funding = {}
        abbreviated = e.find('orgname', {'type': 'abbreviated'})
        fullName = e.find('orgname', {'type': 'full'})
        if abbreviated:
            current_funding['agency'] = abbreviated.get_text(' ')
            if fullName:
                current_funding['agency_fullName'] = fullName.get_text(' ')
        elif fullName:
            current_funding['agency'] = fullName.get_text(' ')
        if current_funding:
            grants.append(current_funding)
    return grants
    
def parse_author(author):
    res = {}
    first_name_elt = author.find('forename')
    if first_name_elt:
        res['first_name'] = first_name_elt.get_text(' ').strip()
    last_name_elt = author.find('surname')
    if last_name_elt:
        res['last_name'] = last_name_elt.get_text(' ').strip()
    email_elt = author.find('email')
    if email_elt:
        res['email'] = email_elt.get_text(' ').strip()
    orcid_elt = author.find('idno', {'type': 'ORCID'})
    if orcid_elt:
        res['orcid'] = orcid_elt.get_text(' ').strip()
    affiliations = []
    for affiliation_elt in author.find_all('affiliation'):
        aff_name = affiliation_elt.get_text(' ').replace('\n', ' ')
        aff_name = re.sub(' +', ' ', aff_name).strip()
        affiliation = {'name': aff_name}
        orgs = []
        for org_elt in affiliation_elt.find_all('orgname'):
            orgs.append({'name': org_elt.get_text(' ').strip()})
        if orgs:
            affiliation['orgs'] = orgs

        address_elt = affiliation_elt.find('addrline')
        if address_elt:
            affiliation['address'] = address_elt.get_text(' ').strip()

        city_elt = affiliation_elt.find('settlement')
        if city_elt:
            affiliation['city'] = city_elt.get_text(' ').strip()

        zip_elt = affiliation_elt.find('postcode')
        if zip_elt:
            affiliation['zipcode'] = zip_elt.get_text(' ').strip()
        
        country_elt = affiliation_elt.find('country')
        if country_elt:
            affiliation['country'] = country_elt.get_text(' ').strip()


        affiliations.append(affiliation)

    if affiliations:
        res['affiliations'] = affiliations
    return res   
