import re


def parse_api_v1_url(url: str) -> dict:
    """
    Parse the url for the api v1
    """
    # Resource download
    api_v1_resource_format = re.compile(r"^\/api\/v1\/datasets\/r\/(?P<resource_id>\w+)\/?$")
    data = re.search(api_v1_resource_format, url)
    if data:
        return {'access': 'api_v1', 'category': 'resource', **data.groupdict()}
    
    # Resource get
    api_v1_resource_get_format = re.compile(r"^\/api\/v1\/datasets\/(?P<dataset_id_or_slug>\w+)\/resources\/(?P<resource_id>\w+)\/?$")
    data = re.search(api_v1_resource_get_format, url)
    if data:
        return {'access': 'api_v1', 'category': 'resource', **data.groupdict()}

    # Dataset get
    api_v1_dataset_format = re.compile(r"^\/api\/v1\/datasets\/(?P<dataset_id_or_slug>\w+)\/?$")
    data = re.search(api_v1_dataset_format, url)
    if data:
        return {'access': 'api_v1', 'category': 'dataset', **data.groupdict()}

    # Reuse get
    api_v1_reuse_format = re.compile(r"^\/api\/v1\/reuses\/(?P<reuse_id_or_slug>\w+)\/?$")
    data = re.search(api_v1_reuse_format, url)
    if data:
        return {'access': 'api_v1', 'category': 'reuse', **data.groupdict()}

    # Organization get
    api_v1_organization_format = re.compile(r"^\/api\/v1\/organizations\/(?P<organization_id_or_slug>\w+)\/?$")
    data = re.search(api_v1_organization_format, url)
    if data:
        return {'access': 'api_v1', 'category': 'organization', **data.groupdict()}
    return None


def parse_api_v2_url(url: str) -> dict:
    # Resource
    api_v2_resource_format = re.compile(r"^\/api\/v2\/datasets\/resources\/(?P<resource_id>\w+)\/?$")
    data = re.search(api_v2_resource_format, url)
    if data:
        return {'access': 'api_v2', 'category': 'resource', **data.groupdict()}

    # Dataset
    api_v2_dataset_format = re.compile(r"^\/api\/v2\/datasets\/(?P<dataset_id_or_slug>\w+)\/?$")
    data = re.search(api_v2_dataset_format, url)
    if data:
        return {'access': 'api_v2', 'category': 'dataset', **data.groupdict()}
    # NB: Reuses and organizations are not supported yet according to the API documentation
    return None


def parse_site(url: str) -> dict:
    pass


def parse_url(url: str) -> dict:
    if url.startswith('/api/v1/'):
        return parse_api_v1_url(url)
    if url.startswith('/api/v2/'):
        return parse_api_v2_url(url)
    if len(url.split('/')[1]) == 2:
        language = url.split('/')[1]
        return parse_site(url)