import re





def parse_api_v1_url(url: str) -> dict:
    """
    Parse the url for the api v1
    """
    # Resource download
    api_v1_resource_format = re.compile(r"^\/api\/1\/datasets\/r\/(?P<resource_id>(\w|\-)+)\/?$")
    data = re.search(api_v1_resource_format, url)
    if data:
        return {'access': 'api_v1', 'category': 'resource', **data.groupdict()}
    
    # Resource get
    api_v1_resource_get_format = re.compile(r"^\/api\/1\/datasets\/(?P<dataset_id_or_slug>(\w|\-)+)\/resources\/(?P<resource_id>(\w|\-)+)\/?$")
    data = re.search(api_v1_resource_get_format, url)
    if data:
        return {'access': 'api_v1', 'category': 'resource', **data.groupdict()}

    # Dataset get
    api_v1_dataset_format = re.compile(r"^\/api\/1\/datasets\/(?P<dataset_id_or_slug>(\w|\-)+)\/?$")
    data = re.search(api_v1_dataset_format, url)
    if data:
        return {'access': 'api_v1', 'category': 'dataset', **data.groupdict()}

    # Reuse get
    api_v1_reuse_format = re.compile(r"^\/api\/1\/reuses\/(?P<reuse_id_or_slug>(\w|\-)+)\/?$")
    data = re.search(api_v1_reuse_format, url)
    if data:
        return {'access': 'api_v1', 'category': 'reuse', **data.groupdict()}

    # Organization get
    api_v1_organization_format = re.compile(r"^\/api\/1\/organizations\/(?P<organization_id_or_slug>(\w|\-)+)\/?$")
    data = re.search(api_v1_organization_format, url)
    if data:
        return {'access': 'api_v1', 'category': 'organization', **data.groupdict()}
    return {}


def parse_api_v2_url(url: str) -> dict:
    # Resource
    api_v2_resource_format = re.compile(r"^\/api\/2\/datasets\/resources\/(?P<resource_id>(\w|\-)+)\/?$")
    data = re.search(api_v2_resource_format, url)
    if data:
        return {'access': 'api_v2', 'category': 'resource', **data.groupdict()}

    # Dataset
    api_v2_dataset_format = re.compile(r"^\/api\/2\/datasets\/(?P<dataset_id_or_slug>(\w|\-)+)\/?$")
    data = re.search(api_v2_dataset_format, url)
    if data:
        return {'access': 'api_v2', 'category': 'dataset', **data.groupdict()}
    # NB: Reuses and organizations are not supported yet according to the API documentation
    return {}


def parse_site(url: str) -> dict:
    site_resource_format = re.compile(r"^\/(?P<language>\w+)\/datasets\/r\/(?P<resource_id>(\w|\-)+)\/?$")
    data = re.search(site_resource_format, url)
    if data:
        return {'access': 'site', 'category': 'resource', **data.groupdict()}

    site_dataset_format = re.compile(r"^\/(?P<language>\w+)\/datasets\/(?P<dataset_id_or_slug>(\w|\-)+)\/?$")
    data = re.search(site_dataset_format, url)
    if data:
        return {'access': 'site', 'category': 'dataset', **data.groupdict()}

    site_organization_format = re.compile(r"^\/(?P<language>\w+)\/organizations\/(?P<organization_id_or_slug>(\w|\-)+)\/?$")
    data = re.search(site_organization_format, url)
    if data:
        return {'access': 'site', 'category': 'organization', **data.groupdict()}

    site_reuse_format = re.compile(r"^\/(?P<language>\w+)\/reuses\/(?P<reuse_id_or_slug>(\w|\-)+)\/?$")
    data = re.search(site_reuse_format, url)
    if data:
        return {'access': 'site', 'category': 'reuse', **data.groupdict()}
    return {}



def parse_url(url: str) -> dict:
    if url.startswith('/api/1/'):
        return parse_api_v1_url(url)
    if url.startswith('/api/2/'):
        return parse_api_v2_url(url)
    if len(url.split('/')[1]) == 2:
        language = url.split('/')[1]
        return parse_site(url)
    return {}
