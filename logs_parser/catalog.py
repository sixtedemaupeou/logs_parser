import os
import requests

# REUSE_CATALOG_URL = 'https://www.data.gouv.fr/fr/datasets/r/970aafa0-3778-4d8b-b9d1-de937525e379'
# DATASET_CATALOG_URL = 'https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3'
# RESOURCE_CATALOG_URL = 'https://www.data.gouv.fr/fr/datasets/r/4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d'
# ORGANIZATION_CATALOG_URL = 'https://www.data.gouv.fr/fr/datasets/r/b7bbfedc-2448-4135-a6c7-104548d396e7'

REUSE_CATALOG_URL = 'https://www.demo.data.gouv.fr/fr/datasets/r/970aafa0-3778-4d8b-b9d1-de937525e379'
DATASET_CATALOG_URL = 'https://www.demo.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3'
RESOURCE_CATALOG_URL = 'https://www.demo.data.gouv.fr/fr/datasets/r/4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d'
ORGANIZATION_CATALOG_URL = 'https://www.demo.data.gouv.fr/fr/datasets/r/b7bbfedc-2448-4135-a6c7-104548d396e7'

CATALOG_URLS = {
    'reuses': REUSE_CATALOG_URL,
    'resources': RESOURCE_CATALOG_URL,
    'organizations': ORGANIZATION_CATALOG_URL,
    'datasets': DATASET_CATALOG_URL,
}
CATALOG_DIR = 'catalogs'



def download_catalogs():
    os.makedirs(CATALOG_DIR, exist_ok=True)
    # Download the catalogs
    for category, catalog_url in CATALOG_URLS.items():
        catalog_path = os.path.join(CATALOG_DIR, category + '.csv')
        print('Downloading catalog:', catalog_url)
        r = requests.get(catalog_url)
        with open(catalog_path, 'w') as f:
            f.write(r.text)


download_catalogs()
