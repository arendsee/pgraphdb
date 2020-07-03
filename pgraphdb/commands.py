import requests
from SPARQLWrapper import SPARQLWrapper


def make_repo(config, url):
    headers = {}
    files = {"config": (config, open(config, "rb"))}
    response = requests.post(f"{url}/rest/repositories", headers=headers, files=files)
    return response


def ls_repo(url):
    headers = {"Accept": "application/json"}
    response = requests.get(f"{url}/rest/repositories", headers=headers)
    return response


def rm_repo(url, repo_name):
    headers = {"Accept": "application/json"}
    response = requests.delete(f"{url}/rest/repositories/{repo_name}", headers=headers)
    return response


def rm_data(url, repo_name, turtle_files):
    graphdb_url = f"{url}/repositories/{repo_name}/statements"
    for turtle in turtle_files:
        with open(turtle, "r") as f:
            turtle_lines = f.readlines()
            sparql_delete = turtle_to_deletion_sparql(turtle_lines)
            sparql = SPARQLWrapper(graphdb_url)
            sparql.method = "POST"
            sparql.queryType = "DELETE"
            sparql.setQuery(sparql_delete)
            sparql.query()


def load_data(url, repo_name, turtle_files):
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    data = dict(
        fileNames=turtle_files,
        importSettings=dict(
            parserSettings=dict(
                # If True, filenames such as "cdc_cvv.ttl" will fail since '_' is an
                # invalid character in a URI
                verifyURISyntax=False
            )
        ),
    )

    rest_url = f"{url}/rest/data/import/server/{repo_name}"
    response = requests.post(rest_url, headers=headers, data=json.dumps(data))
    return response


def list_files(url, repo_name):
    rest_url = f"{url}/rest/data/import/server/{repo_name}"
    response = requests.get(rest_url)
    return response


def rm_pattern(url, repo_name, sparql_file):
    raise NotImplemented
