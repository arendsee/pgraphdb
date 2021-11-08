import requests
import subprocess
import sys
import json
import os
from SPARQLWrapper import SPARQLWrapper, JSON, TURTLE


def make_repo(config, url):
    headers = {}
    files = {"config": (config, open(config, "rb"))}
    response = requests.post(f"{url}/rest/repositories", headers=headers, files=files)
    return response


def ls_repo(url):
    headers = {"Accept": "application/json"}
    response = requests.get(f"{url}/repositories", headers=headers)
    return response


def rm_repo(url, repo_name):
    headers = {"Accept": "application/json"}
    response = requests.delete(f"{url}/repositories/{repo_name}", headers=headers)
    return response


def turtle_to_deletion_sparql(turtle):
    """
    Translates a turtle file into a SPARQL statement deleting the triples in the file

    extract prefix statements
    replace '@prefix' with 'prefix', case insenstive
    """

    prefixes = []
    body = []

    for line in turtle:
        line = line.strip()
        if len(line) > 0 and line[0] == "@":
            # translates '@prefix f: <whatever> .' to 'prefix f: <whatever>'
            prefixes.append(line[1:-1])
        else:
            body.append(line)

    prefix_str = "\n".join(prefixes)
    body_str = "\n".join(body)

    sparql = f"{prefix_str}\nDELETE DATA {{\n{body_str}\n}}"

    return sparql


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


def update(url, repo_name, sparql_file):
    graphdb_url = f"{url}/repositories/{repo_name}/statements"
    sparql = SPARQLWrapper(graphdb_url)
    with open(sparql_file, "r") as fh:
        sparql_str = fh.read()
        sparql.setQuery(sparql_str)
        sparql.setReturnFormat(JSON)
        sparql.method = "POST"
        results = sparql.query().convert()
    return results


def sparql_query(url, repo_name, sparql_file):
    graphdb_url = f"{url}/repositories/{repo_name}"
    sparql = SPARQLWrapper(graphdb_url)

    with open(sparql_file, "r") as fh:
        sparql_str = fh.read()
        sparql.setQuery(sparql_str)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
    return results


def sparql_construct(url, repo_name, sparql_file):
    graphdb_url = f"{url}/repositories/{repo_name}"
    sparql = SPARQLWrapper(graphdb_url)

    with open(sparql_file, "r") as fh:
        sparql_str = fh.read()
        sparql.setQuery(sparql_str)
        sparql.setReturnFormat(TURTLE)
        results = sparql.query().convert().decode("utf-8")
    return results


def load_data(url, repo_name, turtle_file):
    """
    Upload a single turtle file
    """
    with open(turtle_file, "rb") as data:
      headers = {"Content-Type": "text/turtle"}
      rest_url = f"{url}/repositories/{repo_name}/rdf-graphs/service?default"
      response = requests.post(rest_url, headers=headers, data=data)
    return response


def list_files(url, repo_name):
    rest_url = f"{url}/rest/data/import/server/{repo_name}"
    response = requests.get(rest_url)
    return response


def start_graphdb(path=None):
    cmd = "graphdb"
    if path:
        cmd = os.path.join(path, "graphdb")
    try:
        subprocess.run([cmd, "-sd"])
    except FileNotFoundError:
        msg = f"Could not find executable `{cmd}`, please place it in PATH"
        print(msg, sys.stderr())
        sys.exit(1)
