from __future__ import annotations
from typing import TextIO, List, Optional, Union, BinaryIO, Iterable
from mypy_extensions import NoReturn

import requests
import subprocess
import sys
import json
import os
from SPARQLWrapper import SPARQLWrapper, JSON, TURTLE # type: ignore
from pgraphdb.util import handle_response, handle_http_response

def openIfNotOpen(thingy : Union[TextIO, str]) -> TextIO:
    if isinstance(thingy, str):
        return open(thingy, "r")
    else:
        return thingy

def openIfNotOpenBinary(thingy : Union[BinaryIO, str]) -> BinaryIO:
    if isinstance(thingy, str):
        return open(thingy, "rb")
    else:
        return thingy
    

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


def turtle_to_deletion_sparql(turtle : Iterable[str]) -> str:
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


def rm_data(url : str, repo_name : str, turtle_file : Union[TextIO, str]) -> SPARQLWrapper.Wrapper.QueryResult:
    graphdb_url = f"{url}/repositories/{repo_name}/statements"
    f = openIfNotOpen(turtle_file)
    turtle_lines = f.readlines()
    sparql_delete = turtle_to_deletion_sparql(turtle_lines)
    sparql = SPARQLWrapper(graphdb_url)
    sparql.method = "POST"
    sparql.queryType = "DELETE"
    sparql.setQuery(sparql_delete)
    sparql_result = sparql.query()

    # check response
    handle_http_response(sparql_result.response)

    return sparql_result


def update(url : str, repo_name : str, sparql_file : Union[TextIO, str]) -> SPARQLWrapper.Wrapper.QueryResult:
    graphdb_url = f"{url}/repositories/{repo_name}/statements"
    sparql = SPARQLWrapper(graphdb_url)

    fh = openIfNotOpen(sparql_file)
    sparql_str = fh.read()
    sparql.setQuery(sparql_str)
    sparql.setReturnFormat(JSON)
    sparql.method = "POST"
    sparql_result = sparql.query()

    # check response
    handle_http_response(sparql_result.response)

    return sparql_result


def sparql_query(url : str, repo_name : str, sparql_file : Union[TextIO, str]) -> SPARQLWrapper.Wrapper.QueryResult:
    graphdb_url = f"{url}/repositories/{repo_name}"
    sparql = SPARQLWrapper(graphdb_url)
    fh = openIfNotOpen(sparql_file)
    sparql_str = fh.read()
    sparql.setQuery(sparql_str)
    sparql.setReturnFormat(JSON)
    sparql_result = sparql.query()

    # check response
    handle_http_response(sparql_result.response, writeResult=False)

    return sparql_result


def sparql_construct(url : str, repo_name : str, sparql_file : Union[str, TextIO]) -> SPARQLWrapper.Wrapper.QueryResult:
    graphdb_url = f"{url}/repositories/{repo_name}"
    sparql = SPARQLWrapper(graphdb_url)
    fh = openIfNotOpen(sparql_file)
    sparql_str = fh.read()
    sparql.setQuery(sparql_str)
    sparql.setReturnFormat(TURTLE)
    query_result = sparql.query()

    # ensure the response is valid
    handle_http_response(query_result.response)

    return query_result


def load_data(url : str, repo_name : str, turtle_file : Union[str, BinaryIO]) -> requests.Response:
    """
    Upload a single turtle file
    """
    headers = {"Content-Type": "text/turtle"}
    rest_url = f"{url}/repositories/{repo_name}/rdf-graphs/service?default"
    data = openIfNotOpenBinary(turtle_file)
    return handle_response(requests.post(rest_url, headers=headers, data=data))


def list_files(url : str, repo_name : str) -> List[str]:
    rest_url = f"{url}/rest/data/import/server/{repo_name}"
    response = handle_response(requests.get(rest_url), writeResult=False)
    files = []
    for entry in json.loads(response.text):
        files.append(entry["name"])
    return files


def start_graphdb(path : Optional[str] = None) -> NoReturn:
    cmd = "graphdb"
    if path:
        cmd = os.path.join(path, "graphdb")
    try:
        subprocess.run([cmd, "-sd"])
        sys.exit(0)
    except FileNotFoundError:
        msg = f"Could not find executable `{cmd}`, please place it in PATH"
        print(msg, file=sys.stderr)
        sys.exit(1)
