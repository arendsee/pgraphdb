#!/usr/bin/env python3

"""
control a graphdb database.

Usage:
  c79 list [--url=<url>] 
  c79 make [<config_file>] [--url=<url>]
  c79 delete_repository <repo_name> [--url=<url>]
  c79 upload_data <repo_name> [--url=<url>] <turtle_files>...
  c79 delete_data <repo_name> [--url=<url>] <turtle_files>...
  c79 delete_pattern <repo_name> [--url=<url>] <sparql_file>
  c79 list_files <repo_name> [--url=<url>]

Options:
  -h --help       Show this screen.
  --url <url>     The graphdb base URL [default:"http://localhost:7200"]
"""

import os
import sys
import signal
import requests
import json
from docopt import docopt


def make_repo(config, url):
    headers = {}
    files = {"config": (config, open(config, "rb"))}
    response = requests.post(f"{url}/rest/repositories", headers=headers, files=files)
    return response


def list_repo(url):
    headers = {"Accept": "application/json"}
    response = requests.get(f"{url}/rest/repositories", headers=headers)
    return response


def delete_repo(url, repo_name):
    headers = {"Accept": "application/json"}
    response = requests.delete(f"{url}/rest/repositories/{repo_name}", headers=headers)
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


def delete_data(url, repo_name, turtle_files):
    from SPARQLWrapper import SPARQLWrapper

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


def upload_data(url, repo_name, turtle_files):
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


def delete_pattern(url, repo_name, sparql_file):
    raise NotImplemented


def handle_response(response):
    if response.status_code >= 400:
        print(f"ERROR: {response.status_code}: {response.text}", file=sys.stderr)
        return None
    else:
        return response.text


if __name__ == "__main__":
    if os.name is "posix":
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    args = docopt(__doc__, version="c79 0.1.0")

    if args["--url"]:
        url = args["--url"]
    else:
        url = "http://localhost:7200"

    if args["list"]:
        print(handle_response(list_repo(url=url)))

    if args["make"]:
        print(handle_response(make_repo(args["<config_file>"], url=url)))

    if args["delete_repository"]:
        print(handle_response(delete_repo(repo_name=args["<repo_name>"], url=url)))

    if args["delete_data"]:
        delete_data(
            url=url, repo_name=args["<repo_name>"], turtle_files=args["<turtle_files>"]
        )

    if args["upload_data"]:
        print(
            handle_response(
                upload_data(
                    url=url,
                    repo_name=args["<repo_name>"],
                    turtle_files=args["<turtle_files>"],
                )
            )
        )

    if args["list_files"]:
        json_str = handle_response(list_files(url=url, repo_name=args["<repo_name>"]))
        for entry in json.loads(json_str):
            print(entry["name"])

    if args["delete_pattern"]:
        delete_pattern(
            url=url, repo_name=args["<repo_name>"], sparql_file=args["<sparql_file>"]
        )
