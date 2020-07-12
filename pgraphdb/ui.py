#!/usr/bin/env python3

import argparse
import textwrap
import json
import sys
import pgraphdb as cmd
import pgraphdb.util.cli as cli


parser = argparse.ArgumentParser(
    prog="pgraphdb",
    formatter_class=cli.SubcommandHelpFormatter,
    description="Wrapper around the GraphDB REST interface",
    epilog=textwrap.dedent("ladida back end stuff"),
)
subparsers = parser.add_subparsers(metavar="<subcommand>", title="subcommands")
subcommand = cli.subcommand_maker(subparsers)

def handle_response(response):
    if response.status_code >= 400:
        print(f"ERROR: {response.status_code}: {response.text}", file=sys.stderr)
        return None
    else:
        return response.text


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


@subcommand(
    [
        "start",
        cli.argument("config_file"),
        cli.argument("--path", help="The path to the GraphDB bin directory"),
    ]
)
def call_start_graphdb(args):
    """
    Start a GraphDB daemon in server mode
    """
    start_graphdb(path=args.path)


@subcommand(
    [
        "make",
        cli.argument("config_file"),
        cli.argument("--url", help="GraphDB URL", default="http://localhost:7200"),
    ]
)
def call_make_repo(args):
    """
    Create a new data repository within a graphdb database
    """
    print(handle_response(cmd.make_repo(config=args.config_file, url=args.url)))


@subcommand(
    ["ls_repo", cli.argument("--url", help="GraphDB URL", default="http://localhost:7200")]
)
def call_ls_repo(args):
    """
    List all repositories in the GraphDB database
    """
    print(handle_response(cmd.ls_repo(url=args.url)))


@subcommand(
    [
        "rm_repo",
        cli.argument("repo_name", help="Repository name"),
        cli.argument("--url", help="GraphDB URL", default="http://localhost:7200"),
    ]
)
def call_rm_repo(args):
    """
    Delete a repository in the GraphDB database
    """
    print(handle_response(cmd.rm_repo(repo_name=args.repo_name, url=args.url)))


@subcommand(
    [
        "rm_data",
        cli.argument("repo_name", help="Repository name"),
        cli.argument("turtle_files", help="Turtle files", nargs="*"),
        cli.argument("--url", help="GraphDB URL", default="http://localhost:7200"),
    ]
)
def call_rm_data(args):
    """
    Delete all triples listed in the given turtle files 
    """
    cmd.rm_data(url=url, repo_name=args.repo_name, turtle_files=args.turtle_files)


@subcommand(
    [
        "update",
        cli.argument("repo_name", help="Repository name"),
        cli.argument("sparql_file", help="SPARQL file with DELETE or INSERT statement"),
        cli.argument("--url", help="GraphDB URL", default="http://localhost:7200"),
    ]
)
def call_update(args):
    """
    Update database through delete or insert SPARQL query 
    """
    cmd.update(
        url=args.url, repo_name=args.repo_name, sparql_file=args.sparql_file
    )


@subcommand(
    [
        "ls_files",
        cli.argument("repo_name", help="Repository name"),
        cli.argument("--url", help="GraphDB URL", default="http://localhost:7200"),
    ]
)
def call_ls_files(args):
    """
    List data files stored on the GraphDB server
    """
    json_str = handle_response(cmd.list_files(url=args.url, repo_name=args.repo_name))
    for entry in json.loads(json_str):
        print(entry["name"])


@subcommand(
    [
        "load",
        cli.argument("repo_name", help="Repository name"),
        cli.argument("turtle_files", help="Turtle files", nargs="*"),
        cli.argument("--url", help="GraphDB URL", default="http://localhost:7200"),
    ]
)
def call_load_data(args):
    """
    load a given turtle file
    """
    print(
        handle_response(
            cmd.load_data(
                url=args.url, repo_name=args.repo_name, turtle_files=args.turtle_files
            )
        )
    )


@subcommand(
    [
        "query",
        cli.argument("repo_name", help="Repository name"),
        cli.argument("sparql_file", help="The SPARQL query file"),
        cli.argument(
            "--header",
            action="store_true",
            default=False,
            help="Print the header of column names",
        ),
        cli.argument("--url", help="GraphDB URL", default="http://localhost:7200"),
    ]
)
def call_sparql_query(args):
    """
    Submit a SPARQL query
    """

    def val(xs, field):
        if field in xs:
            return xs[field]["value"]
        else:
            return ""

    results = cmd.sparql_query(
        url=args.url, repo_name=args.repo_name, sparql_file=args.sparql_file
    )
    if args.header:
        print("\t".join(results["head"]["vars"]))
    for row in results["results"]["bindings"]:
        fields = (val(row, field) for field in results["head"]["vars"])
        print("\t".join(fields))


def main():
    args = parser.parse_args()
    if len(vars(args)) == 0:
        parser.print_help()
    else:
        args.func(args)


if __name__ == "__main__":
    main()
