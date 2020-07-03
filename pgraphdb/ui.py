#!/usr/bin/env python3

import argparse
import textwrap
import json
import sys
import pgraphdb.commands as cmd


class SubcommandHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """
    Remove the redundant "<subcommand>" string from under the "subcommands:"
    line in the help statement.

    Adapted from Jeppe Ledet-Pedersen on StackOverflow.
    """

    def _format_action(self, action):
        parts = super(argparse.RawDescriptionHelpFormatter, self)._format_action(action)
        if action.nargs == argparse.PARSER:
            parts = "\n".join(parts.split("\n")[1:])
        return parts


cli = argparse.ArgumentParser(
    prog="pgraphdb",
    formatter_class=SubcommandHelpFormatter,
    description="Wrapper around the GraphDB REST interface",
    epilog=textwrap.dedent("ladida back end stuff"),
)
subparsers = cli.add_subparsers(metavar="<subcommand>", title="subcommands")

# subcommand decorator idea adapted from Mike Depalatis blog
def subcommand(args=[], parent=subparsers):
    def decorator(func):
        if func.__doc__:
            help_str = func.__doc__.strip().split("\n")[0]
            desc_str = textwrap.dedent(func.__doc__)
        else:
            help_str = "DOCUMENT ME PLEASE!!!"
            desc_str = None
        cmd_name = args[0]
        parser = parent.add_parser(
            cmd_name,
            description=desc_str,
            help=help_str,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            #  usage=f"pgraphdb {cmd_name} <options>"
        )
        for arg in args[1:]:
            parser.add_argument(*arg[0], **arg[1])
        parser.set_defaults(func=func)

    return decorator


def argument(*name_or_flags, **kwargs):
    return (list(name_or_flags), kwargs)


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
        "make",
        argument("config_file"),
        argument("--url", help="GraphDB URL", default="http://localhost:7200"),
    ]
)
def call_make_repo(args):
    """
    Create a new data repository within a graphdb database
    """
    print(handle_response(cmd.make_repo(config=args.config_file, url=args.url)))


@subcommand(
    ["ls_repo", argument("--url", help="GraphDB URL", default="http://localhost:7200")]
)
def call_ls_repo(args):
    """
    List all repositories in the GraphDB database
    """
    print(handle_response(cmd.ls_repo(url=args.url)))


@subcommand(
    [
        "rm_repo",
        argument("repo_name", help="Repository name"),
        argument("--url", help="GraphDB URL", default="http://localhost:7200"),
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
        argument("repo_name", help="Repository name"),
        argument("turtle_files", help="Turtle files", nargs="*"),
        argument("--url", help="GraphDB URL", default="http://localhost:7200"),
    ]
)
def call_rm_data(args):
    """
    Delete all triples listed in the given turtle files 
    """
    cmd.rm_data(url=url, repo_name=args.repo_name, turtle_files=args.turtle_files)


@subcommand(
    [
        "rm_pattern",
        argument("repo_name", help="Repository name"),
        argument("--url", help="GraphDB URL", default="http://localhost:7200"),
    ]
)
def call_rm_pattern(args):
    """
    Remove triples from store with sparql pattern
    """
    cmd.delete_pattern(
        url=args.url, repo_name=args.repo_name, sparql_file=args.sparql_file
    )


@subcommand(
    [
        "ls_files",
        argument("repo_name", help="Repository name"),
        argument("--url", help="GraphDB URL", default="http://localhost:7200"),
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
        argument("repo_name", help="Repository name"),
        argument("turtle_files", help="Turtle files", nargs="*"),
        argument("--url", help="GraphDB URL", default="http://localhost:7200"),
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


def main():
    args = cli.parse_args()
    if len(vars(args)) == 0:
        cli.print_help()
    else:
        args.func(args)


if __name__ == "__main__":
    main()
