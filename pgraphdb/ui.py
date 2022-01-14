#!/usr/bin/env python3

from __future__ import annotations
from typing import TextIO, List, Optional, BinaryIO
from mypy_extensions import NoReturn

import click
import textwrap
import json
import sys
import collections
import pgraphdb as cmd
from pgraphdb.version import __version__
from pgraphdb.util import handle_response


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

repo_name_arg = click.argument("repo_name")

sparql_file_arg = click.argument("sparql_file", type=click.Path(exists=True))

url_opt = click.option(
    "--url",
    help="The URL where the GraphDB database is hosted",
    default="http://localhost:7200",
)

config_file_arg = click.argument("config_file", type=str)

turtle_files_arg = click.argument(
    "turtle_files", type=click.Path(exists=True), nargs=-1
)


@click.command(name="start")
@click.option("--path", help="The path to the GraphDB bin directory")
def start_cmd(path : Optional[str]) -> NoReturn:
    """
    Start a GraphDB daemon in server mode
    """
    handle_response(cmd.start_graphdb(path=path))
    sys.exit(0)


@click.command(name="make")
@config_file_arg
@url_opt
def make_cmd(config_file : str, url : str) -> NoReturn:
    """
    Create a new data repository within a graphdb database
    """
    handle_response(cmd.make_repo(config=config_file, url=url))
    sys.exit(0)


@click.command(name="ls_repo")
@url_opt
def ls_repo_cmd(url : str) -> NoReturn:
    """
    List all repositories in the GraphDB database
    """
    handle_response(cmd.ls_repo(url=url))
    sys.exit(0)


@click.command(name="rm_repo")
@repo_name_arg
@url_opt
def rm_repo_cmd(repo_name : str, url : str) -> NoReturn:
    """
    Delete a repository in the GraphDB database
    """
    handle_response(cmd.rm_repo(repo_name=repo_name, url=url))
    sys.exit(0)


@click.command(name="rm_data")
@repo_name_arg
@turtle_files_arg
@url_opt
def rm_data_cmd(repo_name : str, turtle_files : List[TextIO], url : str) -> NoReturn:
    """
    Delete all triples listed in the given turtle files
    """
    for turtle_file in turtle_files:
        cmd.rm_data(url=url, repo_name=repo_name, turtle_file=turtle_file)
    sys.exit(0)


@click.command(name="update")
@repo_name_arg
@sparql_file_arg
@url_opt
def update_cmd(repo_name : str, sparql_file : TextIO, url : str) -> NoReturn:
    """
    Update database through delete or insert SPARQL query
    """

    sparql_result = cmd.update(url=url, repo_name=repo_name, sparql_file=sparql_file)

    sys.exit(0)


@click.command(name="ls_files")
@repo_name_arg
@url_opt
def ls_files_cmd(repo_name : str, url : str) -> NoReturn:
    """
    List data files stored on the GraphDB server
    """
    files = cmd.list_files(url=url, repo_name=repo_name)
    for filename in files:
        print(filename)
    sys.exit(0)


@click.command(name="load")
@repo_name_arg
@turtle_files_arg
@url_opt
def load_cmd(repo_name : str, turtle_files : List[BinaryIO], url : str) -> NoReturn:
    """
    load a given turtle file
    """
    for turtle_file in turtle_files:
        cmd.load_data(url=url, repo_name=repo_name, turtle_file=turtle_file)
    sys.exit(0)


@click.command(name="query")
@repo_name_arg
@sparql_file_arg
@click.option(
    "--header", is_flag=True, default=False, help="Print the header of column names"
)
@url_opt
def query_cmd(repo_name : str, sparql_file : TextIO, header : bool, url : str) -> NoReturn:
    """
    Submit a SPARQL query
    """

    def val(xs, field):
        if field in xs:
            return xs[field]["value"]
        else:
            return ""

    sparql_query = cmd.sparql_query(url=url, repo_name=repo_name, sparql_file=sparql_file)

    results = sparql_query.convert()

    if header:
        print("\t".join(results["head"]["vars"]))
    for row in results["results"]["bindings"]:
        fields = (val(row, field) for field in results["head"]["vars"])
        print("\t".join(fields))

    sys.exit(0)


@click.command(name="construct")
@repo_name_arg
@sparql_file_arg
@url_opt
def construct_cmd(repo_name : str, sparql_file : TextIO, url : str) -> NoReturn:
    """
    Submit a SPARQL CONSTRUCT query and return a Turtle formatted response
    """
    sparql_result = cmd.sparql_construct(url=url, repo_name=repo_name, sparql_file=sparql_file)

    print(sparql_result.convert().decode("utf-8"))

    sys.exit(0)


# Thanks to Максим Стукало from https://stackoverflow.com/questions/47972638
# for the solution to getting the subcommands to order non-alphabetically
class OrderedGroup(click.Group):
    def __init__(self, name=None, commands=None, **attrs):
        super(OrderedGroup, self).__init__(name, commands, **attrs)
        self.commands = commands or collections.OrderedDict()

    def list_commands(self, ctx):
        return self.commands


@click.group(
    cls=OrderedGroup,
    help="Wrapper around the GraphDB REST interface",
    context_settings=CONTEXT_SETTINGS,
)
@click.version_option(__version__, "-v", "--version", message=__version__)
def cli_cmd():
    pass


cli_cmd.add_command(make_cmd)
cli_cmd.add_command(start_cmd)
cli_cmd.add_command(load_cmd)
cli_cmd.add_command(query_cmd)
cli_cmd.add_command(update_cmd)
cli_cmd.add_command(construct_cmd)
cli_cmd.add_command(ls_files_cmd)
cli_cmd.add_command(ls_repo_cmd)
cli_cmd.add_command(rm_data_cmd)
cli_cmd.add_command(rm_repo_cmd)


def main():
    cli_cmd()
