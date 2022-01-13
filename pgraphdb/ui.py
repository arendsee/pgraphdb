#!/usr/bin/env python3

import click
import textwrap
import json
import sys
import collections
import pgraphdb as cmd
import pgraphdb.cli as cli
from pgraphdb.version import __version__


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

repo_name_arg = click.argument("repo_name")

sparql_file_arg = click.argument("sparql_file", type=click.Path(exists=True))

url_opt = click.option(
    "--url",
    help="The URL where the GraphDB database is hosted",
    default="http://localhost:7200",
)

config_file_arg = click.argument("config_file", type=click.Path(exists=True))

turtle_files_arg = click.argument(
    "turtle_files", type=click.Path(exists=True), nargs=-1
)


def handle_response(response):
    if response.status_code >= 400:
        print(f"ERROR: {response.status_code}: {response.text}", file=sys.stderr)
        return None
    else:
        return response.text


@click.command(name="start")
@click.option("--path", help="The path to the GraphDB bin directory")
def start_cmd(path):
    """
    Start a GraphDB daemon in server mode
    """
    cmd.start_graphdb(path=path)


@click.command(name="make")
@config_file_arg
@url_opt
def make_cmd(config_file, url):
    """
    Create a new data repository within a graphdb database
    """
    print(handle_response(cmd.make_repo(config=config_file, url=url)))


@click.command(name="ls_repo")
@url_opt
def ls_repo_cmd(url):
    """
    List all repositories in the GraphDB database
    """
    print(handle_response(cmd.ls_repo(url=url)))


@click.command(name="rm_repo")
@repo_name_arg
@url_opt
def rm_repo_cmd(repo_name, url):
    """
    Delete a repository in the GraphDB database
    """
    print(handle_response(cmd.rm_repo(repo_name=repo_name, url=url)))


@click.command(name="rm_data")
@repo_name_arg
@turtle_files_arg
@url_opt
def rm_data_cmd(repo_name, turtle_files, url):
    """
    Delete all triples listed in the given turtle files
    """
    cmd.rm_data(url=url, repo_name=repo_name, turtle_files=turtle_files)


@click.command(name="update")
@repo_name_arg
@sparql_file_arg
@url_opt
def update_cmd(repo_name, sparql_file, url):
    """
    Update database through delete or insert SPARQL query
    """
    cmd.update(url=url, repo_name=repo_name, sparql_file=sparql_file)


@click.command(name="ls_files")
@repo_name_arg
@url_opt
def ls_files_cmd(repo_name, url):
    """
    List data files stored on the GraphDB server
    """
    json_str = handle_response(cmd.list_files(url=url, repo_name=repo_name))
    for entry in json.loads(json_str):
        print(entry["name"])


@click.command(name="load")
@repo_name_arg
@turtle_files_arg
@url_opt
def load_cmd(repo_name, turtle_files, url):
    """
    load a given turtle file
    """
    for turtle_file in turtle_files:
        print(
            handle_response(
                cmd.load_data(url=url, repo_name=repo_name, turtle_file=turtle_file)
            )
        )


@click.command(name="query")
@repo_name_arg
@sparql_file_arg
@click.option(
    "--header", is_flag=True, default=False, help="Print the header of column names"
)
@url_opt
def query_cmd(repo_name, sparql_file, header, url):
    """
    Submit a SPARQL query
    """

    def val(xs, field):
        if field in xs:
            return xs[field]["value"]
        else:
            return ""

    results = cmd.sparql_query(url=url, repo_name=repo_name, sparql_file=sparql_file)
    if header:
        print("\t".join(results["head"]["vars"]))
    for row in results["results"]["bindings"]:
        fields = (val(row, field) for field in results["head"]["vars"])
        print("\t".join(fields))


@click.command(name="construct")
@repo_name_arg
@sparql_file_arg
@url_opt
def construct_cmd(repo_name, sparql_file, url):
    """
    Submit a SPARQL CONSTRUCT query and return a Turtle formatted response
    """
    return cmd.sparql_construct(url=url, repo_name=repo_name, sparql_file=sparql_file)


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
def cli():
    pass


cli.add_command(construct_cmd)
cli.add_command(load_cmd)
cli.add_command(ls_files_cmd)
cli.add_command(ls_repo_cmd)
cli.add_command(make_cmd)
cli.add_command(query_cmd)
cli.add_command(rm_data_cmd)
cli.add_command(rm_repo_cmd)
cli.add_command(start_cmd)
cli.add_command(update_cmd)


def main():
    cli()
