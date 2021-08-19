import argparse
import textwrap


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


# subcommand decorator idea adapted from Mike Depalatis blog
def subcommand_maker(subparsers):
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

    return subcommand


def argument(*name_or_flags, **kwargs):
    return (list(name_or_flags), kwargs)
