import io
import logging
import logging.config

import click
from rich import console as rich_console

from . import sync
import pkg_resources


class RichGroup(click.Group):

    def format_help(self, ctx, formatter):
        commands = []

        for subcommand in self.list_commands(ctx):
            cmd = self.get_command(ctx, subcommand)

            if cmd is None:
                continue
            if cmd.hidden:
                continue

            params = f'\[{", ".join([x.name for x in cmd.params])}]' if cmd.params else ""
            commands.append((subcommand, cmd, params))

        sio = io.StringIO()
        console = rich_console.Console(file=sio, force_terminal=True)

        console.print(f"""
         █████╗ ████████╗         ██╗      ██████╗  ██████╗ 
        ██╔══██╗╚══██╔══╝         ╚██╗     ██╔══██╗██╔════╝ 
        ███████║   ██║       █████╗╚██╗    ██████╔╝██║  ███╗
        ██╔══██║   ██║       ╚════╝██╔╝    ██╔═══╝ ██║   ██║
        ██║  ██║   ██║            ██╔╝     ██║     ╚██████╔╝
        ╚═╝  ╚═╝   ╚═╝            ╚═╝      ╚═╝      ╚═════╝ 
        """)
        console.print("[bold]Commands:")

        for cmd in commands:
            console.print(" ",
                          f'[bold underline]{cmd[0]}',
                          f'{cmd[2]}',
                          f'\n  [italic]{cmd[1].__doc__.strip()} \n' if cmd[1].__doc__ else "\n"
                          )
        formatter.write(sio.getvalue())


def setup_logging():
    logging.config.fileConfig(pkg_resources.resource_filename(__name__, './logging.conf'), disable_existing_loggers=False)
    root_logger = logging.getLogger()
    root_logger.setLevel("INFO")
    root_logger.info(f'Logger created')


@click.option('--config', required=True, type=str)
def one_time_sync(config: str):
    """
    Runs a one-time sync from the Airtable base to the database schema.
    """
    sync.Sync(config_path=config, perpetual=False).run()


@click.option('--config', required=True, type=str)
def perpetual_sync(config: str):
    """
    Syncs the Airtable base to the database schema, then continues to listen for changes and sync them.
    """
    sync.Sync(config_path=config, perpetual=True).run()


@click.group(cls=RichGroup)
def cli():
    setup_logging()


cli.add_command(click.command()(one_time_sync))
cli.add_command(click.command()(perpetual_sync))


def main():
    cli()
