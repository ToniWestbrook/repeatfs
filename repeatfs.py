#! /usr/bin/env python3

#
#   RepeatFS: A file system for reproducibility and automation
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception 
#


import argparse
import os
import sys
import repeatfs.client
from repeatfs.core import Core
from repeatfs.configuration import Configuration

# Third party modules
import daemon
from fuse import FUSE


def parse_args():
    """ Parse program arguments """
    # Build default config directory
    config_dir = os.path.join(os.path.expanduser("~"), ".repeatfs")

    # Argument definitions
    arg_opts = {
        "target": {"type": str, "help": "target directory"},
        "mount": {"type": str, "help": "mount directory"},
        "provenance": {"type": str, "help": "provenance file to replicate"},
        "-c": {"dest": "config_path", "default": config_dir, "help": "config and database directory"},
        "-f": {"dest": "foreground", "action": "store_true", "help": "keep daemon in foreground"},
        "-r": {"dest": "replication_path", "type": str, "default": "./", "help": "replication directory"},
        "-m": {"dest": "mount_path", "type": str, "default": "./", "help": "mount directory"},
        "-l": {"dest": "list_cmds", "action": "store_true", "help": "list replication commands"},
        "-e": {"dest": "expand", "type": str, "nargs": "*", "default": [], "help": "expand and recreate specified command IDs"},
        "--stdout": {"type": str, "help": "redirect execution stdout to file"},
        "--stderr": {"type": str, "help": "redirect execution stderr to file"}}

    arg_cmds = {
        "mount": ["target", "mount", "-c", "-f"],
        "generate": ["-c"],
        "replicate": ["provenance", "-r", "-c", "-l", "-e", "--stdout", "--stderr"],
        "shutdown": ["-m", "-c"]}

    # Parse commands arguments
    argparser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    command_help = ["mount\t\tMount a directory under RepeatFS", "replicate\t\tReplicate RepeatFS provenance", "generate\t\tGenerate a config template"]
    argparser.add_argument("command", choices=["mount", "replicate", "shutdown", "generate"], metavar="command", help="\n".join(command_help))
    argparser.add_argument("options", nargs=argparse.REMAINDER, help="Command options")
    arguments = argparser.parse_args()

    # Parse options
    subparser = argparse.ArgumentParser(prog="{} {}".format(os.path.basename(sys.argv[0]), arguments.command))

    for arg in arg_cmds[arguments.command]:
        subparser.add_argument(arg, **arg_opts[arg])

    return subparser.parse_args(arguments.options, arguments)


def main():
    """ Main entry """
    # Parse arguments
    arguments = parse_args()

    # Generate configuration if requested, otherwise read configuration
    if arguments.command == "generate":
        Configuration.write_template(arguments.config_path)
    else:
        configuration = Configuration(None, arguments.config_path)

    # Mount mode
    if arguments.command == "mount":
        # Foreground mode does not detach process and connects stdio
        daemon_opts = {"working_directory": os.getcwd()}
        if arguments.foreground:
            daemon_opts.update({"detach_process": False, "stdin": sys.stdin, "stdout": sys.stdout, "stderr": sys.stderr})

        # Enter daemon context
        with daemon.DaemonContext(**daemon_opts):
            # Setup core
            core = Core(arguments.target, arguments.mount, configuration)

            # Mount
            FUSE(core.fuse, arguments.mount, raw_fi=True, nothreads=False, foreground=True, allow_other=True)

    # Replicate mode
    if arguments.command == "replicate":
        repeatfs.client.replicate(arguments, configuration)

    # Shutdown RepeatFS
    if arguments.command == "shutdown":
        repeatfs.client.shutdown(arguments.mount_path, configuration)


if __name__ == '__main__':
    main()
