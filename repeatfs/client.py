#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import os
import sys
from repeatfs.api import API
from repeatfs.core import Core


def get_api_path(path, configuration):
    """ Build API path and check for validity """
    api_path = os.path.join(path, configuration.values["api"])
    return api_path if os.path.exists(api_path) else None


def replicate(arguments, configuration):
    """ Initiate replication of provenance """
    api_path = get_api_path(arguments.replication_path, configuration)
    if not api_path:
        Core.log("Error: Replication path must be within RepeatFS mount", Core.LOG_OUTPUT)
        return

    # Read provenance data
    with open(arguments.provenance, "r") as handle:
        prov_json = handle.read()

    # List commands mode
    if arguments.list_cmds:
        for result in API.request(api_path, command="replicate", extended={"action": "list_cmds", "provenance": prov_json, "expand": arguments.expand}):
            if result["message"]:
                Core.log(result["message"], Core.LOG_OUTPUT)

        return

    # Setup streams
    stdout = open(arguments.stdout, "w") if arguments.stdout else sys.stdout
    stderr = open(arguments.stderr, "w") if arguments.stderr else sys.stderr

    # Send replicate request to API
    for result in API.request(api_path, command="replicate", extended={"action": "replicate", "provenance": prov_json, "expand": arguments.expand}):
        if result["message"]:
            Core.log("[{}] {}".format(result["status"], result["message"]), Core.LOG_OUTPUT)

        if "stdout" in result:
            Core.log(result["stdout"], Core.LOG_OUTPUT, end="", file=stdout)

        if "stderr" in result:
            Core.log(result["stderr"], Core.LOG_OUTPUT, end="", file=stderr)

    # Close streams
    if stdout != sys.stdout:
        stdout.close()

    if stderr != sys.stderr:
        stderr.close()

def shutdown(path, configuration):
    """ Initiate RepeatFS shutdown """
    api_path = get_api_path(path, configuration)
    if not api_path:
        Core.log("Error: Command must target valid RepeatFS mount", Core.LOG_OUTPUT)
        return

    next(API.request(api_path, command="shutdown"))
