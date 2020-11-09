#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import os
import re
import sys


class Configuration:
    """ Storages global and per-file configurations """

    # Casts must be defined before CONFIG_FIELDS
    def cast_bool(val):
        """ Boolean cast  """
        return val.lower() == "true"

    def cast_path(val):
        """ Path cast """
        return os.path.expanduser(val)

    def cast_list(val):
        """ List cast (if applicable) """
        if isinstance(val, list):
            return val
        else:
            return [val]

    FIELD_MODE = 0
    FIELD_REQ = 1
    FIELD_DEF = 2
    FIELD_TYPE = 3

    TEMPLATE_EXAMPLES = ("# FASTQ -> FASTA\n"
                         "[entry]\n"
                         "match=\.fastq$\n"
                         "ext=.fasta\n"
                         "cmd=seqtk seq -A {input}\n")

    CONFIG_FILE = "repeatfs.conf"

    CONFIG_FIELDS = {
        "suffix": (False, False, "+", str, "virtual directory suffix"),
        "hidden": (False, False, "False", cast_bool, "prepend '.' to virtual directory paths"),
        "invisible": (False, False, "True", cast_bool, "hide virtual directories from directory listing"),
        "block_size": (False, False, "1048576", int, "filesystem block size"),
        "store_size": (False, False, "1073741824", int, "total filestore size"),
        "read_timeout": (False, False, "1.0", float, "read timeout (seconds)"),
        "cache_path": (False, False, "/tmp/repeatfs.cache", cast_path, "cache path"),
        "io_epsilon": (False, False, "7.0", float, "provenance IO is considered simultaneous within this epsilon (seconds)"),
        "api": (False, False, ".repeatfs-api", str, "file for RepeatFS API and control"),
        "api_size": (False, False, "1048576", int, "reported size of RepeatFS API and control"),
        "match": (True, True, None, str, ""),
        "ext": (True, True, None, str, ""),
        "cmd": (True, True, None, str, ""),
        "output": (True, False, "stdout", str, ""),
        "append": (True, False, None, str, ""),
        "disk_cache": (True, False, "True", cast_bool, ""),
        "init_size": (True, False, "0", int, ""),
        "internal": (True, False, None, cast_list, "")}

    SYSTEM_ENTRIES = [
            {"match": ".*", "ext": ".provenance.html", "cmd": "cat {temp}", "internal": ["self.cache_entry.core.provenance.render_graphviz.virt_render", {"collapse": False}], "init_size": "10485760"},
            {"match": ".*", "ext": ".provenance.json", "cmd": "cat {temp}", "internal": ["self.cache_entry.core.provenance.render_json.virt_render"]}]

    @classmethod
    def write_template(cls, path):
        """ Write template configuration to config directory """
        config_path = os.path.join(path, cls.CONFIG_FILE)

        # Do not overwrite an existing file
        if os.path.isfile(config_path):
            print("Error: configuration file already exists")
            return

        # Create directory and template
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, cls.CONFIG_FILE), "w") as handle:
            handle.write("# Configuration Template\n\n")

            for field in cls.CONFIG_FIELDS:
                if not cls.CONFIG_FIELDS[field][0]:
                    handle.write("## {0}\n#{1}={2}\n\n".format(cls.CONFIG_FIELDS[field][4], field, cls.CONFIG_FIELDS[field][2]))

            handle.write("\n{0}".format(cls.TEMPLATE_EXAMPLES))

        print("Configuration created: {0}".format(config_path))

    def __init__(self, core, path):
        self.values = dict()
        self.actions = dict()
        self.core = core
        self.path = path

        if not self.read_config(os.path.join(path, self.CONFIG_FILE)):
            sys.exit(1)

    def _add_entry(self, entry_mode, values):
        # Check for required fields and set default values
        for field in self.CONFIG_FIELDS:
            if self.CONFIG_FIELDS[field][self.FIELD_MODE] == entry_mode:
                if self.CONFIG_FIELDS[field][self.FIELD_REQ] and field not in values:
                    return "required field '{0}' missing".format(field)

                if not self.CONFIG_FIELDS[field][self.FIELD_REQ] and field not in values:
                    values[field] = self.CONFIG_FIELDS[field][self.FIELD_DEF]

        # Check for conflicts
        if "output" in values and "cmd" in values:
            if values["output"] != "file" and "{output}" in values["cmd"]:
                return "'{output}' command variable only valid for 'file' output"

        # Make active
        for field in values:
            value = None
            if values[field] is not None:
                value = Configuration.CONFIG_FIELDS[field][Configuration.FIELD_TYPE](values[field])

            if entry_mode:
                entry_key = (values['match'], values['ext'])
                self.actions.setdefault(entry_key, dict())
                self.actions[entry_key][field] = value
            else:
                self.values[field] = Configuration.CONFIG_FIELDS[field][Configuration.FIELD_TYPE](values[field])

        return None

    # Read configuration
    def read_config(self, path):
        values = {}

        invalid = None

        try:
            with open(path, "r") as handle:
                entry_mode = False
                line_num = 0

                for line in handle:
                    line = line.rstrip()
                    line_num += 1

                    # Skip comments
                    if re.match("^[ \t]*(#.*)*$", line): continue

                    # Process entry header
                    if re.match("^[ \t]*\[entry\][ \t]*(#.*)*$", line):
                        # Complete the previous entry
                        invalid = self._add_entry(entry_mode, values)
                        if invalid: break

                        # Start new entry
                        values = {}
                        entry_mode = True
                        continue

                    # Check field validity
                    match = re.search("^[ \t]*([^= \t]+)[ \t]*=[ \t]*([^#]+)(#.*)*$", line)

                    if not match or match.groups(1)[0] not in Configuration.CONFIG_FIELDS:
                        invalid = "Invalid line in configuration"
                        break
                    if entry_mode and not Configuration.CONFIG_FIELDS[match.groups(1)[0]][Configuration.FIELD_MODE]:
                        invalid = "Global attribute in entry section"
                        break
                    if not entry_mode and Configuration.CONFIG_FIELDS[match.groups(1)[0]][Configuration.FIELD_MODE]:
                        invalid = "Entry attribute in global section"
                        break

                    # Save field value
                    values[match.groups(1)[0]] = match.groups(1)[1]

            # Add final entry
            if not invalid:
                invalid = self._add_entry(entry_mode, values)

            # Check for errors
            if invalid:
                print("Configuration error: {0} ({1})".format(invalid, line_num))
                return False

            # Add system entries
            for entry in Configuration.SYSTEM_ENTRIES:
                self._add_entry(True, entry)

            return True

        except IOError:
            print("Error: configuration file not found at location `{}`.  Please run 'repeatfs generate' to create a new configuration.".format(path))
            return False
