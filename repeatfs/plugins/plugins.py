#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2023  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#

import importlib
import os
import sys


class PluginBase:
    """ Provides base functionality for all plugins"""
    CONFIG_FIELDS = {}

    @classmethod
    def avail_plugins(cls):
        """ Get available plugins """
        plugins = []

        for entry in os.scandir(os.path.dirname(os.path.realpath(__file__))):
            if entry.is_file() and entry.name.endswith(".py") and entry.name != "plugins.py":
                plugins.append(entry.name[:-3])

        return plugins

    @classmethod
    def config_fields(cls):
        """ Get available plugin config fields """
        options = {}
        plugins = cls.avail_plugins()

        for plugin in plugins:
            module = importlib.import_module("repeatfs.plugins.{}".format(plugin.strip()))
            name = module.__name__.split(".")[-1]
            options.update({ f"{name}.{key}": value for key, value in getattr(module, "Plugin").CONFIG_FIELDS.items() })

        return options

    @classmethod
    def load_plugins(cls, core):
        """ Load specified plugins """
        plugin_insts = []

        # Skip if no plugins defined
        if core.configuration.values["plugins"].strip() == "":
            return plugin_insts

        # Instantiate each plugin
        for pidx, plugin in enumerate(core.configuration.values["plugins"].split(",")):
            try:
                module = importlib.import_module("repeatfs.plugins.{}".format(plugin.strip()))
                plugin_insts.append(getattr(module, "Plugin")(core, pidx))
                plugin_insts[-1].init()
            except:
                core.log("Error: Could not load plugin '{}'".format(plugin.strip()), core.LOG_OUTPUT)

        return plugin_insts

    def __init__(self, core, pidx):
        self.core = core
        self.pidx = pidx
        self.name = self.__class__.__module__.split(".")[-1]
        self.configuration = {}
        self.intercept = False

        # Register system call forwarding
        self.syscalls = {}
        for name in dir(core):
            method = getattr(core, name)
            if callable(method) and name in dir(self):
                self.syscalls[method] = getattr(self, name)

        # Setup plugin specific configuration fields
        for field, value in core.configuration.values.items():
            field_parts = field.split(".")

            if len(field_parts) > 1 and field_parts[0] == self.name:
                self.configuration[field.split(".")[-1]] = value

    def mount(self):
        pass

    def unmount(self):
        pass

    def get_access(self, path, mode):
        pass

    def change_mode(self, path, mode):
        pass

    def change_owner(self, path, uid, gid):
        pass

    def get_attributes(self, path, info):
        pass

    def open_directory(self, path):
        pass

    def get_directory(self, path, fh):
        pass

    def get_link(self, path):
        pass

    def create_node(self, path, mode, dev):
        pass

    def remove_directory(self, path):
        pass

    def create_directory(self, path, mode):
        pass

    def fs_stats(self, path):
        pass

    def unlink(self, path):
        pass

    def make_symlink(self, src, link):
        pass

    def make_hardlink(self, src, link):
        pass

    def rename(self, old, new):
        pass

    def update_time(self, path, times):
        pass

    def open(self, path, info, mode):
        pass

    def read(self, path, length, offset, info):
        pass

    def write(self, path, buf, offset, info):
        pass

    def truncate(self, path, length, info):
        pass

    def close(self, info):
        pass

    def sync(self, path, info):
        pass

