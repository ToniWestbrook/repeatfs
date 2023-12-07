#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2023  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#

from repeatfs.plugins.plugins import PluginBase

class Plugin(PluginBase):
    """ Distributed file system plugin """
    CONFIG_FIELDS = {
        "port": (False, False, "50000", int, "server port")
    }

    def init(self):
        self.intercept = True

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

