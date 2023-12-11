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

    def s_get_access(self, path, mode):
        pass

    def s_change_mode(self, path, mode):
        pass

    def s_change_owner(self, path, uid, gid):
        pass

    def s_get_attributes(self, path, info):
        pass

    def s_open_directory(self, path):
        pass

    def s_get_directory(self, path, fh):
        pass

    def s_get_link(self, path):
        pass

    def s_create_node(self, path, mode, dev):
        pass

    def s_remove_directory(self, path):
        pass

    def s_create_directory(self, path, mode):
        pass

    def s_fs_stats(self, path):
        pass

    def s_unlink(self, path):
        pass

    def s_make_symlink(self, src, link):
        pass

    def s_make_hardlink(self, src, link):
        pass

    def s_rename(self, old, new):
        pass

    def s_update_time(self, path, times):
        pass

    def s_open(self, path, info, mode):
        pass

    def s_read(self, path, length, offset, info):
        pass

    def s_write(self, path, buf, offset, info):
        pass

    def s_truncate(self, path, length, info):
        pass

    def s_close(self, info):
        pass

    def s_sync(self, path, info):
        pass

