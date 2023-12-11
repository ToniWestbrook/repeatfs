#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2023  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#

class Routing:
    def __init__(self, core):
        self.core = core

    def mount(self):
        for plugin in self.core.plugins:
            plugin.syscalls[func](arg1)

        return func(arg1)

    def unmount(self):
        for plugin in self.core.plugins:
            plugin.syscalls[func](arg1)

        return func(arg1)

    def s_get_access(self, path, mode, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_get_access(path, mode)
            if plugin.intercept:
                return retval

        return self.core.get_access(path, mode)

    def s_change_mode(self, path, mode, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_change_mode(path, mode)
            if plugin.intercept:
                return retval

        return self.core.change_mode(path, mode)

    def s_change_owner(self, path, uid, gid, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_change_owner(path, uid, gid)
            if plugin.intercept:
                return retval

        return self.core.change_owner(path, uid, gid)

    def s_get_attributes(self, path, info, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_get_attributes(path, info)
            if plugin.intercept:
                return retval

        return self.core.get_attributes(path, info)

    def s_open_directory(self, path, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_open_directory(path)
            if plugin.intercept:
                return retval

        return self.core.open_directory(path)

    def s_get_directory(self, path, fh, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_get_directory(path, fh)
            if plugin.intercept:
                return retval

        return self.core.get_directory(path, fh)

    def s_get_link(self, path, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_get_link(path)
            if plugin.intercept:
                return retval

        return self.core.get_link(path)

    def s_create_node(self, path, mode, dev, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_create_node(path, mode, dev)
            if plugin.intercept:
                return retval

        return self.core.create_node(path, mode, dev)

    def s_remove_directory(self, path, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_remove_directory(path)
            if plugin.intercept:
                return retval

        return self.core.remove_directory(path)

    def s_create_directory(self, path, mode, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_create_directory(path, mode)
            if plugin.intercept:
                return retval

        return self.core.create_directory(path, mode)

    def s_fs_stats(self, path, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_fs_stats(path)
            if plugin.intercept:
                return retval

        return self.core.fs_stats(path)

    def s_unlink(self, path, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_unlink(path)
            if plugin.intercept:
                return retval

        return self.core.unlink(path)

    def s_make_symlink(self, src, link, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_make_symlink(src, link)
            if plugin.intercept:
                return retval

        return self.core.make_symlink(src, link)

    def s_make_hardlink(self, src, link, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_make_hardlink(src, link)
            if plugin.intercept:
                return retval

        return self.core.make_hardlink(src, link)

    def s_rename(self, old, new, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_rename(old, new)
            if plugin.intercept:
                return retval

        return self.core.rename(old, new)

    def s_update_time(self, path, times, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_update_time(path, times)
            if plugin.intercept:
                return retval

        return self.core.update_time(path, times)

    def s_open(self, path, info, mode=None, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_open(path, info, mode)
            if plugin.intercept:
                return retval

        return self.core.open(path, info, mode)

    def s_read(self, path, length, offset, info, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_read(path, length, offset, info)
            if plugin.intercept:
                return retval

        return self.core.read(path, length, offset, info)

    def s_write(self, path, buf, offset, info, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_write(path, buf, offset, info)
            if plugin.intercept:
                return retval

        return self.core.write(path, buf, offset, info)

    def s_truncate(self, path, length, info, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_truncate(path, length, info)
            if plugin.intercept:
                return retval

        return self.core.truncate(path, length, info)

    def s_close(self, info, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_close(info)
            if plugin.intercept:
                return retval

        return self.core.close(info)

    def s_sync(self, path, info, pidx=0):
        for plugin in self.core.plugins[pidx:]:
            retval = plugin.s_sync(path, info)
            if plugin.intercept:
                return retval

        return self.core.sync(path, info)

    def p_register_open(self, descriptor, pid=None, read=False, write=False, record_file=True, record_process=True, update_last=False):
        for plugin in self.core.plugins:
            plugin.p_register_open(descriptor, pid=pid, read=read, write=write, record_file=record_file, record_process=record_process, update_last=update_last)

        self.core.provenance.register_open(descriptor, pid=pid, read=read, write=write, record_file=record_file, record_process=record_process, update_last=update_last)

    def p_register_close(self, descriptor, write_process=True):
        for plugin in self.core.plugins:
            plugin.p_register_close(descriptor, write_process=write_process)

        self.core.provenance.register_close(descriptor, write_process=write_process)

    def p_build_graph_file(self, file_info):
        for plugin in self.core.plugins:
            plugin.p_build_graph_file(file_info)

    def p_render_file(self, graph, files):
        for plugin in self.core.plugins:
            plugin.p_render_file(graph, files)
