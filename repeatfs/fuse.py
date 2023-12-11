#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import fuse as fuse


class Fuse(fuse.Operations):
    """ Implements FUSE API methods """
    def __init__(self, core):
        self.core = core
        self.fuse_error = fuse.FuseOSError

    def fuse_get_context(self):
        """ Provide fuse_get_context info externally """
        return fuse.fuse_get_context()

    def destroy(self, data=None):
        return self.core.unmount(data)

    def access(self, path, mode):
        self.core.log("CALL: access ({0}, {1})".format(path, mode), self.core.LOG_CALL)
        return self.core.routing.s_get_access(path, mode)

    def chmod(self, path, mode):
        self.core.log("CALL: chmod ({0}, {1})".format(path, mode), self.core.LOG_CALL)
        return self.core.routing.s_change_mode(path, mode)

    def chown(self, path, uid, gid):
        self.core.log("CALL: chown ({0}, {1}, {2})".format(path, uid, gid), self.core.LOG_CALL)
        return self.core.routing.s_change_owner(path, uid, gid)

    def getattr(self, path, info=None):
        self.core.log("CALL: getattr ({0}) ({1})".format(path, info.fh if info else None), self.core.LOG_CALL)
        return self.core.routing.s_get_attributes(path, info)

    def opendir(self, path):
        self.core.log("CALL: opendir ({0})".format(path), self.core.LOG_CALL)
        return self.core.routing.s_open_directory(path)

    def readdir(self, path, fh):
        self.core.log("CALL: readdir ({0}, {1})".format(path, fh), self.core.LOG_CALL)
        return self.core.routing.s_get_directory(path, fh)

    def readlink(self, path):
        self.core.log("CALL: readlink ({0})".format(path), self.core.LOG_CALL)
        return self.core.routing.s_get_link(path)

    def mknod(self, path, mode, dev):
        self.core.log("CALL: mknod ({0}, {1}, {2})".format(path, mode, dev), self.core.LOG_CALL)
        return self.core.routing.s_create_node(path, mode, dev)

    def rmdir(self, path):
        self.core.log("CALL: rmdir ({0})".format(path), self.core.LOG_CALL)
        return self.core.routing.s_remove_directory(path)

    def mkdir(self, path, mode):
        self.core.log("CALL: mkdir ({0}, {1})".format(path, mode), self.core.LOG_CALL)
        return self.core.routing.s_create_directory(path, mode)

    def statfs(self, path):
        self.core.log("CALL: statfs ({0})".format(path), self.core.LOG_CALL)
        return self.core.routing.s_fs_stats(path)

    def unlink(self, path):
        self.core.log("CALL: unlink ({0})".format(path), self.core.LOG_CALL)
        return self.core.routing.s_unlink(path)

    def symlink(self, name, target):
        self.core.log("CALL: symlink ({0}, {1})".format(name, target), self.core.LOG_CALL)
        return self.core.routing.s_make_symlink(target, name)

    def link(self, name, target):
        self.core.log("CALL: link ({0}, {1})".format(target, name), self.core.LOG_CALL)
        return self.core.routing.s_make_hardlink(target, name)

    def rename(self, old, new):
        self.core.log("CALL: rename ({0}, {1})".format(old, new), self.core.LOG_CALL)
        return self.core.routing.s_rename(old, new)

    def utimens(self, path, times=None):
        self.core.log("CALL: utimens ({0}, {1})".format(path, times), self.core.LOG_CALL)
        return self.core.routing.s_update_time(path, times)

    def open(self, path, info):
        self.core.log("CALL: open ({0}, {1})".format(path, info.flags), self.core.LOG_CALL)
        return self.core.routing.s_open(path, info)

    def create(self, path, mode, info):
        self.core.log("CALL: create ({0}, {1})".format(path, mode), self.core.LOG_CALL)
        return self.core.routing.s_open(path, info, mode)

    def read(self, path, length, offset, info=None):
        self.core.log("IO: read ({0}, {1}, {2}, {3})".format(path, length, offset, info.fh), self.core.LOG_IO)
        return self.core.routing.s_read(path, length, offset, info)

    def write(self, path, buf, offset, info=None):
        self.core.log("IO: write ({0}, {1}, {2}, {3})".format(path, offset, info.fh, info.writepage), self.core.LOG_IO)
        return self.core.routing.s_write(path, buf, offset, info)

    def truncate(self, path, length, info=None):
        self.core.log("CALL: truncate ({0}, {1})".format(path, length), self.core.LOG_CALL)
        return self.core.routing.s_truncate(path, length, info)

    # TODO: Verify this sync
    def flush(self, path, info):
        self.core.log("CALL: flush ({0}, {1})".format(path, info.fh), self.core.LOG_CALL)
        return self.core.routing.s_sync(path, info)
        # return 0

    def release(self, path, info):
        self.core.log("CALL: release ({0}, {1})".format(path, info.fh), self.core.LOG_CALL)
        return self.core.routing.s_close(info)

    def fsync(self, path, fdatasync, info):
        self.core.log("CALL: fsync ({0}, {1}, {2})".format(path, fdatasync, info.fh), self.core.LOG_CALL)
        return self.core.routing.s_sync(path, info)
