#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2024  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#

import base64
import json
from kafka import KafkaProducer
from repeatfs.plugins.plugins import PluginBase

class Plugin(PluginBase):
    """ Kafka plugin """
    CONFIG_FIELDS = {
        "server": (False, False, "localhost:9092", str, "bootstrap server address"),
        "topic": (False, False, "repeatfs", str, "kakfa topic"),
    }

    def init(self):
        self.intercept = True

    def mount(self):
        self.producer = KafkaProducer(bootstrap_servers=self.configuration["server"])

    def unmount(self):
        self.producer.close()

    def s_get_access(self, path, mode):
        result = self.core.routing.s_get_access(path, mode, self.pidx + 1)
        self.publish(self.s_get_access, (path, mode), result)
        return result

    def s_change_mode(self, path, mode):
        result = self.core.routing.s_change_mode(path, mode, self.pidx + 1)
        self.publish(self.s_change_mode, (path, mode), result)
        return result

    def s_change_owner(self, path, uid, gid):
        result = self.core.routing.s_change_owner(path, uid, gid, self.pidx + 1)
        self.publish(self.s_change_owner, (path, uid, gid), result)
        return result

    def s_get_attributes(self, path, info):
        result = self.core.routing.s_get_attributes(path, info, self.pidx + 1)
        self.publish(self.s_get_attributes, (path, None), result)
        return result

    def s_open_directory(self, path):
        result = self.core.routing.s_open_directory(path, self.pidx + 1)
        self.publish(self.s_open_directory, (path,), result)
        return result

    def s_get_directory(self, path, fh):
        result = list(self.core.routing.s_get_directory(path, fh, self.pidx + 1))
        self.publish(self.s_get_directory, (path, fh), result)
        return result

    def s_get_link(self, path):
        result = self.core.routing.s_get_link(path, self.pidx + 1)
        self.publish(self.s_get_link, (path,), result)
        return result

    def s_create_node(self, path, mode, dev):
        result = self.core.routing.s_create_node(path, mode, dev, self.pidx + 1)
        self.publish(self.s_create_node, (path, mode, dev), result)
        return result

    def s_remove_directory(self, path):
        result = self.core.routing.s_remove_directory(path, self.pidx + 1)
        self.publish(self.s_remove_directory, (path,), result)
        return result

    def s_create_directory(self, path, mode):
        result = self.core.routing.s_create_directory(path, mode, self.pidx + 1)
        self.publish(self.s_create_directory, (path, mode), result)
        return result

    def s_fs_stats(self, path):
        result = self.core.routing.s_fs_stats(path, self.pidx + 1)
        self.publish(self.s_fs_stats, (path,), result)
        return result

    def s_unlink(self, path):
        result = self.core.routing.s_unlink(path, self.pidx + 1)
        self.publish(self.s_unlink, (path,), result)
        return result

    def s_make_symlink(self, src, link):
        result = self.core.routing.s_make_symlink(src, link, self.pidx + 1)
        self.publish(self.s_make_symlink, (src, link), result)
        return result

    def s_make_hardlink(self, src, link):
        result = self.core.routing.s_make_hardlink(src, link, self.pidx + 1)
        self.publish(self.s_make_hardlink, (src, link), result)
        return result

    def s_rename(self, old, new):
        result = self.core.routing.s_rename(old, new, self.pidx + 1)
        self.publish(self.s_rename, (old, new), result)
        return result

    def s_update_time(self, path, times):
        result = self.core.routing.s_update_time(path, times, self.pidx + 1)
        self.publish(self.s_update_time, (path, times), result)
        return result

    def s_open(self, path, info, mode):
        result = self.core.routing.s_open(path, info, mode, self.pidx + 1)
        self.publish(self.s_open, (path, None, mode), result)
        return result

    def s_read(self, path, length, offset, info):
        result = self.core.routing.s_read(path, length, offset, info, self.pidx + 1)
        self.publish(self.s_read, (path, length, offset, None), base64.b64encode(result).decode("ascii"))
        return result

    def s_write(self, path, buf, offset, info):
        result = self.core.routing.s_write(path, buf, offset, info, self.pidx + 1)
        self.publish(self.s_write, (path, base64.b64encode(buf).decode("ascii"), offset, None), result)
        return result

    def s_truncate(self, path, length, info):
        result = self.core.routing.s_truncate(path, length, info, self.pidx + 1)
        self.publish(self.s_truncate, (path, length, None), result)
        return result

    def s_close(self, info):
        result = self.core.routing.s_close(info, self.pidx + 1)
        self.publish(self.s_close, (None,), result)
        return result

    def s_sync(self, path, info):
        result = self.core.routing.s_sync(path, info, self.pidx + 1)
        self.publish(self.s_sync, (path, None), result)
        return result

    """  Publish operation to Kafka topic """
    def publish(self, func, vals, result=None):
        message = { "op": func.__name__, "args": { }, "result": result }

        for idx, val in enumerate(vals):
            message["args"][func.__code__.co_varnames[idx + 1]] = val

        self.producer.send(self.configuration["topic"], json.dumps(message).encode("utf8"))
