#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2023  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#

import base64
import hashlib
import re
import time
from datetime import datetime
from repeatfs.plugins.plugins import PluginBase
from repeatfs.descriptor_entry import DescriptorEntry

class Plugin(PluginBase):
    """ Snapshot plugin """
    CONFIG_FIELDS = {
        "select": (False, False, "", str, "regex to select which filenames to snapshot")
    }

    def init(self):
        self.intercept = False

        provenance = self.core.provenance
        with provenance.lock:
            cursor = provenance.db_connection.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS snapshot (phost TEXT, path TEXT, screate INT, hash TEXT, data BLOB, PRIMARY KEY(phost, path, screate))")
            provenance.db_connection.commit()

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

    def p_register_open(self, descriptor, pid, read, write, record_file, record_process, update_last):
        """ Record a snapshot of file during open in read mode """
        provenance = self.core.provenance
        desc_entry = DescriptorEntry.get(descriptor)
        path = desc_entry.file_entry.paths["abs_real"]

        # Only take snapshots if: real file, open in read mode, matches selection filter
        if desc_entry.file_entry.derived_source:
            return

        if not self.core.is_flag_read(desc_entry.flags):
            return

        if self.configuration["select"] == "" or not re.search(self.configuration["select"], desc_entry.file_entry.paths["abs_real"]):
            return

        # Check if write has occurred since last snapshot
        with provenance.lock:
            cursor = provenance.db_connection.cursor()

            cursor.execute("SELECT stop FROM write WHERE phost=? AND path=? ORDER BY stop DESC", (provenance.system_name, path))
            write_result = cursor.fetchone()

            cursor.execute("SELECT screate, hash FROM snapshot WHERE phost=? AND path=? ORDER BY screate DESC", (provenance.system_name, path))
            snapshot_result = cursor.fetchone()

            if (snapshot_result is None) or ((write_result is not None) and (write_result["stop"] >= snapshot_result["screate"])):
                self.snapshot(path, None if snapshot_result is None else snapshot_result["hash"])

    def p_register_close(self, descriptor, write_process):
        """ Records a snapshot of file during close of write mode """
        provenance = self.core.provenance
        desc_entry = DescriptorEntry.get(descriptor)
        path = desc_entry.file_entry.paths["abs_real"]

        # Only take snapshots if: real file, close in write mode, matches selection filter
        if desc_entry.file_entry.derived_source:
            return

        if not self.core.is_flag_write(desc_entry.flags):
            return

        if self.configuration["select"] == "" or not re.search(self.configuration["select"], desc_entry.file_entry.paths["abs_real"]):
            return

        with provenance.lock:
            cursor = provenance.db_connection.cursor()
            cursor.execute("SELECT screate, hash FROM snapshot WHERE phost=? AND path=? ORDER BY screate DESC", (provenance.system_name, path))
            snapshot_result = cursor.fetchone()

            self.snapshot(path, None if snapshot_result is None else snapshot_result["hash"])

    def p_build_graph_file(self, file_info):
        """ Add snapshots to file provenance graph """
        provenance = self.core.provenance
        file_info["plugins"][self.name] = { "data": [] }

        with provenance.lock:
            cursor = provenance.db_connection.cursor()
            cursor.execute("SELECT screate, data FROM snapshot WHERE phost=? AND path=? ORDER BY screate", (provenance.system_name, file_info["path"]))

            for row in cursor.fetchall():
                file_info["plugins"][self.name]["data"].append((row["screate"], base64.b64encode(row["data"]).decode("ascii")))

    def p_render_file(self, graph, files):
        """ Build HTML rendering for requested files """
        for file in files:
            # Do not add HTML if no snapshots are found
            graph_file = graph["file"][tuple(file.split("|"))]
            if len(graph_file["plugins"][self.name]["data"]) == 0:
                continue

            html = "<tr><td>Snapshots:</td><td>"

            # Add download link for each available snapshot
            for snapshot in graph_file["plugins"][self.name]["data"]:
                name = graph_file["paths"]["abs_real"].split("/")[-1]
                label = datetime.fromtimestamp(snapshot[0]).strftime("%m/%d/%Y, %H:%M:%S")
                href = "data:application/octet-stream;base64,{}".format(snapshot[1])
                html += "<a download='{}' href='{}'>{}</a><br />".format(name, href, label)

            html += "</td>"

            files[file] = html

    def snapshot(self, path, file_hash):
        """ Take snapshot of requested file """
        provenance = self.core.provenance

        with provenance.lock:
            with open(path, "rb") as handle:
                contents = handle.read()
                md5 = hashlib.md5()
                md5.update(contents)
                new_hash = md5.digest()

            # Do not record snapshot if existing version is identical
            if file_hash == new_hash:
                return

            cursor = provenance.db_connection.cursor()
            cursor.execute("INSERT INTO snapshot VALUES (?, ?, ?, ?, ?)", (provenance.system_name, path, time.time(), new_hash, contents))
            provenance.db_connection.commit()
