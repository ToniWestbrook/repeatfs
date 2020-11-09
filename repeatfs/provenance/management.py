#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import hashlib
import os
import sqlite3
import threading
from functools import partial
from repeatfs.descriptor_entry import DescriptorEntry
from repeatfs.file_entry import FileEntry
from repeatfs.provenance.file_record import FileRecord
from repeatfs.provenance.io_record import IORecord
from repeatfs.provenance.process_record import ProcessRecord
from repeatfs.provenance.graph import Graph
from repeatfs.provenance.render_graphviz import RenderGraphviz
from repeatfs.provenance.render_json import RenderJSON


class Management:
    """ Manage provenance information for IO operations """
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    (OP_IO, OP_ACCESS, OP_CHMOD, OP_CHOWN, OP_ATTR, OP_GETDIR, OP_GETLINK, OP_MKNOD, OP_RMDIR,
     OP_MKDIR, OP_STATS, OP_UNLINK, OP_MKSYM, OP_MKHARD, OP_MOVE, OP_TIME, OP_CD, OP_TRUNCATE) = [2**x for x in range(18)]
    OP_ALL = 2**19 - 1

    def __init__(self, core):
        self.core = core
        self.enable = True
        self.lock = threading.RLock()

        # Setup graphing and renderers
        self.graph = Graph(self)
        self.render_json = RenderJSON(self)
        self.render_graphviz = RenderGraphviz(self)

        # Cache system values
        self.system_name = os.uname().nodename
        self.system_boot = self._get_boot()
        self.hz = os.sysconf(os.sysconf_names['SC_CLK_TCK'])

        # Make DB connection (creates DB if doesn't exist)
        db_path = os.path.join(core.configuration.path, "provenance.db")
        self.db_connection = sqlite3.connect(db_path, check_same_thread=False)
        self._init_db()

        # Register and refresh targets
        self._write_root()

    def _init_db(self):
        """ Create DB structure """
        self.db_keys = dict()
        self.db_keys["mount"] = ("mid",)
        self.db_keys["file"] = ("path", "fcreate")
        self.db_keys["file_last"] = ("path", )
        self.db_keys["process"] = ("phost", "pstart", "pid")
        self.db_keys["read"] = ("phost", "pstart", "pid", "path", "fcreate")
        self.db_keys["write"] = ("phost", "pstart", "pid", "path", "fcreate")

        self.db_vals = dict()
        self.db_vals["mount"] = [("mid", "integer"), ("root", "text"), ("mount", "text")]
        self.db_vals["file"] = [("path", "text"), ("fcreate", "int"), ("type", "text")]
        self.db_vals["file_last"] = [("path", "text"), ("fcreate", "int")]
        self.db_vals["process"] = [("phost", "text"), ("pstart", "int"), ("pid", "int"), ("parent_start", "int"), ("parent_pid", "int"),
                                   ("cmd", "blob"), ("exe", "text"), ("hash", "text"), ("cwd", "text"),
                                   ("tgid_start", "int"), ("tgid", "int"), ("session_start", "int"), ("session_id", "int"), ("env", "text"), 
                                   ("stdin", "text"), ("stdout", "text"), ("stderr", "text"),
                                   ("trunc_stdout", "int"), ("trunc_stderr", "int"), ("mid", "int")]
        self.db_vals["read"] = [("phost", "text"), ("pstart", "int"), ("pid", "int"), ("path", "text"), ("fcreate", "int"),
                                ("start", "int"), ("stop", "int"), ("ops", "int")]
        self.db_vals["write"] = [("phost", "text"), ("pstart", "int"), ("pid", "int"), ("path", "text"), ("fcreate", "int"),
                                 ("start", "int"), ("stop", "int"), ("ops", "int")]

        self.db_ddl = list()
        self.db_ddl.append("CREATE UNIQUE INDEX IF NOT EXISTS mount_rootmount ON mount(root, mount)")
        self.db_ddl.append("CREATE INDEX IF NOT EXISTS process_parent ON process(phost, parent_start, parent_pid)")

        # Create queries
        tables = dict()
        for table in self.db_vals:
            tables[table] = ",".join([" ".join(x) for x in self.db_vals[table]])
            tables[table] += ", PRIMARY KEY ({})".format(",".join(self.db_keys[table]))

        with self.lock:
            self.db_connection.row_factory = sqlite3.Row
            cursor = self.db_connection.cursor()

            # Currently disable synchronous mode for performance
            cursor.execute("PRAGMA synchronous = OFF")

            # Create tables
            for table in tables:
                cursor.execute("CREATE TABLE IF NOT EXISTS {0} ({1})".format(table, tables[table]))

            # Run manually specified DDL queries
            for ddl in self.db_ddl:
                cursor.execute(ddl)

            # Save changes
            self.db_connection.commit()

    def _get_boot(self):
        """ Get system boot time """
        with open("/proc/stat", "r") as handle:
            for line in handle:
                if line.startswith("btime"):
                    return float(line.split(" ")[1].rstrip())

    def _calculate_hash(self, path):
        """ Generate MD5 hash for file """
        md5 = hashlib.md5()

        with open(path, "rb") as handle:
            for chunk in iter(partial(handle.read, 4096), b""):
                md5.update(chunk)

        return md5.hexdigest()

    def _write_root(self):
        """ Write current FS root and refresh IDs """
        with self.lock:
            # Register current FS root if necessary
            cursor = self.db_connection.cursor()
            cursor.execute("INSERT OR IGNORE INTO mount VALUES (NULL, ?, ?)", (self.core.root, self.core.mount))
            self.db_connection.commit()

            # Populate root ID lookup
            cursor.execute("SELECT * FROM mount ORDER BY root=? AND mount=? DESC", (self.core.root, self.core.mount))
            self.mid = cursor.fetchone()["mid"]

    def _get_mount_lookup(self):
        """ Get mount lookup table """
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT * FROM mount")

        return {row["mid"]: (row["root"], row["mount"]) for row in cursor}

    # TODO: write and update_last are separate because of pipes, but pipes should eventually be recorded in last time, and params combined to "create"
    def register_open(self, descriptor, pid=None, read=False, write=False, record_file=True, record_process=True, update_last=False):
        """ Cache process, file, and IO """
        if not self.enable:
            return

        pid = self.core.get_pid(pid)

        # Start new version of file
        if update_last:
            file_entry = DescriptorEntry.get(descriptor).file_entry
            FileRecord.set_last(file_entry, self)

        # Record file
        if record_file:
            FileRecord(descriptor, self)

        # Record IO 
        IORecord(descriptor, pid, self)

        # Record process
        if record_process:
            ProcessRecord.update(pid, self)

        # Record initial read/write
        if read:
            self.register_read(descriptor, op_type=self.OP_IO, pid=pid, update_process=False)

        if write:
            self.register_write(descriptor, op_type=(self.OP_IO | self.OP_TRUNCATE), pid=pid, update_process=False)

    def register_close(self, descriptor, write_process=True):
        """ Write IO records if IO occurred """
        if not self.enable:
            return

        with self.lock:
            # Write file record
            FileRecord.get(descriptor, self).write()

            # Write each process and IO record for this descriptor
            for pid in IORecord.get(descriptor, self):
                io_record = IORecord.get(descriptor, self, pid=pid)
                io_record.write()

                if write_process:
                    ProcessRecord.get(pid, self).write()

        # Remove descriptor from caches
        self.clean_descriptor(descriptor)

    def register_read(self, descriptor, op_type=OP_IO, pid=None, update_process=True, io_time=None):
        """ Update read end time """
        if not self.enable:
            return

        pid = self.core.get_pid(pid)

        # Ensure pid recorded to this descriptor (for descriptors passed to child processes)
        if pid not in IORecord.get(descriptor, self):
            self.register_open(descriptor, pid=pid, record_file=False)

        IORecord.get(descriptor, self, pid=pid).update(IORecord.IO_READ, op_type, io_time=io_time)

        if update_process:
            ProcessRecord.get(pid, self)._update()

    def register_write(self, descriptor, op_type=OP_IO, pid=None, update_process=True, io_time=None):
        """ Update write end time """
        if not self.enable:
            return

        pid = self.core.get_pid(pid)

        # Ensure pid recorded to this descriptor (for descriptors passed to child processes)
        if pid not in IORecord.get(descriptor, self):
            self.register_open(descriptor, pid=pid, record_file=False)

        IORecord.get(descriptor, self, pid=pid).update(IORecord.IO_WRITE, op_type, io_time=io_time)

        # Note truncations in process record for potential file redirection
        if op_type & self.OP_TRUNCATE:
            file_entry = DescriptorEntry.get(descriptor).file_entry
            ProcessRecord.get(pid, self).trunc_history.add(file_entry.paths["abs_mount"])

        if update_process:
            ProcessRecord.get(pid, self)._update()

    def register_op_read(self, file_entry, op_type):
        """ Register an ephemeral read operation """
        if not self.enable:
            return

        with DescriptorEntry(file_entry, None, self.core) as desc_entry:
            self.register_open(desc_entry.id)
            self.register_read(desc_entry.id, op_type)
            self.register_close(desc_entry.id)

    def register_op_write(self, file_entry, op_type, create=False):
        """ Register an ephemeral write operation """
        if not self.enable:
            return

        with DescriptorEntry(file_entry, None, self.core) as desc_entry:
            self.register_open(desc_entry.id, update_last=create)
            self.register_write(desc_entry.id, op_type)
            self.register_close(desc_entry.id)

    def clean_descriptor(self, descriptor):
        """ Remove descriptor based records (file and IO) """
        FileRecord.remove(descriptor, self)
        IORecord.remove(descriptor, self)
