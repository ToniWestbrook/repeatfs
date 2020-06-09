#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import os
from repeatfs.descriptor_entry import DescriptorEntry
from repeatfs.file_entry import FileEntry


class ProcessRecord:
    """ Provides process information """
    UPDATE_THRESHOLD = 20

    _lookup = dict()
    _pipe_cache = dict()

    @classmethod
    def get(cls, pid, management):
        """ Get process entry """
        with management.lock:
            return cls._lookup.get(pid, None)

    @classmethod
    def update(cls, pid, management, ignore_pipes=None):
        """ Create a process entry given a pid, or check for process info updates """
        with management.lock:
            if pid in cls._lookup:
                # Update entry
                process_record = cls._lookup[pid]
                process_record._update(ignore_pipes)
            else:
                # Create and register new entry
                process_record = ProcessRecord(pid, management, ignore_pipes)
                cls._lookup[pid] = process_record

        return process_record

    @classmethod
    def get_stat_info(cls, pid, management):
        """ Get basic process stat info """
        stat_info = {}

        with open("/proc/{0}/stat".format(pid), "r") as handle:
            stat_fields = handle.readline().split(" ")

            # Find end of process name
            for field_mod in range(len(stat_fields) - 1):
                if stat_fields[1 + field_mod].endswith(")"): break

            stat_info["pstart"] = round(int(management.system_boot) + int(stat_fields[21 + field_mod]) / management.hz, 3)
            stat_info["parent_pid"] = int(stat_fields[3 + field_mod])
            stat_info["session_id"] = int(stat_fields[5 + field_mod])

        return stat_info

    def __init__(self, pid, management, ignore_pipes=None):
        """ Process related provenance information """
        self.management = management
        self.pid = pid
        self.cmd = None
        self.cwd_desc = None
        self.trunc_history = set()
        self.count = 0
        self.dirty = True

        # Set remaining fields
        self._update(ignore_pipes=ignore_pipes)

    def _get_cmd(self):
        """ Get current command of process """
        try:
            with open("/proc/{0}/cmdline".format(self.pid), "r") as handle:
                cmd = handle.read()[:-1]

        except PermissionError: cmd = ""

        return cmd

    def _record_pipes(self, ignore_pipes):
        """ Record all pipes connected to standard IO devices """
        for fd in range(3):
            if self.stdio[fd].startswith("pipe:"):
                # Create/retrieve pipe descriptor
                desc_entry = DescriptorEntry.gen_pipe(self.stdio[fd], self.management.core)

                # Manually register pipe provenance (special time=0 indicates pipes)
                self.management.register_open(desc_entry.id, pid=self.pid, record_process=False)
                if fd == 0:
                    self.management.register_read(desc_entry.id, pid=self.pid, io_time=0)
                else:
                    self.management.register_write(desc_entry.id, pid=self.pid, io_time=0)
                self.management.register_close(desc_entry.id, write_process=False)

                # Record in pipe cache
                with self.management.lock:
                    self._pipe_cache.setdefault(self.stdio[fd], set())
                    self._pipe_cache[self.stdio[fd]].add(self.pid)

                if not ignore_pipes:
                    ignore_pipes = set()
                ignore_pipes.add(self.pid)

                # Scan all other processes for this pipe
                for search_pid in os.listdir("/proc"):
                    if not search_pid.isdigit():
                        continue

                    # Skip ignored pipes
                    search_pid = int(search_pid)
                    if search_pid in ignore_pipes:
                        continue

                    for search_fd in range(3):
                        try:
                            search_target = os.readlink("/proc/{0}/fd/{1}".format(search_pid, search_fd))
                        except:
                            continue

                        # If other end of pipe found, register process
                        if search_target == self.stdio[fd]:
                            ProcessRecord.update(search_pid, self.management)

    def _update(self, force=False, ignore_pipes=None):
        """ Update process record with latest information if necessary """
        # Allow repeated checks for updates within threshold
        if not force:
            if self.count > self.UPDATE_THRESHOLD:
                return

            self.count += 1

            if self.cmd == self._get_cmd():
                return

        # Set command
        self.cmd = self._get_cmd()

        # Retrieve stat info
        stat_info = self.get_stat_info(self.pid, self.management)
        for field in stat_info:
            setattr(self, field, stat_info[field])

        # Retrieve thread group ID
        with open("/proc/{0}/status".format(self.pid), "r") as handle:
            for line in handle:
                if line.startswith("Tgid:"):
                    self.tgid = int(line.rstrip().split()[1])
                    break

        # Retrieve parent info
        if self.parent_pid > 0:
            stat_info = self.get_stat_info(self.parent_pid, self.management)
            self.parent_start = stat_info["pstart"]
        else:
            self.parent_start = 0

        # Retrieve tgid leader info
        if self.tgid > 0:
            stat_info = self.get_stat_info(self.tgid, self.management)
            self.tgid_start = stat_info["pstart"]
        else:
            self.tgid_start = 0

        # Retrieve session leader info
        if self.session_id > 0:
            stat_info = self.get_stat_info(self.session_id, self.management)
            self.session_start = stat_info["pstart"]
        else:
            self.session_start = 0

        # Record executable
        try:
            self.exe = os.readlink("/proc/{0}/exe".format(self.pid)) if self.pid > 1 else ""
            try:
                self.md5 = self.management._calculate_hash(self.exe)
            except (PermissionError, FileNotFoundError):
                self.md5 = ""
        except PermissionError:
            self.exe = ""
            self.md5 = ""

        # Record CWD
        try:
            self.cwd = os.readlink("/proc/{0}/cwd".format(self.pid)) if self.pid > 1 else ""
        except PermissionError:
            self.cwd = ""

        # Remove previous CWD descriptor and provenance
        if self.cwd_desc:
            self.management.clean_descriptor(self.cwd_desc.id)
            self.cwd_desc.remove()
            self.cwd_desc = None

        # Update CWD descriptor and provenance (to track CDs)
        if self.cwd:
            cwd_paths = FileEntry.get_paths(self.cwd, self.management.core.root, self.management.core.mount)
            if cwd_paths["orig_type"] == "abs_mount":
                file_entry = FileEntry(cwd_paths["abs_virt"], self.management.core)
                self.cwd_desc = DescriptorEntry(file_entry, None, self.management.core)
                self.management.register_open(self.cwd_desc.id, record_process=False, )
                self.management.register_read(self.cwd_desc.id, self.management.OP_CD, update_process=False, io_time=self.pstart)

        # Record environmental variables
        try:
            self.env = ""
            # with open("/proc/{0}/environ".format(pid), "r") as handle:
            #    self.env = handle.read().rstrip()
        except PermissionError:
            self.env = ""

        # Record stdio connections
        self.stdio = ["", "", ""]
        self.stdio_trunc = [False, False, False]
        for fd in range(3):
            try:
                self.stdio[fd] = os.readlink("/proc/{0}/fd/{1}".format(self.pid, fd)).replace(" (deleted)", "")
                self.stdio_trunc[fd] = (self.stdio[fd] in self.trunc_history)
            except:
                pass
        self.trunc_history.clear()

        # Mark as dirty and add to lookup
        self.dirty = True
        with self.management.lock:
            self._lookup[self.pid] = self

        # Register parent and thread leader process
        if self.parent_pid > 0:
            self.update(self.parent_pid, self.management)

        if self.pid != self.tgid and self.tgid > 0:
            self.update(self.tgid, self.management)

        # Record pipes/processes connected to standard IO devices
        self._record_pipes(ignore_pipes)

        return

    def write(self):
        """ Write process record to the database """
        if not self.dirty:
            return

        with self.management.lock:
            values = (self.management.system_name, self.pstart, self.pid, self.parent_start, self.parent_pid, self.cmd, self.exe,
                      self.md5, self.cwd, self.tgid_start, self.tgid, self.session_start, self.session_id, self.env,
                      self.stdio[0], self.stdio[1], self.stdio[2], self.stdio_trunc[1], self.stdio_trunc[2], self.management.mid)
            cursor = self.management.db_connection.cursor()
            cursor.execute("REPLACE INTO process VALUES ({})".format(",".join(["?"] * 20)), values)
            self.management.db_connection.commit()

        # Write CWD provenance
        if self.cwd_desc:
            self.management.register_close(self.cwd_desc.id, write_process=False)
            self.cwd_desc.remove()
            self.cwd_desc = None

        self.dirty = False

        # Write parent and thread leader entries
        if self.parent_pid > 0:
            self.get(self.parent_pid, self.management).write()

        if self.pid != self.tgid and self.tgid > 0:
            self.get(self.tgid, self.management).write()

        # Write pipe entries
        for fd in range(3):
            if self.stdio[fd].startswith("pipe:"):
                with self.management.lock:
                    # Pipe may be in use twice for this pid (stdout and stderr), check existence and discard
                    if self.stdio[fd] in self._pipe_cache:
                        self._pipe_cache[self.stdio[fd]].discard(self.pid)

                        if len(self._pipe_cache[self.stdio[fd]]) > 0:
                            # Write other end of pipe
                            pipe_pid = next(iter(self._pipe_cache[self.stdio[fd]]))
                            self.get(pipe_pid, self.management).write()
                        else:
                            # Clean up cache once complete
                            del self._pipe_cache[self.stdio[fd]]
