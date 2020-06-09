#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import ast
import hashlib
import json
import os
import sqlite3
import threading
import time
from collections import deque
from functools import partial
from repeatfs.descriptor_entry import DescriptorEntry
from repeatfs.file_entry import FileEntry

# Third party modules
import pygraphviz


class GraphObj:
    @classmethod
    def serialize(cls, obj):
        return obj.__dict__

    def __init__(self, entry):
        for key, val in dict(entry).items():
            setattr(self, key, val)


class GraphFile(GraphObj):
    def __str__(self):
        return "_".join([str(x) for x in [self.path, self.fcreate]])


class GraphProcess(GraphObj):
    def __str__(self):
        return "_".join([str(x) for x in [self.phost, self.pstart, self.pid]])


class GraphIO(GraphObj):
    def __str__(self):
        return "_".join([str(x) for x in [self.path, self.fcreate, self.phost, self.pstart, self.pid]])


class Provenance:
    """ Manage provenance information for IO operations """
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    (OP_IO, OP_ACCESS, OP_CHMOD, OP_CHOWN, OP_ATTR, OP_GETDIR, OP_GETLINK, OP_MKNOD, OP_RMDIR,
     OP_MKDIR, OP_STATS, OP_UNLINK, OP_MKSYM, OP_MKHARD, OP_MOVE, OP_TIME, OP_CD) = [2**x for x in range(17)]

    def __init__(self, core):
        self.core = core
        self.lock = threading.RLock()

        # Temporarily cached dynamic values
        self.pid_cache = dict()  # pid -> p_start
        self.read_cache = dict()  # {descriptor: {# (pid, p_start, path) -> (start, stop) TODO update these
        self.write_cache = dict()  # {descriptor: {# (pid, p_start, path) -> (start, stop)

        # Permanently cached static values
        self.system_name = os.uname().nodename
        self.system_boot = self._get_boot()
        self.hz = os.sysconf(os.sysconf_names['SC_CLK_TCK'])

        # Make DB connection (creates DB if doesn't exist)
        db_path = os.path.join(core.configuration.path, "provenance.db")
        self.db_connection = sqlite3.connect(db_path, check_same_thread=False)
        self._init_db()

        # Register and refresh targets
        self._register_root()

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
        self.db_vals["file"] = [("path", "text"), ("fcreate", "int"), ("type", "text"), ("size", "int"), ("md5", "text"), ("mid", "int")]
        self.db_vals["file_last"] = [("path", "text"), ("fcreate", "int")]
        self.db_vals["process"] = [("phost", "text"), ("pstart", "int"), ("pid", "int"), ("parent_start", "int"), ("parent_pid", "int"),
                                   ("cmd", "text"), ("exe", "text"), ("hash", "text"), ("cwd", "text"),
                                   ("session", "int"), ("env", "text"), ("mid", "int")]
        self.db_vals["read"] = [("phost", "text"), ("pstart", "int"), ("pid", "int"), ("path", "text"), ("fcreate", "int"),
                                ("start", "int"), ("stop", "int"), ("ops", "int")]
        self.db_vals["write"] = [("phost", "text"), ("pstart", "int"), ("pid", "int"), ("path", "text"), ("fcreate", "int"),
                                 ("start", "int"), ("stop", "int"), ("ops", "int")]

        self.db_ddl = list()
        self.db_ddl.append("CREATE UNIQUE INDEX IF NOT EXISTS mount_rootmount ON mount(root, mount)")

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

    def _register_root(self):
        """ Register current FS root and refresh IDs """
        with self.lock:
            # Register current FS root if necessary
            cursor = self.db_connection.cursor()
            cursor.execute("INSERT OR IGNORE INTO mount VALUES (NULL, ?, ?)", (self.core.root, self.core.mount))
            self.db_connection.commit()

            # Populate root ID lookup
            cursor.execute("SELECT * FROM mount ORDER BY root=? AND mount=? DESC", (self.core.root, self.core.mount))
            self.mid = cursor.fetchone()["mid"]

    def _get_boot(self):
        """ Get system boot time """
        with open("/proc/stat", "r") as handle:
            for line in handle:
                if line.startswith("btime"):
                    return float(line.split(" ")[1].rstrip())

    def _gen_pid_index(self, descriptor, pid=None):
        """ Get a (pid, p_start) tuple for current process, register if not cached """
        if not pid:
            pid = self.core.fuse.fuse_get_context()[2]

        with self.lock:
            # Register pid
            self._register_process(pid)

            # Register IO start
            if (descriptor not in self.read_cache) or (self.pid_cache[pid] not in self.read_cache[descriptor]):
                self._register_io_start(descriptor, self.pid_cache[pid])

        return self.pid_cache[pid]

    def _update_cmd(self, pid_index):
        """ Update the cmd associated with process """
        try:
            with open("/proc/{0}/cmdline".format(pid_index[1]), "r") as handle:
                cmd = ' '.join(handle.read().split("\0")[:-1])

        except PermissionError: cmd = ""

        values = (cmd, self.system_name) + pid_index
        cursor = self.db_connection.cursor()
        cursor.execute("UPDATE process SET cmd=? WHERE phost=? AND pstart=? AND pid=?", values)
        self.db_connection.commit()

    def _calculate_hash(self, path):
        """ Generate MD5 hash for file """
        md5 = hashlib.md5()

        with open(path, "rb") as handle:
            for chunk in iter(partial(handle.read, 4096), b""):
                md5.update(chunk)

        return md5.hexdigest()

    def _register_io_start(self, descriptor, pid_index, io_time=None):
        """ Initialize IO records between process and file """
        if io_time is None:
            io_time = time.time()

        with self.lock:
            self.read_cache.setdefault(descriptor, dict())
            self.read_cache[descriptor].setdefault(pid_index, [io_time, None, True, 0])
            self.write_cache.setdefault(descriptor, dict())
            self.write_cache[descriptor].setdefault(pid_index, [io_time, None, True, 0])

    def _register_read(self, descriptor, op_type, pid=None, read_time=None):
        """ Update read end time """
        if read_time is None:
            read_time = time.time()

        with self.lock:
            pid_index = self._gen_pid_index(descriptor, pid)
            self.read_cache[descriptor][pid_index][1] = read_time

            # First read following initialization should record shell cmd changes (redirect)
            if self.read_cache[descriptor][pid_index][2]:
                self._update_cmd(pid_index)
                self.read_cache[descriptor][pid_index][2] = False

            # Add operation type
            self.read_cache[descriptor][pid_index][3] |= op_type

    def _register_write(self, descriptor, op_type, pid=None, write_time=None, truncate=False):
        """ Update write end time """
        if write_time is None:
            write_time = time.time()

        with self.lock:
            pid_index = self._gen_pid_index(descriptor, pid)
            self.write_cache[descriptor][pid_index][1] = write_time

            # First non-truncate write following initialization should record shell cmd changes (redirect)
            if self.write_cache[descriptor][pid_index][2] and (not truncate):
                self._update_cmd(pid_index)
                self.write_cache[descriptor][pid_index][2] = False

            # Add operation type
            self.write_cache[descriptor][pid_index][3] |= op_type

    def _register_process(self, pid):
        """ Lookup process information and save to DB """
        # Do not reregister a process
        if pid in self.pid_cache: return

        self.core.log("Provenance: registering process start {0}".format(pid), self.core.LOG_DEBUG)

        # Retrieve stat info
        with open("/proc/{0}/stat".format(pid), "r") as handle:
            stat_info = handle.readline().split(" ")

            # Find end of process name
            for field_mod in range(len(stat_info) - 1):
                if stat_info[1 + field_mod].endswith(")"): break

            pstart = int(self.system_boot) + int(stat_info[21 + field_mod]) / self.hz
            parent_pid = int(stat_info[3 + field_mod])
            session = int(stat_info[5 + field_mod])

        # Retrieve parent info
        if parent_pid > 0:
            with open("/proc/{0}/stat".format(parent_pid), "r") as handle:
                stat_info = handle.readline().split(" ")

                # Find end of process name
                for field_mod in range(len(stat_info) - 1):
                    if stat_info[1 + field_mod].endswith(")"): break

                parent_start = int(self.system_boot) + int(stat_info[21 + field_mod]) / self.hz
        else:
            parent_start = 0

        # Retrieve exe, exe hash, and CWD
        try:
            exe = os.readlink("/proc/{0}/exe".format(pid)) if pid > 1 else ""
            try:
                md5 = self._calculate_hash(exe)
            except (PermissionError, FileNotFoundError):
                md5 = ""
        except PermissionError:
            exe = ""
            md5 = ""

        try:
            cwd = os.readlink("/proc/{0}/cwd".format(pid)) if pid > 1 else ""
        except PermissionError: cwd = ""

        try:
            env = ""
            # with open("/proc/{0}/environ".format(pid), "r") as handle:
            #    env = handle.read().rstrip()
        except PermissionError: env = ""

        # Save pid to cache
        self.pid_cache[pid] = (pstart, pid)

        # Register standard stream pipes associated with process
        self._register_pipes(pid)

        # Save to DB
        with self.lock:
            values = (self.system_name, pstart, pid, parent_start, parent_pid, '', exe, md5, cwd, session, env, self.mid)
            cursor = self.db_connection.cursor()
            cursor.execute("INSERT OR IGNORE INTO process VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values)
            self.db_connection.commit()

        # Update cmd
        self._update_cmd((pstart, pid))

        # Register CWD as read operation
        cwd_paths = self._get_cwd_paths(cwd, self.core.root, self.core.mount)
        if cwd_paths:
            file_entry = FileEntry(cwd_paths["abs_virt"], self.core)
            with DescriptorEntry(file_entry, None, self.core) as desc_entry:
                self.register_op_read(desc_entry.id, self.OP_CD)

        # Register parent process (if current is not init/systemd)
        if parent_pid > 0:
            self._register_process(parent_pid)

    def _register_file(self, descriptor, force=False, create_time=None):
        """ Save file information to DB """
        if create_time is None:
            create_time = time.time()
        desc_entry = DescriptorEntry.get(descriptor)

        self.core.log("Provenance: registering file {} force {}".format(desc_entry.file_entry.paths["abs_real"], force), self.core.LOG_DEBUG)

        with self.lock:
            cursor = self.db_connection.cursor()

            # If forcing new record, delete existing last entry
            if force:
                values = (desc_entry.file_entry.paths["abs_real"], )
                cursor.execute("DELETE FROM file_last WHERE path = ?", values)

            # Update last seen version of file
            values = (desc_entry.file_entry.paths["abs_real"], create_time)
            cursor.execute("INSERT OR IGNORE INTO file_last VALUES (?, ?)", values)

            # Add file if applicable
            values = (desc_entry.file_entry.file_type, "", "", self.mid, desc_entry.file_entry.paths["abs_real"])
            cursor.execute("INSERT OR IGNORE INTO file (path, fcreate, type, size, md5, mid) "
                           "SELECT path, fcreate, ?, ?, ?, ? FROM file_last WHERE path = ?", values)

            self.db_connection.commit()

    def _register_pipes(self, pid):
        """ Detect and register all standard stream pipes associated with process """
        for handle in range(3):
            try:
                target = os.readlink("/proc/{0}/fd/{1}".format(pid, handle))
            except:
                continue

            if target.startswith("pipe:["):
                self.core.log("Provenance: registering pipe {0} for pid {1}".format(target, pid), self.core.LOG_DEBUG)

                # Register pipe descriptor if none exists yet
                desc_entry = DescriptorEntry.gen_pipe(target, self.core)

                # Register file, associated process, and IO start
                self._gen_pid_index(desc_entry.id, pid)

                # Register appropriate direction of IO
                if handle == 0:
                    self._register_read(desc_entry.id, self.OP_IO, pid=pid, read_time=0)
                else:
                    self._register_write(desc_entry.id, self.OP_IO, pid=pid, write_time=0)

                # Scan all other processes for the pipe
                for match_pid in os.listdir("/proc"):
                    if not match_pid.isdigit(): continue

                    # Check for matching pipe
                    for match_handle in range(3):
                        try:
                            match_target = os.readlink("/proc/{0}/fd/{1}".format(match_pid, match_handle))
                        except:
                            continue

                        if match_target == target:
                            self._gen_pid_index(desc_entry.id, int(match_pid))

                # Finalize the IO
                self.register_close(desc_entry.id)

    def _add_file(self, file_index, graph, info, target):
        """ Add file node to graph """
        file_render = ",".join(map(str, file_index))
        node_color = "green:white" if target else "blue:white"
        graph.add_node(file_index, label=file_index[0], color="black", fillcolor=node_color, style="filled", gradientangle="270", shape="note", URL="javascript:activate_file('{0}');".format(file_render))
        info["file"][file_render] = file_index + ("test", )

    def _add_process(self, entry, graph, info, render):
        """ Add process node to graph """
        process = (entry["phost"], entry["pstart"], entry["pid"])
        parent = [entry["phost"], entry["parent_start"], entry["parent_pid"]]
        process_render = ",".join(map(str, process))
        parent_render = ",".join(map(str, parent))

        # Update info if present
        if len(entry) > 3:
            # Create placeholders for process and parent (to hold children before declared)
            info["process"].setdefault(process_render, [""] * 9 + [[]])
            info["process"].setdefault(parent_render, [""] * 9 + [[]])

            # Update process in info table
            process_start = time.strftime(self.DATE_FORMAT, time.localtime(entry["pstart"]))
            env_render = "<br>".join(sorted(entry["env"].split("\0")[:-1]))
            info["process"][process_render][:9] = [
                entry["phost"], process_start, entry["pid"], parent, entry["cmd"], entry["exe"], entry["hash"], entry["cwd"], env_render]

            # Add process to parent TODO: change this over to a list instead of repeatedly converting
            if list(process) not in info["process"][parent_render][9]:
                info["process"][parent_render][9].append(list(process))

        # If render requested, add to graph
        if render:
            label = info["process"][process_render][4]
            if len(label) > 50:
                label = label[:50] + "..."

            graph.add_node(process, label=label, color="black", fillcolor="red:white", style="filled", gradientangle="270", shape="component", URL="javascript:activate_process('{0}');".format(process_render))

    def _add_io(self, entry, graph, info, write):
        """ Add IO edge to graph """
        process = (entry["phost"], entry["pstart"], entry["pid"])
        process_render = ",".join(map(str, process))
        file_index = (entry["path"], entry["fcreate"])
        file_render = ",".join(map(str, file_index))
        start = time.strftime(self.DATE_FORMAT, time.localtime(entry["start"]))
        stop = time.strftime(self.DATE_FORMAT, time.localtime(entry["stop"]))

        # Direct the graph according to read/write
        if write:
            graph.add_edge(process, file_index, edgeURL="javascript:activate_io('{0}-{1}');".format(process_render, file_render))
            info["io"]["{0}-{1}".format(process_render, file_render)] = ("write", start, stop)
        else:
            graph.add_edge(file_index, process, edgeURL="javascript:activate_io('{0}-{1}');".format(file_render, process_render))
            info["io"]["{0}-{1}".format(file_render, process_render)] = ("read", start, stop)

    def _add_fork(self, parent, child_process, graph, info):
        """ Add forked process IO """
        parent_process = (parent["phost"], parent["pstart"], parent["pid"])
        parent_render = ",".join(map(str, parent_process))
        child_render = ",".join(map(str, child_process))
        fork = time.strftime(self.DATE_FORMAT, time.localtime(child_process[1]))

        # Direct the graph from parent to child
        graph.add_edge(parent_process, child_process, edgeURL="javascript:activate_io('{0}-{1}');".format(parent_render, child_render))
        info["io"]["{0}-{1}".format(parent_render, child_render)] = ("write", fork, fork)

    def _update_processes(self, process, process_tree, info):
        """ Add relatives of process into process tree """
        lineage = list()
        statement = "SELECT * FROM process WHERE AND phost = ? AND pstart = ? AND pid = ?"
        cursor = self.db_connection.cursor()
        cur_process = process

        # Trace lineage
        while cur_process[2] > 0:
            # Retrieve row for current process
            cursor.execute(statement, cur_process)
            row = cursor.fetchone()
            lineage.append((cur_process, row))

            # Iterate to parent
            cur_process = (row["phost"], row["parent_start"], row["parent_pid"])

        # Update tree with lineage, format {proc1: (in_graph, [child_proc1, child_proc2]), proc2, child_proc1, grandchild_proc1, ...}
        parent = (0, 0, 0)
        process_tree.setdefault(parent, (False, set()))

        for cur_process in lineage[::-1]:
            # Add process to process tree
            process_tree.setdefault(cur_process[0], [False, set(), cur_process[1]])
            process_tree[cur_process[0]][0] |= (cur_process[0] == process)
            process_tree[parent][1].add(cur_process[0])
            parent = cur_process[0]

            # Add process to info
            self._add_process(cur_process[1], None, info, False)

    def _build_graph_old(self, path, graph, info, process_tree):
        """ Build graph SVG and sidebar info. Previous is (host, launch, pid, read_stop) """
        # Create queue of remaining paths to process, and add initial path
        remaining = deque()
        remaining.appendleft((path, None))
        io_epsilon = self.core.configuration.values["io_epsilon"]

        with self.lock:
            cursor = self.db_connection.cursor()

            while len(remaining) > 0:
                current, previous = remaining.pop()

                # Create file and writer processes if first time seeing node
                if not graph.has_node(current):
                    # Add file node
                    self._add_file(current, graph, info, previous is None)

                    # Retrieve write/process data (read stop happened after write start)
                    statement = ("SELECT * FROM file NATURAL JOIN write NATURAL JOIN process "
                                 "WHERE path=? AND fcreate = ?")
                    if previous is not None:
                        statement += "AND (write.start = 0 OR write.start <= (? + ?))"
                        cursor.execute(statement, current + (previous[3], io_epsilon))
                    else:
                        cursor.execute(statement, current)

                    for row in cursor:
                        # Start with primary process
                        process_row = dict(row)
                        process = (process_row["phost"], process_row["pstart"], process_row["pid"])
                        # Add code used to be right here

                        # Propagate original time across pipes
                        write_stop = row["stop"]
                        if write_stop == 0:
                            write_stop = previous[3]

                        # Check primary and all parent processes for prior reads
                        while True:
                            read_cursor = self.db_connection.cursor()
                            statement = ("SELECT file.path, file.fcreate, read.stop FROM file NATURAL JOIN read NATURAL JOIN process "
                                         "WHERE phost=? AND pstart=? AND pid=? AND (read.start = 0 OR read.start <= (? + ?)) ")
                            read_cursor.execute(statement, process + (write_stop, io_epsilon))

                            read_row = None
                            for read_row in read_cursor:
                                # Propagate original time across pipes
                                read_stop = read_row["stop"]
                                if read_stop == 0:
                                    read_stop = write_stop

                                # Queue each process read (graphs get too large to perform this recursively)
                                remaining.appendleft(((read_row["path"], read_row["fcreate"]), process + (read_stop, )))

                            # Get current process full info (won't match write's row for parent processes)
                            process_cursor = self.db_connection.cursor()
                            statement = ("SELECT * FROM process "
                                         "WHERE phost=? AND pstart=? AND pid=?")
                            process_cursor.execute(statement, process)
                            process_row = process_cursor.fetchone()

                            # Add process (and write edge) if primary process or participating parent (parent that had read prior to fork)
                            if process == (row["phost"], row["pstart"], row["pid"]):
                                # Primary process and write edge
                                self._add_process(process_row, graph, info, True)
                                self._update_processes(process, process_tree, info)
                                self._add_io(row, graph, info, True)

                                # Note as child participating process for parents
                                child_process = process

                            elif read_row:
                                # Participating parent and fork edge (which read row used doesn't matter)
                                self._add_process(process_row, graph, info, True)
                                self._update_processes(process, process_tree, info)
                                self._add_fork(process_row, child_process, graph, info)

                                # Note as child participating process for parents
                                child_process = process

                            # Setup next parent (read must occur before child process was spawned)
                            if process_row["parent_pid"] == 0: break

                            write_stop = process_row["pstart"]
                            process = (row["phost"], process_row["parent_start"], process_row["parent_pid"])

                # Retrieve read data and connect to previous process (even for previously created nodes)
                if previous is not None:
                    statement = ("SELECT * FROM read "
                                 "WHERE phost=? AND pstart=? AND pid=? AND path=? AND fcreate = ?")
                    cursor.execute(statement, previous[:3] + current)
                    row = cursor.fetchone()
                    self._add_io(row, graph, info, False)

    def _get_graph_id(self, entry, section):
        """ Get requested ID type from row data """
        if section == "fork":
            # Entry is a tuple, first is child ID, second is normal row
            return entry[0] + self._get_graph_id(entry[1], "process")
        else:
            return tuple(entry[x] for x in self.db_keys[section])

    def _get_graph_vals(self, entry, section, primary=True):
        """ Get requested values from row data """
        ret_vals = dict()

        if section == "fork":
            # Entry is a tuple, first is child ID, second is normal row
            ret_vals["phost"] = entry[0][0]
            ret_vals["pstart1"] = entry[0][1]
            ret_vals["pid1"] = entry[0][2]
            ret_vals["pstart2"] = entry[1]["pstart"]
            ret_vals["pid2"] = entry[1]["pid"]

        elif section == "session":
            # Interactive only stores process ID
            for name in self.db_keys["process"]:
                ret_vals[name] = entry[name]

        else:
            for name, _ in self.db_vals[section]:
                ret_vals[name] = entry[name] if name in dict(entry) else None

        # Set graph visibility
        ret_vals["primary"] = primary

        return ret_vals

    def _get_mount_lookup(self):
        """ Get mount lookup table """
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT * FROM mount")

        return {row["mid"]: (row["root"], row["mount"]) for row in cursor}

    def _get_cwd_paths(self, cwd, root, mount):
        """ Get full paths for contained CWD """
        cwd_term = os.path.join(cwd, "")
        mount_term = os.path.join(mount, "")

        if not cwd_term.startswith(mount_term):
            return None

        return FileEntry.get_paths(cwd, root, mount)

    def _build_graph(self, path):
        """ Build graph info """
        sections = ("file", "process", "read", "write", "fork", "session")
        ret_graph = {key: {} for key in sections}

        # Add initial target file to queue
        remaining = deque()
        remaining.appendleft((self._get_graph_id(path, "file"), None, None))
        io_epsilon = self.core.configuration.values["io_epsilon"]

        with self.lock:
            cursor = self.db_connection.cursor()

            while len(remaining) > 0:
                file_id, read_process_id, read_stop = remaining.pop()

                # Add file if not already present
                if file_id not in ret_graph["file"]:
                    statement = ("SELECT * FROM file WHERE path=? AND fcreate = ?")
                    cursor.execute(statement, file_id)
                    ret_graph["file"][file_id] = self._get_graph_vals(cursor.fetchone(), "file")

                    # Retrieve write/process data (read stop happened after write start)
                    statement = ("SELECT * FROM file NATURAL JOIN write NATURAL JOIN process "
                                 "WHERE path=? AND fcreate = ?")

                    if read_stop is not None:
                        statement += "AND (write.start = 0 OR write.start <= (? + ?))"
                        cursor.execute(statement, file_id + (read_stop, io_epsilon))
                    else:
                        cursor.execute(statement, file_id)

                    for write_row in cursor:
                        # Start with primary process
                        write_process_id = self._get_graph_id(write_row, "process")

                        # Propagate original time across pipes
                        write_stop = write_row["stop"]
                        if write_stop == 0:
                            write_stop = read_stop

                        # Check primary and all parent processes for prior reads
                        lineage_id = write_process_id
                        session_closed = False
                        child_id = None

                        while True:
                            # Get current process in lineage full info (won't match write's row for parent processes)
                            lineage_cursor = self.db_connection.cursor()
                            statement = ("SELECT * FROM process "
                                         "WHERE phost=? AND pstart=? AND pid=?")
                            lineage_cursor.execute(statement, lineage_id)
                            lineage_row = lineage_cursor.fetchone()

                            ret_graph["process"][lineage_id] = self._get_graph_vals(lineage_row, "process")

                            # Stop tracing back through parents once we hit init (pid 1)
                            if lineage_row["parent_pid"] == 0: break

                            # Close out session if we're at the leader, stop following process reads
                            if not session_closed and (lineage_row["pid"] == lineage_row["session"]):
                                ret_graph["session"][lineage_id] = self._get_graph_vals(lineage_row, "session")
                                session_closed = True

                            if session_closed:
                                lineage_id = (lineage_row["phost"], lineage_row["parent_start"], lineage_row["parent_pid"])
                                continue

                            read_cursor = self.db_connection.cursor()
                            statement = ("SELECT file.path, file.fcreate, read.stop FROM file NATURAL JOIN read NATURAL JOIN process "
                                         "WHERE phost=? AND pstart=? AND pid=? AND (read.start = 0 OR read.start <= (? + ?)) ")
                            read_cursor.execute(statement, lineage_id + (write_stop, io_epsilon)) # previously write_process_id
                            read_row = None

                            for read_row in read_cursor:
                                # Propagate original time across pipes
                                read_stop = read_row["stop"]
                                if read_stop == 0:
                                    read_stop = write_stop

                                # Queue each process read (graphs get too large to perform this recursively)
                                remaining.appendleft((self._get_graph_id(read_row, "file"), lineage_id, read_stop)) # previously write_process_id

                            # Add process (primary or participating parent)
                            if lineage_id == write_process_id or read_row:
                                if lineage_id == write_process_id:
                                    # Primary with write
                                    ret_graph["write"][self._get_graph_id(write_row, "write")] = self._get_graph_vals(write_row, "write")
                                else:
                                    # Participating parent with fork
                                    ret_graph["fork"][self._get_graph_id((child_id, lineage_row), "fork")] = self._get_graph_vals((child_id, lineage_row), "fork")

                                # Note as child participating process for parents
                                child_id = lineage_id

                            else:
                                # Non-participating parent
                                ret_graph["process"][lineage_id]["primary"] = False

                            # Setup next parent (read must occur before child process was spawned)
                            write_stop = lineage_row["pstart"]
                            lineage_id = (lineage_row["phost"], lineage_row["parent_start"], lineage_row["parent_pid"])

                # Retrieve read data and connect to previous process (even for previously created nodes)
                if read_process_id is not None:
                    statement = ("SELECT * FROM read "
                                 "WHERE phost=? AND pstart=? AND pid=? AND path=? AND fcreate = ?")
                    cursor.execute(statement, read_process_id + file_id)
                    read_row = cursor.fetchone()
                    ret_graph["read"][self._get_graph_id(read_row, "read")] = self._get_graph_vals(read_row, "read")

        return ret_graph

    # TODO: add "public" parameter that removes any absolute path info (files, CWDs, environments)
    def _finalize_graph(self, graph):
        """ Finalize graph (calculate common root, add full paths) """
        mount_paths = set()
        collapsed_paths = set()
        lineage_fields = ("phost", "parent_start", "parent_pid")
        mount_lookup = self._get_mount_lookup()

        # Prepare additional fields and common paths for files
        for entry in graph["file"].values():
            root, mount = mount_lookup[entry["mid"]]

            # Modify fields
            entry["paths"] = FileEntry.get_paths(entry["path"], root, mount)
            entry["paths"].pop("relative")
            entry.pop("path")

            # Non-collapsed based on common mounts, collapsed on paths with IO only
            mount_paths.add(root)
            collapsed_paths.add(os.path.dirname(entry["paths"]["abs_real"]))

        for process_id, entry in graph["process"].items():
            root, mount = mount_lookup[entry["mid"]]
            cwd_paths = self._get_cwd_paths(entry["cwd"], root, mount)

            # Modify fields
            entry["cwd"] = {"orig": entry["cwd"], "abs_real": "", "rel_mount": "", "rel_collapsed": ""}

            # Only add process from lower-most session
            lineage = process_id

            while not lineage[2] == 0:
                if lineage in graph["session"]:
                    # Non-collapsed based on common mounts, collapsed on common CWD
                    mount_paths.add(root)

                    # Add corresponding absolute path
                    if cwd_paths:
                        entry["cwd"]["abs_real"] = cwd_paths["abs_real"]
                        collapsed_paths.add(entry["cwd"]["abs_real"])

                    break

                lineage = tuple(graph["process"][lineage][field] for field in lineage_fields)

        # Calculate common paths
        common_mount = os.path.commonpath(mount_paths)
        common_collapsed = os.path.commonpath(collapsed_paths)

        # Update entries with common paths
        for entry in graph["file"].values():
            entry["paths"]["rel_mount"] = os.path.relpath(entry["paths"]["abs_real"], common_mount)
            entry["paths"]["rel_collapsed"] = os.path.relpath(entry["paths"]["abs_real"], common_collapsed)

        for entry in graph["process"].values():
            if entry["cwd"]["abs_real"]:
                entry["cwd"]["rel_mount"] = os.path.relpath(entry["cwd"]["abs_real"], common_mount)
                entry["cwd"]["rel_collapsed"] = os.path.relpath(entry["cwd"]["abs_real"], common_collapsed)

    def _group_process(self, graph, process_tree, info, process):
        """ Group process nodes on a graph with relative process """
        remaining = deque(process_tree[process][1])
        children = list()

        # Display group process node on graph
        self._add_process(process_tree[process][2], graph, info, True)

        # Note all children currently in graph
        while len(remaining) > 0:
            current_process = remaining.pop()
            if process_tree[current_process][0]:
                children.append(current_process)

            remaining.extendleft(process_tree[current_process][1])

        # Move all IO of children to group process, and remove children
        for child in children:
            for edge in graph.edges(child):
                edge0_tuple = ast.literal_eval(edge[0])
                edge1_tuple = ast.literal_eval(edge[1])
                edge0_process = (edge0_tuple == child)

                if edge0_process:
                    # Write (Process->File)
                    process_render = "{0}-{1}".format(",".join(map(str, process)), ",".join(map(str, edge1_tuple)))
                    child_render = "{0}-{1}".format(",".join(map(str, child)), ",".join(map(str, edge1_tuple)))
                    graph.add_edge(process, edge[1], edgeURL="javascript:activate_io('{0}');".format(process_render))
                    graph.delete_edge(child, edge[1])
                else:
                    # Read (File->Process)
                    process_render = "{0}-{1}".format(",".join(map(str, edge0_tuple)), ",".join(map(str, process)))
                    child_render = "{0}-{1}".format(",".join(map(str, edge0_tuple)), ",".join(map(str, child)))
                    graph.add_edge(edge[0], process, edgeURL="javascript:activate_io('{0}');".format(process_render))
                    graph.delete_edge(edge[0], child)

                # Add new IO information
                info["io"]["{0}".format(process_render)] = info["io"]["{0}".format(child_render)]
                del info["io"]["{0}".format(child_render)]

            # Remove old child process
            graph.delete_node(child)

    def _collapse_processes(self, graph, process_tree, info, max_procs):
        """ Collapse by grouping processes by parent processes """
        total_count = 0
        remaining = deque()
        collapse = set()

        # Walk graph until the maximum number of processes has been reached
        remaining.appendleft((0, 0, 0))

        while len(remaining) > 0:
            process = remaining.pop()
            proc_count = 0

            # If the current total plus children is under max, continue.  Otherwise, add to collapse set
            if total_count + len(process_tree[process][1]) < max_procs:
                for child in process_tree[process][1]:
                    # If the child is present in the graph, add to the total count
                    if process_tree[child][0]:
                        proc_count += 1

                total_count += proc_count
                remaining.extendleft(process_tree[process][1])
            else:
                total_count += 1
                collapse.add(process)

        # Group any process nodes that are relatives of members of the collapse set
        for group in collapse:
            self._group_process(graph, process_tree, info, group)

    def _collapse_files(self, path, graph, info):
        """ Collapse by removing internal files """
        files = dict()

        # Iterate through all IO
        for io in graph.edges():
            io0_tuple = ast.literal_eval(io[0])
            io1_tuple = ast.literal_eval(io[1])

            # Check if and where a file node is present
            if len(io0_tuple) == 2:
                file_node, proc_node = io
                file_path = io0_tuple[0]
            elif len(io1_tuple) == 2:
                proc_node, file_node = io
                file_path = io1_tuple[0]
            else:
                # Process to process fork
                continue

            # Note file, process, and IO count
            files.setdefault((file_node, file_path), dict())
            files[(file_node, file_path)].setdefault(proc_node, 0)
            files[(file_node, file_path)][proc_node] += 1

        # Iterate through all files, and remove those with one associated process with both R/W (except requested file)
        for file_info in files:
            # Skip requested file, and files with more than 1 process
            if file_info[1] == path: continue
            if len(files[file_info]) > 1: continue

            # Iterate through the processes (should only be 1)
            for proc_node in files[file_info]:
                # Skip processes that only have one direction of IO
                if files[file_info][proc_node] == 1: continue

                # Remove file (automatically removes IO)
                graph.delete_node(file_info[0])

    def _wrap_graph_head(self, handle, path):
        # Headers
        html = ("<html><head>"
                "<title>Provenance for {0}</title>").format(path).encode()
        html += (b"<style>.sidebar{height:100%;width:450px;background-color:#c4c8d5;position:fixed!important;z-index:1;overflow:hidden}"
                 b".main-title{text-align:center;font-size:26px;padding:20px}"
                 b"table{border-collapse: collapse;}"
                 b"td,th{border: 1px solid black;text-align:left;padding:8px;}"
                 b"tr:nth-child(even){background-color:#DDDDDD} tr:nth-child(odd){background-color:#CCCCCC}</style>"
                 b"</head><body style='padding:0;margin:0;font-family:Arial,sans-serif;'>")

        # Sidebar
        html += (b"<div class='sidebar' style='right:0'>"
                 b"<div id='sidebar-title' style='text-align:center;padding-top:10px'></div>"
                 b"<div id='sidebar-body' style='padding:20px'></div></div>")

        # Start SVG DIV
        html += ("<div style='margin-right:400px'>"
                 "<div class='main-title'>Provenance for {0}</div>"
                 "<div style='text-align:center'>").format(path).encode()

        handle.write(html)

    def _wrap_graph_tail(self, handle, info):
        html = b"</div></div><script>"

        # Intialize info
        html += (b"var file = {};\n"
                 b"var process = {};\n"
                 b"var io = {};\n")

        for node_type in info:
            for item in info[node_type]:
                java_array = "[{0}]".format(str(info[node_type][item])[1:-1])
                html += "{0}['{1}'] = {2};\n".format(node_type, item, java_array).encode()

        # Create sidebar function
        html += (b"function activate_file(id) { "
                 b"document.getElementById('sidebar-title').innerHTML = '<b>File Information</b>';"
                 b"document.getElementById('sidebar-body').innerHTML = '<table style=\"width:100%\">' + "
                 b"'<tr><td>Path</td><td>' + file[id][0] + '</td></tr>' + "
                 b"'</table>';"
                 b"}\n"

                 b"function activate_process(id) { "
                 b"parent_link = \"<a href=\\\"javascript:activate_process('\" + process[id][3] + \"')\\\">\\\"\" + process[process[id][3]][4] + \"\\\"</a><br />\";"
                 b"if (process[id][3][2] == 1) parent_link = \"\\\"\" + process[process[id][3]][4] + \"\\\"\";"
                 b"child_links = '';"
                 b"for (var idx = 0 ; idx < process[id][8].length ; idx++) {"
                 b"   child_links += \"<a href=\\\"javascript:activate_process('\" + process[id][8][idx] + \"')\\\">\\\"\" + process[process[id][8][idx]][4] + \"\\\"</a><br />\";"
                 b"}"
                 b"document.getElementById('sidebar-title').innerHTML = '<b>Process Information</b>';"
                 b"document.getElementById('sidebar-body').innerHTML = '<div style=\"overflow:auto;border: 1px solid black;\"><table style=\"width:100%;border-style:hidden;\">' + "
                 b"'<tr><td>Host Name</td><td>' + process[id][0] + '</td></tr>' + "
                 b"'<tr><td>Start Time</td><td>' + process[id][1] + '</td></tr>' + "
                 b"'<tr><td>Process ID</td><td>' + process[id][2] + '</td></tr>' + "
                 b"'<tr><td>Parent</td><td>' + parent_link + '</td></tr>' + "
                 b"'<tr><td>Command</td><td>' + process[id][4] + '</td></tr>' + "
                 b"'<tr><td>Executable</td><td>' + process[id][5] + '</td></tr>' + "
                 b"'<tr><td>Hash</td><td>' + process[id][6] + '</td></tr>' + "
                 b"'<tr><td>Working Dir</td><td>' + process[id][7] + '</td></tr>' + "
                 b"'<tr><td>Children</td><td>' + child_links + '</td></tr>' + "
                 b"'<tr><td>Environment</td><td>' + process[id][8] + '</td></tr>' + "
                 b"'</table></div>';"
                 b"}\n"

                 b"function activate_io(id) { "
                 b"document.getElementById('sidebar-title').innerHTML = '<b>IO Information</b>';"
                 b"document.getElementById('sidebar-body').innerHTML = '<table style=\"width:100%\">' + "
                 b"'<tr><td>Direction</td><td>' + io[id][0] + '</td></tr>' + "
                 b"'<tr><td>Start</td><td>' + io[id][1] + '</td></tr>' + "
                 b"'<tr><td>End</td><td>' + io[id][2] + '</td></tr>' + "
                 b"'</table>';"
                 b"}\n")

        html += b"</script></body></html>"
        handle.write(html)

    def register_open(self, descriptor):
        """ Register process, file, and IO start """
        self._gen_pid_index(descriptor)

    def register_close(self, descriptor):
        """ Write IO records if IO occurred """
        self.core.log("Provenance: Registering close {0}".format(descriptor), self.core.LOG_DEBUG)
        desc_entry = DescriptorEntry.get(descriptor)
        read_entry = self.read_cache[descriptor]
        write_entry = self.write_cache[descriptor]

        with self.lock:
            cursor = self.db_connection.cursor()

            # Register file if necessary (use earliest IO start time as create time)
            create_time = None
            for pid_index in read_entry:
                if create_time is None or read_entry[pid_index][0] < create_time:
                    create_time = read_entry[pid_index][0]

            self._register_file(descriptor, create_time=create_time)

            # Save I/O for each process associated with this descriptor
            for direction in ("read", "write"):
                cache = read_entry if direction == "read" else write_entry
                query_lookup = ("SELECT start, ops FROM {0} "
                                "INNER JOIN file_last ON ({0}.path = file_last.path AND {0}.fcreate = file_last.fcreate) "
                                "WHERE phost = ? AND pstart = ? AND pid = ? AND {0}.path = ?".format(direction))

                query_update = ("INSERT OR REPLACE INTO {0} (phost, pstart, pid, path, fcreate, start, stop, ops)"
                                "SELECT ?, ?, ?, path, fcreate, ?, ?, ? FROM file_last WHERE path = ?".format(direction))

                for pid_index in cache:
                    if cache[pid_index][1] is not None:
                        # Lookup existing start time and operations
                        values = (self.system_name, ) + pid_index + (desc_entry.file_entry.paths["abs_real"], )
                        cursor.execute(query_lookup, values)
                        result = cursor.fetchone()

                        # Merge start and operations if applicable
                        start = cache[pid_index][0]
                        ops = cache[pid_index][3]
                        if result:
                            start = result[0]
                            ops |= result[1]

                        # Update I/O table
                        values = (self.system_name, ) + pid_index + (start, cache[pid_index][1], ops, desc_entry.file_entry.paths["abs_real"])
                        cursor.execute(query_update, values)

            self.db_connection.commit()
            """
            # Save reads for each process associated with this descriptor (retrieve earlier start time if available)
            for pid_index in read_entry:
                if read_entry[pid_index][1] is not None:
                    values = (self.system_name, ) + pid_index + (desc_entry.file_entry.paths["abs_real"], )

                    cursor.execute("SELECT start, ops FROM read "
                                   "INNER JOIN file_active ON (read.path = file_active.path AND read.fcreate = file_active.fcreate) "
                                   "WHERE phost = ? AND pstart = ? AND pid = ? AND read.path = ?", values)
                    result = cursor.fetchone()
                    start = result[0] if result else read_entry[pid_index][0]
                    ops = read_entry[pid_index][3]
                    if result: ops |= result[1]

                    values = (self.system_name, ) + pid_index + (start, read_entry[pid_index][1], ops, desc_entry.file_entry.paths["abs_real"])
                    cursor.execute("INSERT OR REPLACE INTO read (phost, pstart, pid, path, fcreate, start, stop, ops)"
                                   "SELECT ?, ?, ?, path, fcreate, ?, ?, ? FROM file_active WHERE path = ?", values)

            # Save writes for each process associated with this descriptor (retrieve earlier start time if available)
            for pid_index in write_entry:
                if write_entry[pid_index][1] is not None:
                    values = (self.system_name, ) + pid_index + (desc_entry.file_entry.paths["abs_real"], )

                    cursor.execute("SELECT start, ops FROM write "
                                   "INNER JOIN file_active ON (write.path = file_active.path AND write.fcreate = file_active.fcreate) "
                                   "WHERE phost = ? AND pstart = ? AND pid = ? AND write.path = ?", values)
                    result = cursor.fetchone()
                    start = result[0] if result else write_entry[pid_index][0]
                    ops = read_entry[pid_index][3]
                    if result: ops |= result[1]

                    values = (self.system_name, ) + pid_index + (start, write_entry[pid_index][1], desc_entry.file_entry.paths["abs_real"])
                    cursor.execute("INSERT OR REPLACE INTO write (phost, pstart, pid, path, fcreate, start, stop)"
                                   "SELECT ?, ?, ?, path, fcreate, ?, ? FROM file_active WHERE path = ?", values)"""

    def register_create(self, descriptor):
        """ Register new file """
        self._register_file(descriptor, force=True)
        self._gen_pid_index(descriptor)

    def register_read(self, descriptor, op_type=OP_IO):
        """ Update read end time """
        self._register_read(descriptor, op_type)

    def register_write(self, descriptor, op_type=OP_IO, truncate=False):
        """ Update write end time """
        self._register_write(descriptor, op_type, truncate=truncate)

    def register_op_read(self, descriptor, op_type):
        """ Register a file system read operation """
        self.register_open(descriptor)
        self.register_read(descriptor, op_type)
        self.register_close(descriptor)

    def register_op_write(self, descriptor, op_type):
        """ Register a file system read operation """
        self.register_open(descriptor)
        self.register_write(descriptor, op_type)
        self.register_close(descriptor)

    def register_delete(self, descriptor):
        """ Remove active marker from deleted file """
        self._register_delete(descriptor)

#    def register_move(self, old, new):
#        """ Record move as read old and write new """
#        self.register_open(old)
#        self.register_open(new)
#        self.register_read(old)
#        self.register_write(new)
#        self.register_close(old)
#        self.register_close(new)
#
#        # Remove active marker for old file
#        self.register_delete(old)

    def virt_graph_svg(self, process, handle, collapse):
        file_path = process.cache_entry.file_entry.derived_source.paths["abs_real"]

        # Lookup active version of requested file
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT * FROM file_last WHERE path = ?", (file_path, ))
        result = cursor.fetchone()
        target_file = (result["path"], result["fcreate"])

        # Prepare graph
        graph = pygraphviz.AGraph(directed=True, splines="ortho", ranksep=".7", fontsize="20", pad="0.5")
        graph.node_attr["fontsize"] = "12"

        # Build graph from history
        info = {'file': dict(), 'process': dict(), 'io': dict()}
        process_tree = dict()
        self._build_graph_old(target_file, graph, info, process_tree)

        # Collapse graph if requested
        if collapse:
            self._collapse_processes(graph, process_tree, info, 7)
            self._collapse_files(file_path, graph, info)

        # Save to disk
        graph.layout(prog='dot')
        self._wrap_graph_head(handle, file_path)
        graph.draw(path=handle, format="svg")
        self._wrap_graph_tail(handle, info)

    def virt_graph_json(self, process, handle):
        file_path = process.cache_entry.file_entry.derived_source.paths["abs_real"]

        # Lookup active version of requested file
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT * FROM file_last WHERE path = ?", (file_path, ))
        result = cursor.fetchone()

        # Build provenance graph
        graph = self._build_graph(result)
        self._finalize_graph(graph)

        # Convert tuple keys to strings, save to disk
        str_graph = {section: {"_".join(str(kp) for kp in k): v for (k, v) in graph[section].items()} for section in graph}
        handle.write(bytes(json.dumps(str_graph).encode("utf8")))

    def repeat_test(self, process, handle):
        """ Create shell script to reproduce results """
        file_path = process.cache_entry.file_entry.derived_soruce.paths["abs_real"]

        # Lookup active version of requested file
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT * FROM file_last WHERE path = ?", (file_path, ))
        result = cursor.fetchone()
        target_file = (result["path"], result["fcreate"])

        # Prepare graph
        graph = pygraphviz.AGraph(directed=True, splines="ortho", ranksep=".7", fontsize="20", pad="0.5")
        graph.node_attr["fontsize"] = "12"

        # Build graph from history
        info = {'file': dict(), 'process': dict(), 'io': dict()}
        process_tree = dict()
        self._build_graph_old(target_file, graph, info, process_tree)

        # Filter and sort visible processes
        visible_procs = []
        process_added = set()

        for pid, proc in process_tree.items():
            # Skip invisible parent processes
            if not proc[0]:
                continue

            # Check if process has already been added
            if pid in process_added:
                continue

            # TODO: remove this, skip run.sh
            if "run.sh" in proc[2]["cmd"]:
                continue

            # Add process to list and record children
            visible_procs.append((proc[2]["pstart"], proc[2]["cwd"], proc[2]["cmd"]))
            process_queue = list(proc[1])

            while len(process_queue) > 0:
                child = process_queue.pop()
                if child in process_added:
                    continue

                process_added.add(child)
                process_queue.extend(list(process_tree[child][1]))

        visible_procs = sorted(visible_procs)

        for proc in sorted(visible_procs):
            handle.write(bytes("cd {} && {}\n".format(*proc[1:]).encode("utf8")))
