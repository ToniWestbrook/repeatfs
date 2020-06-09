#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import os
from collections import deque
from repeatfs.descriptor_entry import DescriptorEntry
from repeatfs.file_entry import FileEntry


class Graph:
    """ Graph building functionality """
    def __init__(self, management):
        self.management = management

    def _get_graph_id(self, entry, section):
        """ Get requested ID type from row data """
        if section == "fork":
            # Entry is a tuple, first is child ID, second is normal row
            return entry[0] + self._get_graph_id(entry[1], "process")
        else:
            return tuple(str(entry[x]) for x in self.management.db_keys[section])

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
            # Session stores process keys
            for name in self.management.db_keys["process"]:
                ret_vals[name] = entry[name]

        else:
            for name, _ in self.management.db_vals[section]:
                ret_vals[name] = entry[name] if name in dict(entry) else None

        # Set graph visibility (no longer used)
        # ret_vals["primary"] = primary

        return ret_vals

    # TODO: add "public" parameter that removes any absolute path info (files, CWDs, environments)
    def _finalize_graph(self, graph):
        """ Finalize graph (calculate common root and rewrite paths) """
        mount_paths = set()
        lineage_fields = ("phost", "parent_start", "parent_pid")
        mount_lookup = self.management._get_mount_lookup()

        # Calcualte common mount path TODO: Migrate all the modifications below, have a token represent common root, rewrite cmd/env/stdio
        for process_id, entry in graph["process"].items():
            root, mount = mount_lookup[entry["mid"]]

            # Only add process from bottom-most session in process tree
            lineage = process_id
            while not lineage[2] == "0":
                if lineage in graph["session"]:
                    mount_paths.add(root)
                    break

                lineage = tuple(str(graph["process"][lineage][field]) for field in lineage_fields)

        common_mount = os.path.commonpath(mount_paths)

        # Update entries with relative paths
        for entry in graph["file"].values():
            entry["paths"] = {"abs_real": entry["path"], "rel_mount": entry["path"][len(common_mount) + 1:]}
            del entry["path"]

        for entry in graph["process"].values():
            # Rewrite CWD
            entry["cwd"] = {"orig": entry["cwd"], "rel_mount": None}
            cwd_paths = FileEntry.get_paths(entry["cwd"]["orig"], root, mount)

            if cwd_paths["orig_type"] == "abs_mount":
                entry["cwd"]["rel_mount"] = cwd_paths["abs_real"][len(common_mount) + 1:]

            # Rewrite absolute paths as relative from mount point
            for field in ("cmd", "env", "stdin", "stdout", "stderr"):
                entry[field] = entry[field].replace(mount, "$$$")

            # Split cmd arguments
            entry["cmd"] = entry["cmd"].split("\0")

    def build_graph(self, target_id, op_filter=None):
        """ Build graph info """
        sections = ("file", "process", "read", "write", "session", "target")
        ret_graph = {key: {} for key in sections}

        # Add provenance target
        target_graph_id = self._get_graph_id(target_id, "file")
        ret_graph["target"] = {(0, ): target_graph_id}

        # Add initial target file to queue
        remaining = deque()
        remaining.appendleft((self._get_graph_id(target_id, "file"), None, None))
        io_epsilon = self.management.core.configuration.values["io_epsilon"]
        if op_filter is None:
            op_filter = self.management.OP_ALL

        with self.management.lock:
            cursor = self.management.db_connection.cursor()

            while len(remaining) > 0:
                file_id, read_process_id, read_stop = remaining.pop()

                # Add file if not already present
                if file_id not in ret_graph["file"]:
                    statement = ("SELECT * FROM file WHERE path=? AND fcreate = ?")
                    cursor.execute(statement, file_id)
                    ret_graph["file"][file_id] = self._get_graph_vals(cursor.fetchone(), "file")

                    # Retrieve write/process data (read stop happened after write start)
                    statement = ("SELECT * FROM file NATURAL JOIN write NATURAL JOIN process "
                                 "WHERE path=? AND fcreate = ? AND (write.ops & ?) > 0 ")

                    if read_stop is not None:
                        statement += "AND (write.start = 0 OR write.start <= (? + ?)) ORDER BY write.start DESC"
                        cursor.execute(statement, file_id + (op_filter, read_stop, io_epsilon))
                    else:
                        statement += " ORDER BY write.start DESC"
                        cursor.execute(statement, file_id + (op_filter, ))
                        
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

                        while True:
                            # Get current process in lineage full info (won't match write's row for parent processes)
                            lineage_cursor = self.management.db_connection.cursor()
                            statement = ("SELECT * FROM process "
                                         "WHERE phost=? AND pstart=? AND pid=?")
                            lineage_cursor.execute(statement, lineage_id)
                            lineage_row = lineage_cursor.fetchone()

                            ret_graph["process"][lineage_id] = self._get_graph_vals(lineage_row, "process")

                            # Record thread group leader
                            thread_id = (lineage_row["phost"], str(lineage_row["tgid_start"]), str(lineage_row["tgid"]))

                            if lineage_id != thread_id:
                                thread_cursor = self.management.db_connection.cursor()
                                thread_cursor.execute(statement, thread_id)
                                thread_row = thread_cursor.fetchone()

                                ret_graph["process"][thread_id] = self._get_graph_vals(thread_row, "process")

                            # Stop tracing back through parents once we hit init (pid 1)
                            if lineage_row["parent_pid"] == 0: break

                            # Close out session if we're at the leader, stop following process reads
                            session_match = (lineage_row["pstart"], lineage_row["pid"]) == (lineage_row["session_start"], lineage_row["session_id"])
                            if not session_closed and session_match:
                                ret_graph["session"][lineage_id] = self._get_graph_vals(lineage_row, "session")
                                session_closed = True

                            if session_closed:
                                lineage_id = (lineage_row["phost"], str(lineage_row["parent_start"]), str(lineage_row["parent_pid"]))
                                continue

                            read_cursor = self.management.db_connection.cursor()
                            statement = ("SELECT file.path, file.fcreate, read.stop FROM file NATURAL JOIN read NATURAL JOIN process "
                                         "WHERE phost=? AND pstart=? AND pid=? AND (read.ops & ?) > 0 AND (read.start = 0 OR read.start <= (? + ?)) "
                                         "ORDER BY read.start DESC ")
                            read_cursor.execute(statement, lineage_id + (op_filter, write_stop, io_epsilon)) # previously write_process_id
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
                                    pass
                                    #ret_graph["fork"][self._get_graph_id((child_id, lineage_row), "fork")] = self._get_graph_vals((child_id, lineage_row), "fork")

                                # Note as child participating process for parents (was part of fork code)
                                # child_id = lineage_id

                            else:
                                # Non-participating parent
                                ret_graph["process"][lineage_id]["primary"] = False

                            # Setup next parent (read must occur before child process was spawned)
                            write_stop = lineage_row["pstart"]
                            lineage_id = (lineage_row["phost"], str(lineage_row["parent_start"]), str(lineage_row["parent_pid"]))

                # Retrieve read data and connect to previous process (even for previously created nodes)
                if read_process_id is not None:
                    statement = ("SELECT * FROM read "
                                 "WHERE phost=? AND pstart=? AND pid=? AND path=? AND fcreate = ?")
                    cursor.execute(statement, read_process_id + file_id)
                    read_row = cursor.fetchone()
                    ret_graph["read"][self._get_graph_id(read_row, "read")] = self._get_graph_vals(read_row, "read")

        # Finalize graph
        self._finalize_graph(ret_graph)

        return ret_graph
