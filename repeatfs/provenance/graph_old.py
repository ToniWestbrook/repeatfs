#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import ast

# Third party modules
import pygraphviz

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
