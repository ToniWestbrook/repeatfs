#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import os
from repeatfs.provenance.replication import Replication
from repeatfs.provenance.render_base import RenderBase

# Third party modules
import pygraphviz


class RenderGraphviz(RenderBase):
    """ Render provenance graph as Graphviz SVG """
    STYLES = {
        "process": {"color": "black", "fillcolor": "red:white", "style": "filled", "gradientangle": "270", "shape": "component", "fontsize": "18"},
        "file": {"color": "black", "fillcolor": "blue:white", "style": "filled", "gradientangle": "270", "shape": "note", "fontsize": "18"},
        "target": {"color": "black", "fillcolor": "green:white", "style": "filled", "gradientangle": "270", "shape": "note", "fontsize": "18"},
        "io": {"color": "black"},
        "fork": {"color": "red"}
    }

    def __init__(self, management):
        self.management = management

#    def _add_file(self, file_index, graph, info, target):
#        """ Add file node to graph """
#        file_render = ",".join(map(str, file_index))
#        node_color = "green:white" if target else "blue:white"
#        graph.add_node(file_index, label=file_index[0], color="black", fillcolor=node_color, style="filled", gradientangle="270", shape="note", URL="javascript:activate_file('{0}');".format(file_render))
#        info["file"][file_render] = file_index + ("test", )
#
#    def _add_process(self, entry, graph, info, render):
#        """ Add process node to graph """
#        process = (entry["phost"], entry["pstart"], entry["pid"])
#        parent = [entry["phost"], entry["parent_start"], entry["parent_pid"]]
#        process_render = ",".join(map(str, process))
#        parent_render = ",".join(map(str, parent))
#
#        # Update info if present
#        if len(entry) > 3:
#            # Create placeholders for process and parent (to hold children before declared)
#            info["process"].setdefault(process_render, [""] * 9 + [[]])
#            info["process"].setdefault(parent_render, [""] * 9 + [[]])
#
#            # Update process in info table
#            process_start = time.strftime(self.DATE_FORMAT, time.localtime(entry["pstart"]))
#            env_render = "<br>".join(sorted(entry["env"].split("\0")[:-1]))
#            info["process"][process_render][:9] = [
#                entry["phost"], process_start, entry["pid"], parent, entry["cmd"], entry["exe"], entry["hash"], entry["cwd"], env_render]
#
#            # Add process to parent TODO: change this over to a list instead of repeatedly converting
#            if list(process) not in info["process"][parent_render][9]:
#                info["process"][parent_render][9].append(list(process))
#
#        # If render requested, add to graph
#        if render:
#            label = info["process"][process_render][4]
#            if len(label) > 50:
#                label = label[:50] + "..."
#
#            graph.add_node(process, label=label, color="black", fillcolor="red:white", style="filled", gradientangle="270", shape="component", URL="javascript:activate_process('{0}');".format(process_render))
#
#    def _add_io(self, entry, graph, info, write):
#        """ Add IO edge to graph """
#        process = (entry["phost"], entry["pstart"], entry["pid"])
#        process_render = ",".join(map(str, process))
#        file_index = (entry["path"], entry["fcreate"])
#        file_render = ",".join(map(str, file_index))
#        start = time.strftime(self.DATE_FORMAT, time.localtime(entry["start"]))
#        stop = time.strftime(self.DATE_FORMAT, time.localtime(entry["stop"]))
#
#        # Direct the graph according to read/write
#        if write:
#            graph.add_edge(process, file_index, edgeURL="javascript:activate_io('{0}-{1}');".format(process_render, file_render))
#            info["io"]["{0}-{1}".format(process_render, file_render)] = ("write", start, stop)
#        else:
#            graph.add_edge(file_index, process, edgeURL="javascript:activate_io('{0}-{1}');".format(file_render, process_render))
#            info["io"]["{0}-{1}".format(file_render, process_render)] = ("read", start, stop)
#
#    def _add_fork(self, parent, child_process, graph, info):
#        """ Add forked process IO """
#        parent_process = (parent["phost"], parent["pstart"], parent["pid"])
#        parent_render = ",".join(map(str, parent_process))
#        child_render = ",".join(map(str, child_process))
#        fork = time.strftime(self.DATE_FORMAT, time.localtime(child_process[1]))
#
#        # Direct the graph from parent to child
#        graph.add_edge(parent_process, child_process, edgeURL="javascript:activate_io('{0}-{1}');".format(parent_render, child_render))
#        info["io"]["{0}-{1}".format(parent_render, child_render)] = ("write", fork, fork)
#
#    def _update_processes(self, process, process_tree, info):
#        """ Add relatives of process into process tree """
#        lineage = list()
#        statement = "SELECT * FROM process WHERE AND phost = ? AND pstart = ? AND pid = ?"
#        cursor = self.db_connection.cursor()
#        cur_process = process
#
#        # Trace lineage
#        while cur_process[2] > 0:
#            # Retrieve row for current process
#            cursor.execute(statement, cur_process)
#            row = cursor.fetchone()
#            lineage.append((cur_process, row))
#
#            # Iterate to parent
#            cur_process = (row["phost"], row["parent_start"], row["parent_pid"])
#
#        # Update tree with lineage, format {proc1: (in_graph, [child_proc1, child_proc2]), proc2, child_proc1, grandchild_proc1, ...}
#        parent = (0, 0, 0)
#        process_tree.setdefault(parent, (False, set()))
#
#        for cur_process in lineage[::-1]:
#            # Add process to process tree
#            process_tree.setdefault(cur_process[0], [False, set(), cur_process[1]])
#            process_tree[cur_process[0]][0] |= (cur_process[0] == process)
#            process_tree[parent][1].add(cur_process[0])
#            parent = cur_process[0]
#
#            # Add process to info
#            self._add_process(cur_process[1], None, info, False)

#    def _build_graph_old(self, path, graph, info, process_tree):
#        """ Build graph SVG and sidebar info. Previous is (host, launch, pid, read_stop) """
#        # Create queue of remaining paths to process, and add initial path
#        remaining = deque()
#        remaining.appendleft((path, None))
#        io_epsilon = self.core.configuration.values["io_epsilon"]
#
#        with self.lock:
#            cursor = self.db_connection.cursor()
#
#            while len(remaining) > 0:
#                current, previous = remaining.pop()
#
#                # Create file and writer processes if first time seeing node
#                if not graph.has_node(current):
#                    # Add file node
#                    self._add_file(current, graph, info, previous is None)
#
#                    # Retrieve write/process data (read stop happened after write start)
#                    statement = ("SELECT * FROM file NATURAL JOIN write NATURAL JOIN process "
#                                 "WHERE path=? AND fcreate = ?")
#                    if previous is not None:
#                        statement += "AND (write.start = 0 OR write.start <= (? + ?))"
#                        cursor.execute(statement, current + (previous[3], io_epsilon))
#                    else:
#                        cursor.execute(statement, current)
#
#                    for row in cursor:
#                        # Start with primary process
#                        process_row = dict(row)
#                        process = (process_row["phost"], process_row["pstart"], process_row["pid"])
#                        # Add code used to be right here
#
#                        # Propagate original time across pipes
#                        write_stop = row["stop"]
#                        if write_stop == 0:
#                            write_stop = previous[3]
#
#                        # Check primary and all parent processes for prior reads
#                        while True:
#                            read_cursor = self.db_connection.cursor()
#                            statement = ("SELECT file.path, file.fcreate, read.stop FROM file NATURAL JOIN read NATURAL JOIN process "
#                                         "WHERE phost=? AND pstart=? AND pid=? AND (read.start = 0 OR read.start <= (? + ?)) ")
#                            read_cursor.execute(statement, process + (write_stop, io_epsilon))
#
#                            read_row = None
#                            for read_row in read_cursor:
#                                # Propagate original time across pipes
#                                read_stop = read_row["stop"]
#                                if read_stop == 0:
#                                    read_stop = write_stop
#
#                                # Queue each process read (graphs get too large to perform this recursively)
#                                remaining.appendleft(((read_row["path"], read_row["fcreate"]), process + (read_stop, )))
#
#                            # Get current process full info (won't match write's row for parent processes)
#                            process_cursor = self.db_connection.cursor()
#                            statement = ("SELECT * FROM process "
#                                         "WHERE phost=? AND pstart=? AND pid=?")
#                            process_cursor.execute(statement, process)
#                            process_row = process_cursor.fetchone()
#
#                            # Add process (and write edge) if primary process or participating parent (parent that had read prior to fork)
#                            if process == (row["phost"], row["pstart"], row["pid"]):
#                                # Primary process and write edge
#                                self._add_process(process_row, graph, info, True)
#                                self._update_processes(process, process_tree, info)
#                                self._add_io(row, graph, info, True)
#
#                                # Note as child participating process for parents
#                                child_process = process
#
#                            elif read_row:
#                                # Participating parent and fork edge (which read row used doesn't matter)
#                                self._add_process(process_row, graph, info, True)
#                                self._update_processes(process, process_tree, info)
#                                self._add_fork(process_row, child_process, graph, info)
#
#                                # Note as child participating process for parents
#                                child_process = process
#
#                            # Setup next parent (read must occur before child process was spawned)
#                            if process_row["parent_pid"] == 0: break
#
#                            write_stop = process_row["pstart"]
#                            process = (row["phost"], process_row["parent_start"], process_row["parent_pid"])
#
#                # Retrieve read data and connect to previous process (even for previously created nodes)
#                if previous is not None:
#                    statement = ("SELECT * FROM read "
#                                 "WHERE phost=? AND pstart=? AND pid=? AND path=? AND fcreate = ?")
#                    cursor.execute(statement, previous[:3] + current)
#                    row = cursor.fetchone()
#                    self._add_io(row, graph, info, False)

    def _build_elements(self, graph, expand_procs, collapse_files):
        """ Build elements given session chains """
        # Note target file
        target = graph["target"][(0,)]
        if collapse_files:
            target = graph["file"][target]["paths"]["abs_real"]

        # Get session children processes
        replication = Replication(graph, self.management.core, expand=expand_procs)
        session_chains = replication.get_session_chains(filter_expanded=False)
        session_children = set(child for chain in session_chains for child in chain)

        # Note all processes associated with each IO type per file
        file_groups = {}

        for io_type in ("read", "write"):
            for io_id in graph[io_type]:
                io_file = io_id[3:]
                io_process = replication.trace_session_child(io_id[:3], session_chains)
                if collapse_files:
                    io_file = graph["file"][io_file]["paths"]["abs_real"]

                file_groups.setdefault(io_file, {"read": set(), "write": set()})
                file_groups[io_file][io_type].add(io_process)

        # Build IO groups (common read, common write pairings for each file)
        io_groups = {}

        for file_id in file_groups:
            # Freeze sets to allow hashing
            read_group = frozenset(file_groups[file_id]["read"])
            write_group = frozenset(file_groups[file_id]["write"])
            io_group = (read_group, write_group)

            # Note file associated with each IO group
            io_groups.setdefault(io_group, {"files": [], "target": None})
            if file_id == target:
                io_groups[io_group]["target"] = file_id
            else:
                io_groups[io_group]["files"].append(file_id)

        return {"process": session_children, "io": io_groups}

    def _add_process(self, process_id, graph, svg, js):
        """ Add process to SVG/JS """
        # Add SVG
        label = " ".join(graph["process"][process_id]["cmd"])
        label = label.replace("/home/anthonyw", "/opt")
        if len(label) > 40:
            label = "{}...".format(label[:40])

        if not svg.has_node(process_id):
            svg.add_node(process_id, label=label, URL="javascript:activate_process('something');", **self.STYLES["process"])

        # Add Javascript

    def _add_io_group(self, group_id, files_info, graph, svg, js):
        """ Add IO group to SVG/JS """
        # Add file SVG
        for check_target in (False, True):
            # Only process target if present
            if check_target and not files_info["target"]:
                continue

            if len(files_info["files"]) == 1 or check_target:
                file_id = files_info["target"] if check_target else files_info["files"][0]
                if isinstance(file_id, tuple):
                    label = os.path.basename(graph["file"][file_id]["paths"]["abs_real"])
                else:
                    label = os.path.basename(file_id)
            else:
                # If no files present (target only), skip to target
                if len(files_info["files"]) == 0:
                    continue

                file_id = None
                label = "{} files".format(len(files_info["files"]))

            node_id = file_id if check_target else group_id
            if not svg.has_node(node_id):
                style = "target" if check_target else "file"
                svg.add_node(node_id, label=label, URL="javascript:activate_file('something');", **self.STYLES[style])

            # Add IO SVG (read, write)
            for io_type in range(2):
                if len(group_id[io_type]) > 0:
                    for process_id in group_id[io_type]:
                        if check_target:
                            edge = (file_id, process_id) if io_type == 0 else (process_id, file_id)
                        else:
                            edge = (group_id, process_id) if io_type == 0 else (process_id, group_id)

                        if not svg.has_edge(*edge):
                            svg.add_edge(*edge)

    def _add_fork(self, parent_id, child_id, svg, js):
        """ Add fork between processes """
        edge = (parent_id, child_id)
        if not svg.has_edge(*edge):
            svg.add_edge(*edge, **self.STYLES["fork"])

    def _add_elements(self, graph, svg, js, expand_procs=None, collapse_files=True):
        """ Add elements from graph to SVG/JS """
        # Build elements for session chains
        elements = self._build_elements(graph, expand_procs, collapse_files)

        # Render processes
        for process_id in elements["process"]:
            self._add_process(process_id, graph, svg, js)

        # Render IO groups
        for group_id in elements["io"]:
            self._add_io_group(group_id, elements["io"][group_id], graph, svg, js)

        # Render forks
        for process_id in elements["process"]:
            process_info = graph["process"][process_id]
            parent_id = (process_info["phost"], str(process_info["parent_start"]), str(process_info["parent_pid"]))
            
            if parent_id in elements["process"]:
                self._add_fork(parent_id, process_id, svg, js)

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

    def virt_render(self, process, handle, options=None):
        """ Render SVG """
        op_filter = self.management.OP_IO | self.management.OP_MOVE  # | self.management.OP_ALL
        graph = self._get_graph(process, options, op_filter=op_filter)

        # Prepare graph
        js = []
        svg = pygraphviz.AGraph(directed=True, splines="ortho", ranksep=".7", fontsize="20", pad="0.5")
        svg.node_attr["fontsize"] = "12"
        # self._add_elements(graph, svg, js, expand_procs=[("turing", "1585344631.4", "23501")])
        self._add_elements(graph, svg, js)

        # Save to disk
        svg.layout(prog='dot')
        self._wrap_graph_head(handle, os.path.basename(graph["file"][graph["target"][(0,)]]["paths"]["abs_real"]))
        svg.draw(path=handle, format="svg")
        # self._wrap_graph_tail(handle, info)
