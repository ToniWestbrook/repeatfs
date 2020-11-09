#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import io
import json
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

    def _build_elements(self, graph, expand_procs, collapse_files):
        """ Build elements given session chains """
        # Note target file
        target = graph["target"][(0,)]
        if collapse_files:
            target = (graph["file"][target]["paths"]["abs_real"], )

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

                # To collapse multiple versions of file, store by path only
                if collapse_files:
                    io_file = (graph["file"][io_file]["paths"]["abs_real"], )

                file_groups.setdefault(io_file, {"read": dict(), "write": dict(), "fcreate": set()})
                file_groups[io_file][io_type][io_process] = io_id;
                file_groups[io_file]["fcreate"].add(io_id[4])

        # Build IO groups (common read, common write pairings for each file)
        io_groups = {}

        for file_id in file_groups:
            # Freeze sets to allow hashing
            read_group = frozenset(file_groups[file_id]["read"].keys())
            write_group = frozenset(file_groups[file_id]["write"].keys())

            if file_id == target:
                # Ensure target is always in its own IO group
                io_group = (read_group, write_group, file_id)
            else:
                io_group = (read_group, write_group)

            # Note file and original IO associated with each IO group
            io_groups.setdefault(io_group, {"files": {}, "target": file_id == target, "read_lookup": {}, "write_lookup": {}})
            io_groups[io_group]["files"][file_id] = file_groups[file_id]["fcreate"]

            for io_type in ("read", "write"):
                for io_id in file_groups[file_id][io_type]:
                    io_groups[io_group]["{}_lookup".format(io_type)].setdefault(io_id, [])
                    io_groups[io_group]["{}_lookup".format(io_type)][io_id].append(file_groups[file_id][io_type][io_id])

        return {"process": session_children, "io": io_groups}

    def _add_process(self, process_id, graph, svg):
        """ Add process to SVG/JS """
        # Add SVG
        label = " ".join(graph["process"][process_id]["cmd"])
        label = label.replace("/home/anthonyw", "/opt")
        if len(label) > 40:
            label = "{}...".format(label[:40])

        if not svg.has_node(process_id):
            svg.add_node(process_id, label=label, URL="javascript:activate_process('{}');".format("|".join(process_id)), **self.STYLES["process"])

        # Add Javascript

    def _add_io_group(self, group_id, files_info, graph, svg):
        """ Add IO group to SVG/JS """
        # Build IDs and label
        node_id = []
        for path_id in files_info["files"]:
            for fcreate in files_info["files"][path_id]:
                node_id.append((path_id[0], fcreate))

        js_files = ["'{}'".format("|".join(file_id)) for file_id in node_id]

        if len(node_id) == 1:
            label = os.path.basename(node_id[0][0])
        else:
            label = "{} files".format(len(node_id))

        # Add group SVG
        if not svg.has_node(node_id):
            style = "target" if files_info["target"] else "file"
            svg.add_node(node_id, label=label, URL="javascript:activate_file([{}]);".format(",".join(js_files)), **self.STYLES[style])

        # Add IO SVG (read, write)
        for io_idx, io_type in enumerate(("read", "write")):
            if len(group_id[io_idx]) > 0:
                for process_id in group_id[io_idx]:
                    edge = (node_id, process_id) if io_idx == 0 else (process_id, node_id)

                    js_io = []
                    for orig_id in files_info["{}_lookup".format(io_type)][process_id]:
                        js_io.append("'{}'".format("|".join(orig_id)))

                    if not svg.has_edge(*edge):
                        svg.add_edge(*edge, URL="javascript:activate_io({}, [{}]);".format(io_idx, ",".join(js_io)))

    def _add_fork(self, parent_id, child_id, svg):
        """ Add fork between processes """
        edge = (parent_id, child_id)
        if not svg.has_edge(*edge):
            svg.add_edge(*edge, URL="javascript:activate_fork('{}', '{}');".format("|".join(parent_id), "|".join(child_id)), **self.STYLES["fork"])

    def _add_elements(self, graph, svg, expand_procs=None, collapse_files=True):
        """ Add elements from graph to SVG/JS """
        # Build elements for session chains
        elements = self._build_elements(graph, expand_procs, collapse_files)

        # Render processes
        for process_id in elements["process"]:
            self._add_process(process_id, graph, svg)

        # Render IO groups
        for group_id in elements["io"]:
            self._add_io_group(group_id, elements["io"][group_id], graph, svg)

        # Render forks
        for process_id in elements["process"]:
            process_info = graph["process"][process_id]
            parent_id = (process_info["phost"], str(process_info["parent_start"]), str(process_info["parent_pid"]))

            if parent_id in elements["process"]:
                self._add_fork(parent_id, process_id, svg)

    def virt_render(self, process, handle, options=None):
        """ Render SVG """
        op_filter = self.management.OP_IO | self.management.OP_MOVE  # | self.management.OP_ALL
        graph = self._get_graph(process, options, op_filter=op_filter)

        # Prepare graph
        svg = pygraphviz.AGraph(directed=True, splines="ortho", ranksep=".7", fontsize="20", pad="0.5")
        svg.node_attr["fontsize"] = "12"
        options = {}
        options["expand_procs"] = process.cache_entry.config.get("expand_procs", [])

        self._add_elements(graph, svg, **options)

        svg.layout(prog='dot')
        svg_handle = io.BytesIO()
        svg.draw(path=svg_handle, format="svg")

        # Load template
        fields = {}
        fields["python-target"] = os.path.basename(graph["file"][graph["target"][(0,)]]["paths"]["abs_real"]).encode("utf-8")
        fields["python-path"] = process.cache_entry.file_entry.paths["abs_real"].replace("/", "|").encode("utf-8")
        fields["python-svg"] = svg_handle.getvalue()
        fields["python-graph"] =  self.management.render_json.render_bytes(process).replace(b"\"", b"\\\"")
        fields["python-expanded"] = json.dumps(["|".join(val) for val in options["expand_procs"]]).encode("utf-8").replace(b"\"", b"\\\"")
        fields["python-api"] = self.management.core.configuration.values["api"].encode("utf-8")
        fields["python-suffix"] = (self.management.core.configuration.values["suffix"] * 2).encode("utf-8")
        fields["python-session"] = process.cache_entry.config.get("session", "0").encode("utf-8")

        template_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "render_graphviz.html")

        with open(template_path, "rb") as template_handle:
            template = template_handle.read()

        for field in fields:
            template = template.replace("[{}]".format(field).encode("utf-8"), fields[field])

        # Write to process handle
        handle.write(template)
