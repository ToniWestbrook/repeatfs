#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import io
import os
from collections import deque
from repeatfs.descriptor_entry import DescriptorEntry
from repeatfs.file_entry import FileEntry


class RenderBase:
    """ Render provenance functionality """
    def _get_latest_entry(self, process):
        """ Get latest file entry associated with request process """
        file_path = process.cache_entry.file_entry.derived_source.paths["abs_real"]

        with self.management.lock:
            cursor = self.management.db_connection.cursor()
            cursor.execute("SELECT * FROM file_last WHERE path = ?", (file_path, ))
            result = cursor.fetchone()

        return result

    def _get_graph(self, process, options, op_filter=None):
        """ Build and finalize graph for given process """
        # Lookup active version of requested file
        target_id = self._get_latest_entry(process)

        # Build provenance graph
        return self.management.graph.build_graph(target_id, op_filter=op_filter)

    def virt_render(self, process, handle, options=None):
        handle.write(b"Not implemented\n")

    def render_bytes(self, process, options=None):
        """ Get render as bytes """
        handle = io.BytesIO()
        self.virt_render(process, handle, options)

        return handle.getvalue()
