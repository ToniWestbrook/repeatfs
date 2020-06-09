#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import json
from repeatfs.provenance.render_base import RenderBase


class RenderJSON(RenderBase):
    """ Render provenance graph as JSON """
    def __init__(self, management):
        self.management = management

    def virt_render(self, process, handle, options=None):
        """ Render JSON """
        graph = self._get_graph(process, options)

        # Convert tuple keys to strings
        str_graph = {}
        for section in graph:
            str_graph[section] = {}
            for key, val in graph[section].items():
                str_key = "|".join(str(kp) for kp in key)
                str_graph[section][str_key] = val

        # Write as JSON
        handle.write(bytes(json.dumps(str_graph).encode("utf8")))
