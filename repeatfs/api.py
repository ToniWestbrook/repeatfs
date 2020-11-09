#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import fuse
import io
import json
import os
import threading
from multiprocessing import Process
from repeatfs.provenance.replication import Replication
from repeatfs.cache_entry import CacheEntry
from repeatfs.descriptor_entry import DescriptorEntry


class API:
    """ Provides API information """
    STATE_START, STATE_EXEC = range(2)
    API_BARE, API_SIMPLE, API_FULL = range(3)

    _session_lookup = dict()
    _lock = threading.RLock()

    @classmethod
    def get(cls, desc_id):
        """ Get a session entry """
        with cls._lock:
            return cls._session_lookup.get(desc_id, None)

    @classmethod
    def request(cls, path, command="", extended={}):
        """ Send JSON request to API """
        # Open handle to API
        fd = os.open(path, os.O_RDWR)

        # If command present, send to API
        if command:
            request_data = dict(extended)
            request_data["command"] = command
            request_json = json.dumps(request_data)
            os.write(fd, (request_json + "\n").encode("utf-8"))

        # Read JSON lines and yield next full response
        while True:
            line = b""
            while True:
                char = os.read(fd, 1)
                if char == b"\n":
                    break

                line += char

            json_response = json.loads(line)
            yield json_response

            if json_response["final"]:
                break

    def __init__(self, desc_id, core):
        self.desc_id = desc_id
        self.core = core
        self.state = API.STATE_START
        self.cmd_info = None
        self.exec_lock = threading.RLock()

        # Create input buffer and output pipe descriptors
        self.input = io.StringIO()
        self.output_r, self.output_w = os.pipe()

        # Generate thread-safe session index and register
        with self._lock:
            self._session_lookup[self.desc_id] = self

        # Immediately execute inline commands
        file_entry = DescriptorEntry.get(self.desc_id).file_entry
        if file_entry.inline_cmd:
            self.write("{}\n".format(file_entry.inline_cmd).encode("utf-8"))

    def __str__(self):
        return "id {0} input {1} {2} output {3} {4}".format(
            self.desc_id, self.input[0], self.input[1], self.output[0], self.output[1])

    def get_commands(self):
        """ API command lookup """
        ret_commands = {
            "shutdown": (fuse.fuse_exit, self.API_BARE),
            "config_vdf": (CacheEntry.api_config, self.API_SIMPLE),
            "replicate": (Replication.api_receive, self.API_FULL)}

        return ret_commands

    def remove(self):
        """ Remove descriptor from session lookup """
        with self._lock:
            del self._session_lookup[self.desc_id]

    def read(self, length):
        """ Read data from API """
        # Perform no action if command not yet received
        if self.state == self.STATE_START:
            return ""

        return os.read(self.output_r, length)

    def write(self, buf):
        """ Write data to API """
        cmd_lookup = self.get_commands()

        with self.exec_lock:
            # Perform no action if command already received
            if self.state == self.STATE_EXEC:
                return len(buf)

            # Buffer command and check for complete JSON line
            str_buf = buf.decode("utf8")
            self.input.write(str_buf)

            if "\n" in str_buf:
                self.state = self.STATE_EXEC
                self.input.seek(0)

                try:
                    self.cmd_info = json.loads(self.input.readline())
                    cmd_info = cmd_lookup.get(self.cmd_info["command"], None)
                    if cmd_info:
                        if cmd_info[1] == self.API_BARE:
                            # Immediate API with no arg
                            cmd_info[0]()
                        elif cmd_info[1] == self.API_SIMPLE:
                            # Immediate API with arg
                            cmd_info[0](self)
                        else:
                            # Full API command
                            Process(target=cmd_info[0], args=(self, )).start()
                    else:
                        self.respond(status="unknown", message=self.cmd_info["command"])

                except json.decoder.JSONDecodeError:
                    self.respond(status="malformed", message=self.input.getvalue())

                except Exception as e:
                    self.respond(status="error", message=e)

            return len(buf)

    def respond(self, status="", message="", extended={}, final=True):
        """ Send JSON response to client """
        # Build response dictionary
        response_data = dict(extended)
        response_data.update({"status": status, "message": message, "final": final})

        # Send JSON response line and close pipe if finalized
        os.write(self.output_w, (json.dumps(response_data) + "\n").encode("utf-8"))
        if final:
            os.close(self.output_w)
