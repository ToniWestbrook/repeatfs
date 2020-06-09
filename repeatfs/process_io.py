#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import io
import operator
import os
import shlex
import stat
import subprocess
import threading
from repeatfs.descriptor_entry import DescriptorEntry


class ProcessIO():
    """ Provides an interface for process IO with a blocking buffer """
    def __init__(self, cache_entry):
        self.cache_entry = cache_entry
        self.process = None
        self.pid_auth = dict()
        self.stream_buffer = None
        self.write_open = True
        self.blocks_byte_pos = 0
        self.reset_pos = 0
        self.lock = threading.Condition()
        self.read_active = False
        self.write_active = False

    def _read_buffer(self, size):
        # Must be called with lock
        block_size = self.cache_entry.core.configuration.values["block_size"]

        try:
            # Directly pass-through BufferedReader (stdout/stderr) streams
            if isinstance(self.stream_buffer, io.BufferedReader):
                ret_data = self.stream_buffer.read(size)
                return ret_data

            # Block until buffer filled or all write modes from source process are (currently) closed
            while True:
                current_size = len(self.stream_buffer.getbuffer())
                if current_size >= block_size or not self.write_open: break
                self.lock.wait()

            # Reset to end of last read
            self.stream_buffer.seek(self.reset_pos)
            ret_data = self.stream_buffer.read(size)

            # If we've read entire block, reset
            if self.stream_buffer.tell() == block_size:
                self.stream_buffer.seek(0)
                self.stream_buffer.truncate()

            # Mark current position as next reset
            self.reset_pos = self.stream_buffer.tell()

            return ret_data

        finally:
            self.lock.notifyAll()

    def _write_buffer(self, data):
        # Must be called with lock
        block_size = self.cache_entry.core.configuration.values["block_size"]

        try:
            # Writes are only valid for BytesIO (file) streams
            if not isinstance(self.stream_buffer, io.BytesIO): return

            # Block until buffer is not full
            while True:
                current_size = len(self.stream_buffer.getbuffer())
                if current_size < block_size: break
                self.lock.wait()

            return self.stream_buffer.write(data)

        finally:
            self.lock.notifyAll()

    # Prepare an internally generated file
    def _internal_prepare(self):
        sys_config = self.cache_entry.core.configuration.values
        file_config = self.cache_entry.core.configuration.actions[self.cache_entry.file_entry.virt_action[:2]]

        if file_config["internal"] is None: return

        # Create the temporary file for the entry output
        path = os.path.join(sys_config["cache_path"], "{0}.temp".format(self.cache_entry.cache_path))
        with os.fdopen(os.open(path, os.O_CREAT | os.O_WRONLY), "ab") as handle:
            method = file_config["internal"][0].split(".")
            operator.attrgetter(".".join(method[1:]))(self)(self, handle, *file_config["internal"][1:])

    # Cleanup internally generated files
    def _internal_cleanup(self):
        sys_config = self.cache_entry.core.configuration.values
        path = os.path.join(sys_config["cache_path"], "{0}.temp".format(self.cache_entry.cache_path))

        if os.path.exists(path):
            os.remove(path)

    # Run command associated with file
    def req_init(self):
        # Must be called with lock
        sys_config = self.cache_entry.core.configuration.values
        file_config = self.cache_entry.core.configuration.actions[self.cache_entry.file_entry.virt_action[:2]]
        match_groups = self.cache_entry.file_entry.virt_action[2]

        # Ignore init request if running or previously completed
        if self.cache_entry.final or self.process: return

        # Reset stream position
        self.blocks_byte_pos = 0

        # Execute internal prepration, if applicable
        self._internal_prepare()

        if self.cache_entry.file_entry.file_type == stat.S_IFREG:
            # Build string replacements
            replacements = dict()
            replacements["input"] = self.cache_entry.file_entry.derived_source.paths["abs_mount"]
            replacements["output"] = self.cache_entry.file_entry.paths["abs_mount"]
            replacements["output_base"] = replacements["output"][:-len(file_config["ext"])]
            replacements["temp"] = "{0}.temp".format(self.cache_entry.cache_path)

            for group_pair in enumerate(match_groups):
                group_idx = "input_{0}".format(group_pair[0])
                replacements[group_idx] = os.path.join(os.path.dirname(replacements["input"]), group_pair[1])

            # Construct the command
            output = file_config["output"]
            command = file_config["cmd"]
            command = command.format(**replacements)
            self.cache_entry.core.log("Running command \"{0}\", directing to {1}".format(command, output), self.cache_entry.core.LOG_DEBUG)

            # Start execution
            stdout_dev = subprocess.PIPE if output == "stdout" else subprocess.DEVNULL
            stderr_dev = subprocess.PIPE if output == "stderr" else subprocess.DEVNULL
            self.process = subprocess.Popen(shlex.split(command), bufsize=sys_config["block_size"], stdout=stdout_dev, stderr=stderr_dev)
            self.pid_auth[self.process.pid] = True

            # Standard streams are pass-through to a file ReadBuffer
            if output == "stdout": self.stream_buffer = self.process.stdout
            if output == "stderr": self.stream_buffer = self.process.stderr
            if output == "file": self.stream_buffer = io.BytesIO(bytes(0))

    # End process if running
    def end_process(self):
        # Must be called with lock
        if self.process is not None:
            self.cache_entry.core.log("Killing process {0} (may already be killed)".format(self.process.pid), self.cache_entry.core.LOG_DEBUG)
            self.process.kill()
            self.process.wait()
            self.write_open = False
            self.process = None
            self.pid_auth.clear()

            # Cleanup internal preparation, if applicable
            self._internal_cleanup()

    # Check if the process is complete
    def check_process(self):
        # Must be called with lock
        if self.process:
            if self.process.poll() is not None:
                self.cache_entry.core.log("Process finalized: {0} {1} {2}".format(self.cache_entry.file_entry.paths["abs_virt"], self.cache_entry.mtime, self.cache_entry.file_entry.virt_mtime), self.cache_entry.core.LOG_DEBUG)
                # self.size = (self.process_next_block - 1) * self.core.configuration.values["block_size"] + block_size
                self.cache_entry.mtime = self.cache_entry.file_entry.virt_mtime
                self.cache_entry.final = True

    def check_lineage(self, pid):
        """ Check to see if pid is in process owner pid lineage """
        self.pid_auth[pid] = False
        current_pid = pid

        while current_pid > 1:
            # Check if current PID in lineage is the owner
            if current_pid == self.process.pid:
                self.pid_auth[pid] = True
                break

            with open("/proc/{0}/stat".format(current_pid), "r") as handle:
                stat_info = handle.readline().split(" ")

                # Find end of process name
                for field_mod in range(len(stat_info) - 1):
                    if stat_info[1 + field_mod].endswith(")"): break

                current_pid = int(stat_info[3 + field_mod])

    # Check if descriptor's open FUSE context (at open) is process owner
    def context_owner(self, descriptor=None, pid=None):
        if descriptor is not None:
            desc_entry = DescriptorEntry.get(descriptor)
            context_pid = desc_entry.open_pid
        elif pid is not None:
            context_pid = pid
        else:
            return False

        if not self.process: return False

        # If context pid isn't recorded in authorization rules, check and update
        if context_pid not in self.pid_auth:
            self.check_lineage(context_pid)

        return self.pid_auth[context_pid]

    # Perform a stream read if available
    def read(self, req_block):
        sys_config = self.cache_entry.core.configuration.values
        block_size = sys_config["block_size"]

        with self.lock:
            while self.read_active:
                self.lock.wait()

            try:
                # Ensure no other reads start when lock is released during blocked read
                self.read_active = True

                process_block = self.blocks_byte_pos // block_size
                process_start = self.blocks_byte_pos % block_size
                process_data = None

                # If process is still running and requested block is at or after process position, read stream up to end of block
                if self.process and req_block >= process_block:
                    process_data = bytearray(self._read_buffer(block_size - process_start))
                    self.blocks_byte_pos += len(process_data)

                    # If that was the final byte in stream, check if process complete
                    if len(process_data) == 0:
                        self.check_process()

                return (process_block, process_start, process_data)

            finally:
                self.read_active = False
                self.lock.notifyAll()

    # Perform a stream write if available, and return length not sent to stream
    def write(self, data, pos, descriptor):
        sys_config = self.cache_entry.core.configuration.values
        block_size = sys_config["block_size"]

        with self.lock:
            while self.write_active:
                self.lock.wait()

            try:
                # Ensure no other writes start when lock is released during blocked read
                self.write_active = True

                # Do not write to stream if not our process or process is complete
                if not self.context_owner(descriptor=descriptor): return len(data)

                # Note portion not sent to stream
                ret_len = self.blocks_byte_pos - pos
                if ret_len > len(data):
                    ret_len = len(data)

                # Send remainder to stream
                if ret_len < len(data):
                    # Fill zeroes if position is after current tell position of buffer
                    abs_tell_pos = self.stream_buffer.tell() + ((self.blocks_byte_pos // block_size) * block_size)
                    if pos > abs_tell_pos:
                        self.truncate(pos, descriptor, True)

                    # Adjust buffer write amount for portion returned for mem cache write
                    buffer_remain = len(data)
                    if pos < self.blocks_byte_pos:
                        buffer_remain -= (self.blocks_byte_pos - pos)
                        self.stream_buffer.seek(0)
                    else:
                        # Position must be within blocks_byte_pos and tell at this point
                        blocks_byte_start = ((self.blocks_byte_pos // block_size) * block_size)
                        self.stream_buffer.seek(pos - blocks_byte_start)

                    while buffer_remain > 0:
                        write_len = buffer_remain
                        if write_len > (block_size - self.stream_buffer.tell()):
                            write_len = (block_size - self.stream_buffer.tell())

                        data_pos = len(data) - buffer_remain
                        self._write_buffer(data[data_pos:data_pos + write_len])
                        buffer_remain -= write_len

                return ret_len

            finally:
                self.write_active = False
                self.lock.notifyAll()

    # Perform a stream truncate if available, return False if truncate should happen in memory cache
    def truncate(self, pos, descriptor, write_call):
        sys_config = self.cache_entry.core.configuration.values
        block_size = sys_config["block_size"]

        with self.lock:
            while self.write_active and not write_call:
                self.lock.wait()

            try:
                # Do not write to stream if not our process or process is complete
                if not self.context_owner(descriptor=descriptor): return False

                # Check if truncate should happen in memory cache
                if pos < self.blocks_byte_pos: return False

                # Calculate truncate position relative to buffer
                trunc_remain = pos - ((self.blocks_byte_pos // block_size) * block_size)

                if trunc_remain < self.stream_buffer.tell():
                    # If before current buffer position, truncate
                    self.stream_buffer.truncate(trunc_remain)
                else:
                    # Otherwise, zero fill
                    trunc_remain -= self.stream_buffer.tell()

                    while trunc_remain > 0:
                        empty_len = trunc_remain
                        if empty_len > (block_size - self.stream_buffer.tell()):
                            empty_len = (block_size - self.stream_buffer.tell())

                        self._write_buffer(bytes(empty_len))
                        trunc_remain -= empty_len

                return True

            finally:
                if not write_call:
                    self.write_active = False
                    self.lock.notifyAll()

    # Close the stream
    def close(self, read, write):
        with self.lock:
            # If no more open reads, end the process
            if read:
                self.end_process()

            # If no more open owner writes, close the stream
            if write:
                self.write_open = False
                self.lock.notifyAll()
