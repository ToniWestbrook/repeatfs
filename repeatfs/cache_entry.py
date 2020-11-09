#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import hashlib
import io
import json
import operator
import os
import queue
import subprocess
import threading
import time
from repeatfs.descriptor_entry import DescriptorEntry
from repeatfs.process_io import ProcessIO


class CacheEntry:
    """ Provides functionality for caching virtual files to memory and disk """
    BLOCK_DATA, BLOCK_DIRTY = range(2)
    IO_READ, IO_WRITE, IO_TRUNCATE, IO_RESET = range(4)

    entries = dict()
    block_history = queue.Queue()

    @classmethod
    def api_config(cls, api_out):
        """ API: Receive configure command """
        # Attempt to configure VDF
        try:
            # Check and clean path
            for field in ("path", "options"):
                if field not in api_out.cmd_info:
                    api_out.respond(status="error", message="{} not specified".format(field))
                    return

            path = api_out.cmd_info["path"].replace("|", "/")
            if path not in CacheEntry.entries:
                    api_out.respond(status="error", message="invalid path specified")
                    return

            # Check, clean, and update configuration
            options = api_out.cmd_info["options"]
            if "expand_procs" in options:
                options["expand_procs"] = [tuple(val.split("|")) for val in options["expand_procs"]]

            cache_entry = CacheEntry.entries[path]
            cache_entry.update_config(api_out.cmd_info["options"])

            # Clear cache
            cache_entry.io(CacheEntry.IO_RESET, 0, None, 1, api_out.desc_id)

            api_out.respond(status="ok", message="", final=True)

        except json.decoder.JSONDecodeError:
            api_out.respond(status="malformed", message=api_out.cmd_info["options"])

        except Exception as e:
            api_out.respond(status="error", message=e)

    def __init__(self, core, entry):
        self.core = core

        # One-time intialization
        self.lock = threading.Condition()
        self.blocks = dict()
        self.waiting = dict()
        self.config = dict()
        self.descriptors = set()
        self.process_io = ProcessIO(self)

        # Reset-time intialization: blocks, blocks_block_pos, file_entry, size, mtime, final, cache_path
        self.reset_cache(entry)

    def _io_read(self, block_idx, block_pos, size, ret_data, ret_size):
        # Must be called with lock
        block_data = self.blocks[block_idx][self.BLOCK_DATA]

        # Calculate the amount that can be retrieved from this block
        consume_size = size - ret_size
        avail_size = len(block_data[block_pos:block_pos + consume_size])
        if consume_size > avail_size: consume_size = avail_size

        # Retrieve data
        ret_data[ret_size:ret_size + consume_size] = self.blocks[block_idx][self.BLOCK_DATA][block_pos:block_pos + consume_size]
        ret_size += consume_size

        return ret_data, ret_size

    def _io_write(self, block_idx, block_pos, new_data, new_dirty, ret_size):
        # Must be called with lock
        block_size = self.core.configuration.values["block_size"]

        block_data = bytearray()
        if block_idx in self.blocks:
            block_data = self.blocks[block_idx][self.BLOCK_DATA]

        # Calculate the amount that can be written to this block
        consume_size = len(new_data) - ret_size
        avail_size = block_size - block_pos
        if consume_size > avail_size: consume_size = avail_size

        # Write data
        block_data[block_pos:block_pos + consume_size] = new_data[ret_size:ret_size + consume_size]
        self.blocks[block_idx] = (block_data, new_dirty)
        ret_size += consume_size

        # Note write in history
        self.block_history.put((self, block_idx))

        # Update current block and file size
        if block_idx + 1 > self.blocks_block_pos:
            self.blocks_block_pos = block_idx + 1

        # Update file size if this is the last block
        if block_idx + 1 == self.blocks_block_pos:
            self.size = block_idx * block_size + len(block_data)

        return ret_size

    def _io_truncate(self, block_idx, block_pos, ret_size):
        # Must be called with lock
        block_size = self.core.configuration.values["block_size"]

        # Delete from position onward
        for del_idx in range(block_idx + 1, self.blocks_block_pos):
            del self.blocks[del_idx]

        # Delete up to position in last block, mark dirty
        self.blocks[block_idx][self.BLOCK_DATA][:] = self.blocks[block_idx][self.BLOCK_DATA][:block_pos]
        self.blocks[block_idx] = (self.blocks[block_idx][self.BLOCK_DATA], True)

        # Update block position and size
        self.blocks_block_pos = block_idx + 1
        self.size = block_idx * block_size + block_pos

        return ret_size

    # TODO: Make 0 blocks "virtual", note only - do not create bytearrays/take up memory
    def _io_fill(self, block_idx, block_pos):
        # Must be called with lock
        block_size = self.core.configuration.values["block_size"]

        # Fill blocks between current and end
        for fill_idx in range(self.blocks_block_pos - 1, block_idx):
            # No previous blocks if file is empty
            if fill_idx == -1: continue

            if fill_idx == self.blocks_block_pos - 1:
                # Fill the remainder of current last block
                block_data = self.blocks[fill_idx][self.BLOCK_DATA]
                block_data.extend(bytearray(block_size - len(block_data)))
                self.blocks[fill_idx] = (block_data, True)
            else:
                # Create new block
                self.blocks[fill_idx] = (bytearray(block_size), True)

        # Fill in bytes in new last block
        if block_idx not in self.blocks:
            block_data = bytearray()
        else:
            block_data = self.blocks[block_idx][self.BLOCK_DATA]

        block_data.extend(bytearray(block_pos - len(block_data)))
        self.blocks[block_idx] = (block_data, True)

        # Update block position and size
        self.blocks_block_pos = block_idx + 1
        self.size = block_idx * block_size + block_pos

    def io(self, operation, pos, data, size, descriptor):
        sys_config = self.core.configuration.values
        desc_entry = DescriptorEntry.get(descriptor)
        ret_data = bytearray(size)
        ret_size = 0

        while ret_size < size:
            block_size = sys_config["block_size"]
            block = (pos + ret_size) // block_size
            start = (pos + ret_size) % block_size

            # Phase 1 (Availability, Resets) - wait for IO priority
            self.core.log("IO loop phase 1: op {0}, block {1} start {2} ret_size {3}".format(operation, block, start, ret_size), self.core.LOG_DEBUG)
            with self.lock:
                self.priority_wait(block, descriptor, operation)

                try:
                    # Immediately process cache resets, then exit
                    if operation == self.IO_RESET:
                        self.reset_cache(desc_entry.file_entry)
                        return size

                    # Request a block if not available or not full (may also finalize file)
                    req_block = (block not in self.blocks) or len(self.blocks[block][self.BLOCK_DATA]) < block_size
                finally:
                    self.lock.notify_all()

            # Phase 2 (Fetch from stream/disk into memory cache)
            if req_block:
                self.core.log("IO loop phase 2", self.core.LOG_DEBUG)
                self.check_expired()
                self.req_mem_block(block, descriptor, operation)

            # Phase 3 (Perform partial and full IO to/from memory cache)
            self.core.log("IO loop phase 3", self.core.LOG_DEBUG)
            with self.lock:
                self.priority_wait(block, descriptor, operation)

                try:
                    # Handle partial blocks
                    if operation == self.IO_READ:
                        # For reads, return available data if past EOF
                        if self.final and (pos + ret_size) >= self.size:
                            return bytes(ret_data[:ret_size])

                    if operation == self.IO_WRITE or operation == self.IO_TRUNCATE:
                        # For writes, grow file past EOF
                        if pos > self.size:
                            self._io_fill(block, start)

                    # If block is now available, perform IO on it
                    if block in self.blocks:
                        # Perform IO operation
                        if operation == self.IO_READ:
                            ret_data, ret_size = self._io_read(block, start, size, ret_data, ret_size)

                        if operation == self.IO_WRITE:
                            ret_size = self._io_write(block, start, data, True, ret_size)

                        if operation == self.IO_TRUNCATE:
                            ret_size = self._io_truncate(block, start, size)

                finally:
                    self.core.log("IO loop complete, notifying all", self.core.LOG_DEBUG)
                    self.lock.notify_all()

        return bytes(ret_data) if operation == self.IO_READ else ret_size

    # Is descriptor in read mode
    def is_descriptor_read(self, descriptor):
        desc_entry = DescriptorEntry.get(descriptor)
        return ((desc_entry.flags % 2) == 0)

    # Is descriptor in write mode
    def is_descriptor_write(self, descriptor):
        desc_entry = DescriptorEntry.get(descriptor)
        return ((desc_entry.flags & 0x3) > 0)

    # Register a descriptor, check for owner writes
    def register_descriptor(self, descriptor):
        desc_entry = DescriptorEntry.get(descriptor)
        with self.lock:
            # Register descriptor in set
            self.core.log("Registering descriptor {0}".format(descriptor), self.core.LOG_DEBUG)
            self.descriptors.add(descriptor)

            # Check for owner write
            desc_write = self.process_io.context_owner(descriptor=descriptor)
            desc_write &= self.core.is_flag_write(desc_entry.flags)

            if desc_write:
                self.process_io.write_open = True

    # Unregister descriptor, check for last read/write
    def unregister_descriptor(self, descriptor):
        reads = False
        writes = False

        with self.lock:
            # Unregister descriptor from set and waiting
            self.descriptors.remove(descriptor)
            self.waiting.pop(descriptor, None)

            # For for remaining reads/writes
            for desc_remain in self.descriptors:
                desc_entry = DescriptorEntry.get(desc_remain)

                desc_read = self.core.is_flag_read(desc_entry.flags)
                reads |= desc_read

                desc_write = self.process_io.context_owner(descriptor=desc_entry.id)
                desc_write &= self.core.is_flag_write(desc_entry.flags)
                writes |= desc_write

            # Close stream if no further reads or writes
            self.process_io.close(not reads, not writes)
            self.lock.notifyAll()

    # Reset file's cache
    def reset_cache(self, file_entry):
        # Must be called with lock (or from constructor)

        # Initialize memory cache
        self.blocks.clear()
        self.blocks_block_pos = 0
        self.file_entry = file_entry
        self.size = 0
        self.mtime = 0
        self.final = False

        # Compute disk cache location
        hash = hashlib.md5()
        hash.update(self.file_entry.paths["abs_real"].encode("utf8"))
        self.cache_path = os.path.join(self.core.configuration.values["cache_path"], hash.hexdigest())

        # Initialize disk cache
        cache_handle = open(self.cache_path, "w")
        cache_handle.close()

    # Flush a block to disk cache
    def flush_block(self, block_idx):
        # Must be called with lock
        block_size = self.core.configuration.values["block_size"]
        block_data = self.blocks[block_idx][self.BLOCK_DATA]
        block_dirty = self.blocks[block_idx][self.BLOCK_DIRTY]

        # Skip flush if disk_cache is disabld or clean
        if not block_dirty: return

        end = (block_idx * block_size + len(block_data)) == self.size
        self.set_disk_block(block_idx, block_data, end)

    # Flush expired blocks from memory to disk cache
    def check_expired(self):
        block_size = self.core.configuration.values["block_size"]
        max_blocks = self.core.configuration.values["store_size"] // block_size

        if self.block_history.qsize() >= max_blocks:
            self.core.log("Initiating cache flush", self.core.LOG_DEBUG)
            while self.block_history.qsize() > max_blocks // 2:
                entry_pair = self.block_history.get()

                with entry_pair[0].lock:
                    if entry_pair[1] in entry_pair[0].blocks:
                        # Flush dirty blocks to disk cache
                        entry_pair[0].flush_block(entry_pair[1])

                        # Delete block from memory cache
                        del entry_pair[0].blocks[entry_pair[1]]

    # Wait until block operation has priority
    def priority_wait(self, block, descriptor, operation):
        sys_config = self.core.configuration.values

        # Must be called with lock
        while True:
            # Only prioritize reads (for now)
            if operation != self.IO_READ: return

            self.waiting[descriptor] = (block, time.time())

            # Priority to reads requesting blocks already cached
            if block in self.blocks: return

            # Priority to reads early in file (without timeout)
            min_block = block
            for block_pair in self.waiting.values():
                if time.time() - block_pair[1] < sys_config["read_timeout"]:
                    if block_pair[0] < min_block:
                        min_block = block_pair[0]

            if block == min_block: return

            self.lock.wait(sys_config["read_timeout"])

    # Get disk block
    def get_disk_block(self, block_idx):
        block_size = self.core.configuration.values["block_size"]
        file_size = os.path.getsize(self.cache_path)

        with open(self.cache_path, "rb") as cache_handle:
            cache_handle.seek(block_size * block_idx)
            block_data = cache_handle.read(block_size)
            end = (cache_handle.tell() == file_size)

            return (block_data, end)

    # Set disk block
    def set_disk_block(self, block_idx, block_data, end):
        byte_pos = self.core.configuration.values["block_size"] * block_idx
        file_size = os.path.getsize(self.cache_path)

        with open(self.cache_path, "r+b") as cache_handle:
            # Ensure data exists up to location
            if file_size < byte_pos:
                cache_handle.seek(0, 2)
                cache_handle.write(bytearray(byte_pos - file_size))

            # Write block
            cache_handle.seek(byte_pos, 0)
            cache_handle.write(block_data)

            # Truncate if last block
            if end: cache_handle.truncate()

    # Load a block into the memory cache using requested block as hint
    def req_mem_block(self, req_block, descriptor, operation):
        # Must be called with lock
        block_size = self.core.configuration.values["block_size"]

        # Request process init (will ignore if already running/complete)
        self.process_io.req_init()

        # Attempt to load from disk cache before reading from the process
        disk_data = None

        with self.lock:
            # If requested block is before current block (and wasn't already fetched), fetch from disk
            if req_block not in self.blocks and req_block < self.blocks_block_pos:
                disk_data = self.get_disk_block(req_block)[0]
                self._io_write(req_block, 0, disk_data, False, 0)

                # If this wasn't a full block, also attempt process IO
                if len(disk_data) == block_size:
                    return

        # For read operations from non-owner, if not in the disk cache, fetch more from process
        if not self.process_io.context_owner(descriptor=descriptor) and operation == self.IO_READ:
            process_info = self.process_io.read(req_block)
            process_block, process_start, process_data = process_info

            if process_data:
                with self.lock:
                    self._io_write(process_block, process_start, process_data, True, 0)

    def update_config(self, options):
        """ Update cache entry configuration """
        with self.lock:
            self.config.update(options)
