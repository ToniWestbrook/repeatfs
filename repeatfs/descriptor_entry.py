#! /usr/bin/env python3

#
#   This file is part of RepeatFS 
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception 
#             


import os
import threading
from repeatfs.file_entry import FileEntry


class DescriptorEntry:
    """ Provides file descriptor information """
    _desc_lookup = dict()
    _pipe_lookup = dict()
    _idx = 1
    _lock = threading.RLock()

    @classmethod
    def get(cls, desc_id):
        """ Get a descriptor entry """
        with cls._lock:
            return cls._desc_lookup.get(desc_id, None)

    @classmethod
    def gen_pipe(cls, pipe, core):
        """ Make pipe, or retrieve pipe descriptor if exists already """
        with cls._lock:
            if pipe in cls._pipe_lookup:
                # Retrieve entry
                desc_entry = cls._desc_lookup[cls._pipe_lookup[pipe]]
            else:
                # Create and register new entry
                file_entry = FileEntry(pipe, core)
                desc_entry = DescriptorEntry(file_entry, None, core)
                cls._pipe_lookup[pipe] = desc_entry.id

        return desc_entry

    def __init__(self, file_entry, flags, core):
        self.core = core
        self.file_entry = file_entry
        self.flags = flags
        self.open_pid = self.core.fuse.fuse_get_context()[2]

        # For non-derived/api file, register real descriptor
        self.fs_lock = threading.RLock()
        self.fs_descriptor = None
        if not file_entry.derived_source and not file_entry.api and flags is not None:
            self.fs_descriptor = os.open(file_entry.paths["abs_real"], flags)

        # Generate thread-safe descriptor index and register
        with DescriptorEntry._lock:
            self.id = DescriptorEntry._idx
            DescriptorEntry._idx += 1
            DescriptorEntry._desc_lookup[self.id] = self

    def __enter__(self):
        """ Allow with block for temporary descriptors """
        return self

    def __exit__(self, etype, evalue, trace):
        """ Remove temporary file descriptor """
        self.remove()

    def __str__(self):
        return "id {0} file_entry {1} flags {2} open_pid {3} fs_desc {4}".format(
            self.id, self.file_entry, self.flags, self.open_pid, self.fs_descriptor)

    def remove(self):
        """ Remove descriptor from descriptor lookup """
        with self._lock:
            del self._desc_lookup[self.id]