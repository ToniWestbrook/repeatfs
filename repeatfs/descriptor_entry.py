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
    _file_lookup = dict()
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

    @classmethod
    def rename(cls, old_abs, new_virt, core):
        """ Update existing descriptors with new path information """
        with DescriptorEntry._lock:
            if old_abs not in cls._file_lookup:
                return

            file_entry = FileEntry(new_virt, core)
            cls._file_lookup.setdefault(file_entry.paths["abs_real"], set())

            # Update any open descriptors with new path info
            for desc_id in cls._file_lookup[old_abs]:
                cls.get(desc_id).file_entry = file_entry
                cls._file_lookup[file_entry.paths["abs_real"]].add(desc_id)

            del cls._file_lookup[old_abs]

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
            DescriptorEntry._file_lookup.setdefault(file_entry.paths["abs_real"], set()).add(self.id)

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
        """ Remove descriptor from descriptor and file lookups """
        with DescriptorEntry._lock:
            DescriptorEntry._file_lookup[self.file_entry.paths["abs_real"]].remove(self.id)
            if not DescriptorEntry._file_lookup[self.file_entry.paths["abs_real"]]:
                del DescriptorEntry._file_lookup[self.file_entry.paths["abs_real"]]

            del DescriptorEntry._desc_lookup[self.id]
