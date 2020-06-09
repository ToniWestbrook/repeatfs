#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import time
from repeatfs.descriptor_entry import DescriptorEntry
from repeatfs.file_entry import FileEntry


class FileRecord:
    """ Provides file information """
    _lookup = dict()
    _last_cache = dict()
    _dirty_cache = set()

    @classmethod
    def get(cls, descriptor, management):
        """ Get file for a  descriptor """
        with management.lock:
            return cls._lookup.get(descriptor, None)

    @classmethod
    def remove(cls, descriptor, management):
        """ Remove IO for a descriptor """
        with management.lock:
            cls._lookup.pop(descriptor, None)

    @classmethod
    def get_last(cls, file_entry, management):
        """ Get cached latest fcreate of path """
        with management.lock:
            if file_entry.paths["abs_real"] in cls._last_cache:
                return cls._last_cache[file_entry.paths["abs_real"]]

            # Check DB if cache miss
            cursor = management.db_connection.cursor()
            values = (file_entry.paths["abs_real"], )
            cursor.execute("SELECT fcreate FROM file_last WHERE path = ?", values)
            result = cursor.fetchone()

            if result:
                return cls._last_cache.setdefault(file_entry.paths["abs_real"], result["fcreate"])

            # Update cache and DB if first occurrence of file
            return cls.set_last(file_entry, management)

    @classmethod
    def set_last(cls, file_entry, management):
        """ Get cached latest fcreate of path """
        set_time = round(time.time(), 3)

        with management.lock:
            # Update cache
            cls._last_cache[file_entry.paths["abs_real"]] = set_time

            # Update DB
            cursor = management.db_connection.cursor()
            values = (file_entry.paths["abs_real"], set_time)
            cursor.execute("REPLACE INTO file_last VALUES (?, ?)", values)
            management.db_connection.commit()

        return set_time

    def __init__(self, descriptor, management):
        """ Process related provenance information """
        self.management = management
        self.descriptor = descriptor

        # Set file creation time
        file_entry = DescriptorEntry.get(descriptor).file_entry
        self.fcreate = self.get_last(file_entry, self.management)

        # Save record to lookup
        with self.management.lock:
            self._lookup.setdefault(descriptor, self)

    def write(self):
        """ Write IO record to the database """
        file_entry = DescriptorEntry.get(self.descriptor).file_entry

        with self.management.lock:
            if (self.fcreate, file_entry.paths["abs_real"]) in self._dirty_cache:
                return

            cursor = self.management.db_connection.cursor()
            values = (file_entry.paths["abs_real"], self.fcreate, file_entry.file_type)
            cursor.execute("INSERT OR IGNORE INTO file VALUES (?, ?, ?)", values)
            self.management.db_connection.commit()

            self._dirty_cache.add((self.fcreate, file_entry.paths["abs_real"]))
