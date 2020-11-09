#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import os
import re
import stat


class FileEntry:
    @classmethod
    def get_paths(cls, path, root, mount):
        """ Build absolute and relative (to root/mount) paths """
        ret_paths = dict()
        root_term = os.path.join(root, "")
        mount_term = os.path.join(mount, "")

        # Calculate relative path
        ret_paths["relative"] = os.path.join(path, "") #path.rstrip(os.sep)
        ret_paths["orig_type"] = "relative"

        if os.path.isabs(ret_paths["relative"]):
            ret_paths["orig_type"] = "abs_virt"

            # Check for absolute real path
            if ret_paths["relative"].startswith(root_term):
                ret_paths["relative"] = ret_paths["relative"][len(root) + 1:]
                ret_paths["orig_type"] = "abs_real"

            # Check for absolute virtual path
            if ret_paths["relative"].startswith(mount_term):
                ret_paths["relative"] = ret_paths["relative"][len(mount) + 1:]
                ret_paths["orig_type"] = "abs_mount"

        # Set absolute paths
        if ":" in ret_paths["relative"]:
            # Non-disk paths (pipes, future) use relative as absolutes)
            ret_paths["abs_real"] = ret_paths["relative"]
            ret_paths["abs_mount"] = ret_paths["relative"]
            ret_paths["abs_virt"] = ret_paths["relative"]
        else:
            ret_paths["abs_real"] = os.path.join(root_term, ret_paths["relative"])
            ret_paths["abs_mount"] = os.path.join(mount_term, ret_paths["relative"])
            ret_paths["abs_virt"] = os.path.join(os.sep, ret_paths["relative"])

        # Clean paths
        ret_paths["relative"] = ret_paths["relative"].rstrip(os.sep)
        ret_paths["abs_real"] = ret_paths["abs_real"].rstrip(os.sep)
        ret_paths["abs_mount"] = ret_paths["abs_mount"].rstrip(os.sep)
        ret_paths["abs_virt"] = ret_paths["abs_virt"].rstrip(os.sep)

        return ret_paths

    """ Provides meta-data for real and derived files """
    def __init__(self, virt_path, core):
        self.core = core
        self.virt_mtime = 0
        self.init_size = 0
        self.valid = False
        self.api = False
        self.file_type = None
        self.provenance = True
        self.virt_action = None
        self.derived_source = None
        self.derived_actions = dict()

        # Check for inline commands
        inline_sep = self.core.configuration.values["suffix"] * 2
        inline_fields = virt_path.split(inline_sep)
        self.inline_cmd = "".join(inline_fields[1:])

        # Build paths
        virt_path = inline_fields[0]
        self.paths = FileEntry.get_paths(virt_path.lstrip(os.sep), self.core.root, self.core.mount)

        # Check for API
        api_file = os.path.join(os.sep, self.core.configuration.values["api"])

        if self.paths["abs_virt"].endswith(api_file):
            self.file_type = stat.S_IFREG
            self.provenance = False
            self.valid = True
            self.api = True
            return

        # Check for pipes
        if self.paths["abs_virt"].startswith("pipe:"):
            self.file_type = stat.S_IFREG
            self.valid = True
            return

        self._build_entry()

    def __str__(self):
        return "relative {} abs_real {} abs_virt {} type {} action {} derived actions {} valid {}, dervied source:\n{}".format(
            self.paths["relative"], self.paths["abs_real"], self.paths["abs_virt"],
            self.file_type, self.virt_action, self.derived_actions, self.valid, self.derived_source)

    def _build_entry(self):
        """ Calculate details of FileEntry record """
        # Check if a real file or possible derived
        if os.path.lexists(self.paths["abs_real"]):
            # Real path exists
            stats = os.lstat(self.paths["abs_real"])
            self.file_type = stat.S_IFMT(stats.st_mode)
            self.valid = True
            self._populate_time(stats)
            self._populate_actions()
        else:
            virt_dir = os.path.dirname(self.paths["abs_virt"])
            virt_base = os.path.basename(self.paths["abs_virt"])

            # Check for valid derived virtual path
            source_dir = None

            # Must prioritize virtual path as directory, or else nested virtual directories will be recognized as files
            if virt_base.endswith(self.core.configuration.values['suffix']):
                source_dir = self.paths["abs_virt"]
                self.file_type = stat.S_IFDIR
            elif virt_dir.endswith(self.core.configuration.values['suffix']):
                source_dir = virt_dir
                self.file_type = stat.S_IFREG
            else:
                # If neither the full path, nor the parent had a derived suffix, mark invalid
                return

            # If hidden mode and no dot, mark invalid
            source_base = os.path.basename(source_dir)
            if self.core.configuration.values["hidden"] and not source_base.startswith("."):
                return

            # Base directory must have a corresponding file
            source_path = source_dir[:-len(self.core.configuration.values['suffix'])]

            if self.core.configuration.values["hidden"]:
                source_path = os.path.join(os.path.dirname(source_path), os.path.basename(source_path)[1:])

            derived_source = FileEntry(source_path, self.core)
            if not derived_source.valid or (derived_source.derived_source and (derived_source.file_type != stat.S_IFREG)):
                return

            # Consider file successfully derived at this point (allows error handling for new files in derived dirs)
            self.derived_source = derived_source

            if self.file_type == stat.S_IFREG:
                # If potential virtual file, ensure action defined for destination (Dirs are already valid)
                if virt_base not in derived_source.derived_actions:
                    return

                # Set VDF configuration options
                derived_action = derived_source.derived_actions[virt_base][:2]
                self.init_size = self.core.configuration.actions[derived_action]["init_size"]

            self.valid = True
            self._populate_time()
            self._populate_actions()

    def _populate_actions(self):
        """ Populate possible actions for this file """
        virt_base = os.path.basename(self.paths["abs_virt"])

        # Set virtual action if this a derived file
        if (self.file_type == stat.S_IFREG) and self.derived_source:
            self.virt_action = self.derived_source.derived_actions[virt_base]

        for action in self.core.configuration.actions:
            if not self.derived_source or (self.file_type == stat.S_IFREG):
                # For any files or real directories, derived actions are based off file itself (virt_base)
                current_base = virt_base
            else:
                # For derived directories, derived actions are based off target file (target_base)
                current_base = os.path.basename(self.derived_source.paths["abs_virt"])

            # Add derived action if regex matches
            match = re.search(action[0], current_base)
            if match:
                action_name = current_base + action[1]
                self.derived_actions[action_name] = action + (match.groups(), )

    def _populate_time(self, stats=None):
        """ Populate modified time for this file """
        if self.derived_source:
            self.virt_mtime = self.derived_source.virt_mtime
        else:
            self.virt_mtime = stats.st_mtime
