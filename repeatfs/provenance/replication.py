#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import functools
import json
import os
import subprocess
import threading
from repeatfs.provenance.process_record import ProcessRecord
from multiprocessing import Queue


class Replication:
    """ Recreate and evaluate provenance replication """
    @classmethod
    def api_receive(cls, api_out):
        """ API: Receive replication related command """
        # Attempt to build replication
        try:
            # Deserialize JSON data and call action
            graph_raw = json.loads(api_out.cmd_info["provenance"])
            provenance = {section: {tuple(k.split("|")): v for (k, v) in graph_raw[section].items()} for section in graph_raw}
            expand = set(tuple(key.split("|")) for key in api_out.cmd_info["expand"])
            replication = Replication(provenance, api_out.core, expand=expand, api_out=api_out)

            getattr(replication, "action_{}".format(api_out.cmd_info["action"]))()

        except json.decoder.JSONDecodeError:
            api_out.respond(status="malformed", message=api_out.cmd_info["provenance"])

        except Exception as e:
            api_out.respond(status="error", message=e)

    def __init__(self, provenance, core, expand=None, api_out=None):
        """ Create replication definition from provenance """
        self.provenance = provenance
        self.core = core
        self.expand = expand if expand else []
        self.api_out = api_out
        self.lock = threading.RLock()

    def _send_stream(self, stream, name):
        """ Send output of stream to client """
        for line in iter(stream.readline, b""):
            extended = {name: line.decode("utf-8")}

            # Ensure output pipe isn't written by both streams simultaneously
            with self.lock:
                self.api_out.respond(status="ok", message="", extended=extended, final=False)

    def is_redirection(self, path):
        """ Check if path is redirection (non-pipe/pty) """
        check_abs = path.startswith(self.core.mount)
        check_rel = not (path.startswith("/") or (":" in path and "/" not in path))

        return check_abs or check_rel

    def _build_command(self, chain):
        """ Build shell command from chain """
        # Connect multiple commands by pipes
        processes = self.provenance["process"]

        commands = []
        for process_id in chain:
            command = processes[process_id]["cmd"][0]
            for arg in processes[process_id]["cmd"][1:]:
                if " " not in arg:
                    command += " {}".format(arg)
                else:
                    if "\"" in arg:
                        command += " '{}'".format(arg)
                    else:
                        command += " \"{}\"".format(arg)
            commands.append(command)

        # Connect multiple commands by pipes
        pipe_cmds = " | ".join(commands)

        # Add file redirection
        for stream in ("stdin", "stdout", "stderr"):
            process = processes[chain[0]] if stream == "stdin" else processes[chain[-1]]
            redirect = process[stream].replace("$$$", self.core.mount)

            # Check for file redirection (TTY and special files fail)
            check_abs = redirect.startswith(self.core.mount)
            check_rel = not (redirect.startswith("/") or ":" in redirect)

            if check_abs or check_rel:
                if stream == "stdin":
                    pipe_cmds += " < {}".format(redirect)
                else:
                    operator = ">" if process["trunc_{}".format(stream)] else ">>"
                    pipe_cmds += " {0} {1}".format(operator, redirect)

        return pipe_cmds

    def _build_chain(self, process_id):
        """ Retrieve full chain of piped commands for any specific command """
        processes = self.provenance["process"]
        chain = []

        # Find left-most member of chain
        left_proc = process_id

        while processes[left_proc]["stdin"].startswith("pipe:"):
            # Note current iteration's process
            current_proc = left_proc

            # Search other processes for other end of pipe
            for search_proc in processes:
                if left_proc == search_proc:
                    continue

                stdio = (processes[search_proc]["stdout"], processes[search_proc]["stderr"])
                if processes[left_proc]["stdin"] in stdio:
                    left_proc = search_proc
                    break

            # Stop if the other end of the pipe wasn't found
            if left_proc == current_proc:
                break

        # Build chain from left-most member
        right_proc = left_proc
        chain.append(right_proc)

        while processes[right_proc]["stdout"].startswith("pipe:") or processes[right_proc]["stderr"].startswith("pipe:"):
            # Note current iteration's process
            current_proc = right_proc

            # Search other processes for other end of pipe
            for search_proc in self.provenance["process"]:
                if right_proc == search_proc:
                    continue

                if not processes[search_proc]["stdin"].startswith("pipe:"):
                    continue

                stdio = (processes[right_proc]["stdout"], processes[right_proc]["stderr"])
                if processes[search_proc]["stdin"] in stdio:
                    right_proc = search_proc
                    break

            # Stop if the other end of the pipe wasn't found
            if right_proc == current_proc:
                break

            # Return hashable tuple
            chain.append(right_proc)

        return tuple(chain)

    def _execute_chain(self, chain):
        """ Execute a process chain """
        # Associate replicated pid with original pid
        def register_pid(orig_proc_id):
            stat_info = ProcessRecord.get_stat_info(os.getpid(), self.core.provenance)
            stat_info["pid"] = os.getpid()

            process_map_mp.put((orig_proc_id, stat_info))
            process_map_mp.close()
            process_map_mp.join_thread()

        process_map_mp = Queue()
        processes = self.provenance["process"]
        run_procs = []

        for process_idx, process_id in enumerate(chain):
            process = processes[process_id]
            stdio_handles = {stdio: subprocess.PIPE for stdio in ("stdin", "stdout", "stderr")}

            # Check CWD
            if process["cwd"]["rel_mount"] is None:
                self.api_out.respond(status="warning", message="Process {} CWD was outside of file system".format(process["pid"]), final=False)
                cwd = self.core.mount
            else:
                cwd = os.path.join(self.core.mount, process["cwd"]["rel_mount"])

            # Redirect stdio
            for stdio in stdio_handles:
                # Setup file redirection
                if self.is_redirection(process[stdio]):
                    if stdio == "stdin":
                        mode = "r"
                    else:
                        mode = "w" if process["trunc_{}".format(stdio)] else "a"

                    stdio_path = process[stdio].replace("$$$", self.core.mount)
                    stdio_handles[stdio] = open(stdio_path, mode)

                # Setup pipes
                elif process[stdio].startswith("pipe:"):
                    if stdio == "stdin":
                        # Check that we don't have an orphan pipe
                        if process_idx > 0:
                            if process["stdin"] == processes[chain[process_idx - 1]]["stdout"]:
                                stdio_handles[stdio] = run_procs[-1].stdout
                            if process["stdin"] == processes[chain[process_idx - 1]]["stderr"]:
                                stdio_handles[stdio] = run_procs[-1].stderr
                    elif stdio == "stderr":
                        # Check for combined stdout/stderr pipe
                        if process["stdout"] == process["stderr"]:
                            stdio_handles[stdio] = subprocess.STDOUT

            args = []
            for arg in process["cmd"]:
                args.append(arg.replace("$$$", self.core.mount))

            preexec = functools.partial(register_pid, process_id)
            run_proc = subprocess.Popen(args, cwd=cwd, shell=False, preexec_fn=preexec, **stdio_handles)
            run_procs.append(run_proc)

            # Show stdout/stderr if not redirected
            for stdio in ("stdout", "stderr"):
                if (stdio_handles[stdio] == subprocess.PIPE) and (not process[stdio].startswith("pipe:")):
                    threading.Thread(target=self._send_stream, args=[getattr(run_proc, stdio), stdio]).start()

        # Wait for completion of all processes
        for run_proc in run_procs:
            run_proc.wait()

        # Close handles
        for run_proc in run_procs:
            for stdio in ("stdin", "stdout", "stderr"):
                if getattr(run_proc, stdio):
                    getattr(run_proc, stdio).close()

        # Convert process map queue to dictionary
        process_map = {}

        for _ in range(len(run_procs)):
            proc_orig_id, proc_repl_info = process_map_mp.get()
            process_map[proc_orig_id] = proc_repl_info

        return process_map

    def _verify_child_match(self, orig_id, repl_id, repl_provenance):
        """ Attempt to match original children to replicated children """
        child_map = []

        # Get children (orig, repl), ordered by time
        proc_ids = (orig_id, repl_id)
        children = [[], []]
        provenance = (self.provenance, repl_provenance)

        for prov_idx in range(len(provenance)):
            for child_id in provenance[prov_idx]["process"]:
                child_process = provenance[prov_idx]["process"][child_id]
                if proc_ids[prov_idx] == (child_process["phost"], str(child_process["parent_start"]), str(child_process["parent_pid"])):
                    children[prov_idx].append(child_id)

            children[prov_idx] = sorted(children[prov_idx])

        # Check for same number of children
        if len(children[0]) != len(children[1]):
            return None

        # Check for same executed commands in order
        for idx in range(len(children[0])):
            child_orig_id = children[0][idx]
            child_repl_id = children[1][idx]
            child_orig_proc = provenance[0]["process"][child_orig_id]
            child_repl_proc = provenance[1]["process"][child_repl_id]

            if child_orig_proc["exe"].split(os.sep)[-1] != child_repl_proc["exe"].split(os.sep)[-1]:
                return None

            child_map.append((child_orig_id, child_repl_proc))

        # All checks passed, return mapping
        return child_map

    def _verify_execution(self, process_map):
        """ Verify replication matched original """
        procs_remaining = list(process_map.items())

        # Find current version of replicated file
        repl_target = self.provenance["file"][tuple(self.provenance["target"][("0",)])]
        repl_path = os.path.join(self.core.root, repl_target["paths"]["rel_mount"])

        with self.core.provenance.lock:
            cursor = self.core.provenance.db_connection.cursor()
            cursor.execute("SELECT * FROM file_last WHERE path = ?", (repl_path, ))
            result = cursor.fetchone()

        if not result:
            self.api_out.respond(status="warning", message="Target file does not appear in replicated provenance, check output for errors", final=False)
            return

        # Build provenance graph for target file
        repl_provenance = self.core.provenance.graph.build_graph(result)

        # Check all processes
        while procs_remaining:
            # Lookup provenance for original and replicated process
            orig_id, repl_info = procs_remaining.pop(0)
            repl_id = (self.core.provenance.system_name, str(repl_info["pstart"]), str(repl_info["pid"]))
            orig_process = self.provenance["process"][orig_id]

            # Verify process ran and was recorded to provenance
            if repl_id not in repl_provenance["process"]:
                self.api_out.respond(status="warning", message="Process {} ({}) does not appear in replicated provenance".format(repl_id[2], " ".join(orig_process["cmd"])), final=False)
                continue

            repl_process = repl_provenance["process"][repl_id]
            repl_cmd = " ".join(repl_process["cmd"])
            self.api_out.respond(status="ok", message="Process {} ({}) executed".format(repl_id[2], repl_cmd), final=False)

            # Verify MD5 checksums match
            if orig_process["hash"] != repl_process["hash"]:
                self.api_out.respond(status="warning", message="Process {} ({}) different version than original".format(repl_id[2], repl_cmd), final=False)

            # Queue children to be verified
            child_map = self._verify_child_match(orig_id, repl_id, repl_provenance)

            if child_map is None:
                self.api_out.respond(status="warning", message="Unable to find children of process {} ({}) ".format(repl_id[2], repl_cmd), final=False)
                continue

            procs_remaining.extend(child_map)

    def get_session_chains(self, filter_expanded):
        """ Get ordered list of process chains for session children """
        session_chains = set()

        # Expand all session leaders and manual requests
        potential_leaders = set(self.provenance["session"].keys())
        expand_remain = set(potential_leaders)
        expand_remain.update(self.expand)
        session_children = set()

        # Find all session children
        while True:
            # Check if expansion is complete
            expand_current = expand_remain.intersection(potential_leaders)
            if len(expand_current) == 0:
                break

            # Find children of current expansion targets
            for process_id, process_info in self.provenance["process"].items():
                parent_id = tuple(str(process_info[key]) for key in ["phost", "parent_start", "parent_pid"])

                if parent_id in expand_current:
                    # Ensure thread leader
                    if (process_info["pstart"], process_info["pid"]) == (process_info["tgid_start"], process_info["tgid"]):
                        # Add process to potential leaders, and note as session child
                        potential_leaders.add(process_id)
                        session_children.add(process_id)

                        # Remove parent process from children if requested
                        if filter_expanded:
                            session_children.discard(parent_id)

                    # Update remaining
                    expand_remain.discard(parent_id)

        # Build chains for all session children
        for process_id in session_children:
            chain = self._build_chain(process_id)
            session_chains.add(chain)

        # Sort processes by start time of first member in chain
        return sorted(session_chains, key=lambda x: self.provenance["process"][x[0]]["pstart"])

    def trace_session_child(self, process_id, session_chains):
        """ Get the session child the process belongs to """
        child_id = process_id

        # Follow tree until root process
        while child_id[2] != "1":
            process = self.provenance["process"][child_id]
            leader = (process["tgid_start"], process["tgid"]) == (process["pstart"], process["pid"])

            if leader:
                # Process is thread group leader
                for chain in session_chains:
                    if child_id in chain:
                        return child_id

                child_id = (process["phost"], str(process["parent_start"]), str(process["parent_pid"]))
            else:
                # Process is thread
                child_id = (process["phost"], str(process["tgid_start"]), str(process["tgid"]))

        return None

    def action_list_cmds(self):
        """ List commands to be executed """
        if self.api_out is None:
            return

        # Obtain processes spawned by session leaders
        session_chains = self.get_session_chains(filter_expanded=True)

        self.api_out.respond(status="ok", message="Command List", final=False)

        for chain in session_chains:
            ids = ", ".join(["|".join(process) for process in chain])
            message = "[{}] {}".format(ids, self._build_command(chain))
            self.api_out.respond(status="ok", message=message, extended={"commands": session_chains}, final=False)

        self.api_out.respond(status="ok", message="", final=True)

    def action_replicate(self):
        """ Execute replication """
        if self.api_out.respond is None:
            return

        self.api_out.respond(status="info", message="Starting replication", final=False)

        # Obtain process chains spawned by session leaders
        session_chains = self.get_session_chains(filter_expanded=True)

        # Execute chains and record process map (orig->repl)
        process_map = {}
        for chain in session_chains:
            process_map.update(self._execute_chain(chain))

        self.api_out.respond(status="info", message="Replication complete", final=False)

        # Verify replication
        self.api_out.respond(status="info", message="Starting verification", final=False)
        self._verify_execution(process_map)
        self.api_out.respond(status="info", message="Verification complete", final=True)
