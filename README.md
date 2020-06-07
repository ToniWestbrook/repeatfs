# RepeatFS
RepeatFS: a file system providing reproducibility through provenance and automation

RepeatFS is a Python, FUSE-based file system with the goal of promoting scientific informatics reproducibility by recording all file and IO operations.  This provenance record can then be exported and used to replicate the original file on other systems.  During replication, RepeatFS will verify that all software versions, command line parameters, and other relevant attributes match, and will report out any deviation from the original record.  While other provenance software often involves learning scripting languages or migrating your workflow to a confined platform, RepeatFS operates invisibily at the file system level, and is compatible with virtually all Linux/MacOS command-line software.

In addition to replication and verification, RepeatFS also provides provenance visualization.  For any file, it is capable of generating a webpage visualizing the complete provenance history, including all programs that wrote to that file, all files read by those programs, all programs that wrote to those programs, etc.

Lastly, RepeatFS provides Virtual Dynamic Files (VDFs).  These VDFs automatically execute commonly performed tasks (such as converting file types) in a systematic and uniform fashion.  Each supported file will have a corresponding VDF presented on disk.  Upon accessing this file, RepeatFS will run the appropriate task, and populate the file on the fly with the correct information.  These files are then cached in memory, so subsequently accessing them does not require the process to run a second time.  VDFs may be chained together for combining operations, and may be copied to turn them into normal files.

INSTALLATION
--
**Dependencies**
* Python 3
* FUSE 3
* libfuse
* GraphViz

Dependencies are available in all popular system package managers.  Python 3, Libfuse, and GraphViz are also available within Anaconda (you may be able to find a channel with FUSE available as well):

* Debian/Ubuntu: `sudo apt install python3 fuse3 libfuse-3-3 graphviz`
* RHEL/Fedora: `sudo yum install python3 fuse fuse-libs graphviz`
* Anaconda: `conda install -c conda-forge python=3 libfuse graphviz`

After the dependencies have been installed, RepeatFS can be installed from PyPI using pip:

```
TO BE COMPLETED
```

USAGE
--
RepeatFS functions as a transparent layer between you and your files, recording all IO activity. In order to use RepeatFS, you'll mount the target directory (and all subdirectories/files) you want to monitor.  Then, anytime you wish to access any files within the monitored directory, you'll instead use the path to the RepeatFS mount.

*Mount and monitor a directory*:

```
repeatfs.py mount <directory to monitor> <RepeatFS mount directory>
```

Stop monitoring a directory:

```
umount <RepeatFS mount directory>
```

The most powerful feature of RepeatFS is the ability to record provenance and replicate the creation of the file on a different system.  To ensure all operations are successfully recorded, be sure to perform the entirety of your analysis using a RepeatFS mount. 

Path to a file's provenance - this is a VDF, and is populated automatically when accessed, and may be copied to any location.  Note the plus sign next to the file name below - all VDFs are available using a plus sign next to the filename:

```
<RepeatFS mount directory>/<any sub directories>/<file name>+/<file name>.provenance.json
```

Replicate a file (replication destination must be within an active RepeatFS mount:

```
repeatfs.py replicate -r <replication destination> <provenance file>
```

EXAMPLES
--
