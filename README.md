# RepeatFS
RepeatFS: a file system providing reproducibility through provenance and automation

RepeatFS is a Python, FUSE-based file system with the goal of promoting scientific informatics reproducibility by recording all file and IO operations.  This provenance record can then be exported and used to replicate the original file on other systems.  During replication, RepeatFS will verify that all software versions, command line parameters, and other relevant attributes match, and will report out any deviation from the original record.  While other provenance software often involves learning scripting languages or migrating your workflow to their platform, RepeatFS operates invisibily at the file system level, and is compatible with virtually all Linux/MacOS command-line software.  
