# RepeatFS
RepeatFS: a file system providing reproducibility through provenance and automation

RepeatFS is a Python, FUSE-based file system with the goal of promoting scientific informatics reproducibility by recording all file and IO operations.  This provenance record can then be exported and used to replicate the analysis on other systems.  During replication, RepeatFS will verify that all software versions, command line parameters, and other relevant attributes match, and will report out any deviation from the original record.  While other provenance software often involves learning scripting languages or migrating your workflow to a confined platform, RepeatFS operates invisibily at the file system level, and is compatible with virtually all Linux/MacOS command-line software.

In addition to replication and verification, RepeatFS also provides provenance visualization.  For any file, it is capable of generating a webpage visualizing the complete provenance history, including all programs that wrote to that file, all files read by those programs, all programs that wrote to those programs, etc.

Lastly, RepeatFS provides Virtual Dynamic Files (VDFs).  These VDFs automatically execute commonly performed tasks (such as converting file types) in a systematic and uniform fashion.  Each supported file will have a corresponding VDF presented on disk.  Upon accessing this file, RepeatFS will run the appropriate task, and populate the file on the fly with the correct information.  These files are then cached in memory, so subsequently accessing them does not require the process to run a second time.  VDFs may be chained together for combining operations, and may be copied to turn them into normal files.

INSTALLATION
--
**Dependencies**
* Python 3 (and pip)
* FUSE 3 (and libfuse)
* GraphViz

Dependencies are available in all popular system package managers.  Python 3, Libfuse, and GraphViz are also available within Anaconda (you may be able to find a channel with FUSE available as well):

* Debian/Ubuntu: `sudo apt install python3 python3-pip fuse3 libfuse3-3 graphviz libgraphviz-dev`
* RHEL/Fedora: `sudo yum install python3 python3-pip fuse fuse-libs graphviz graphviz-devel`
* Anaconda: `conda install -c conda-forge python=3 pip libfuse graphviz`

After the dependencies have been installed, RepeatFS can be installed from PyPI using pip:

```
pip3 install repeatfs
```

If installing using a normal user account, pip will likely install RepeatFS into your `~/.local/bin` directory.  If this directory is in your PATH variable, you can simply run `repeatfs`.  If not, you'll need to add this directory to your PATH variable, or run RepeatFS using the full path `~/.local/bin/repeatfs`.

USAGE
--
RepeatFS functions as a transparent layer between you and your files, recording all IO activity. In order to use RepeatFS, you'll mount the target directory you want to monitor (which includes subdirectories and files).  Then, anytime you wish to access any files within the monitored directory, you'll instead use the path to the RepeatFS mount.

**Mount and monitor a directory**:

```
repeatfs mount <directory to monitor> <RepeatFS mount directory>
```

**Stop monitoring a directory**:

```
fusermount -u <RepeatFS mount directory>
```

The most powerful feature of RepeatFS is the ability to record provenance and replicate the creation of the file on a different system.  To ensure all operations are successfully recorded, be sure to perform the entirety of your analysis using a RepeatFS mount. 

**Path to a file's provenance record** - this is a VDF, and is populated automatically when accessed, and may be copied to any location.  Note the plus sign next to the file name below - all VDFs are available using a plus sign next to the filename:

```
<RepeatFS mount directory>/<any sub directories>/<file name>+/<file name>.provenance.json
```

**Replicate a file** (replication destination must be within an active RepeatFS mount:

```
repeatfs replicate -r <replication destination> <provenance file>
```

**Path to a file's provenance graph** - like the provenance record, this is also a VDF.  RepeatFS visualizes provenance by generating an HTML file that can be vieweed in any browser:
```
<RepeatFS mount directory>/<any sub directories>/<file name>+/<file name>.provenance.html
```



EXAMPLES
--
In this example, we first mount our work directory using RepeatFS.  Then we download a copy of the UniProt SwissProt database, decompress it, and perform two simple tasks: extract fasta header sequences, and count the number of lines in the fasta file.  We take both results, and add it to a new archive.

```
repeatfs mount ~/work ~/mnt
cd ~/mnt

wget ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz
gzip -d uniprot_sprot.fasta.gz
grep ">" uniprot_sprot.fasta > ~/mnt/headers.txt
cat uniprot_sprot.fasta | wc -l > ~/mnt/count.txt
tar -cvf results.tar headers.txt count.txt
```

To visualize the provenance of the `results.tar` file within our browser:

```
cd results.txt+
firefox results.txt.provenance.html
````

![Example 1](https://raw.githubusercontent.com/ToniWestbrook/repeatfs/master/images/example1.png)

RepeatFS can also replicate these steps to recreate `results.tar` using the `results.tar.provenance.json` file.  You can use this file (or distribute it to others) to reproduce your work.  In the following example, we've copied the provenance record into our home directory.  We then mount a directory with RepeatFS and replicate the work (and save stdout and stderr into log files):

```
repeatfs mount ~/replicate ~/mnt
cd ~/mnt

repeatfs replicate ~/results.txt.provenance.json --stdout stdout.log --stderr stderr.log
```
RepeatFS will execute and verify each step

```
[info] Starting replication
[info] Replication complete
[info] Starting verification
[ok] Process 16056 (wget ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz) executed
[ok] Process 16091 (gzip -d uniprot_sprot.fasta.gz) executed
[ok] Process 16107 (grep --color=auto > uniprot_sprot.fasta) executed
[ok] Process 16110 (cat uniprot_sprot.fasta) executed
[ok] Process 16113 (wc -l) executed
[ok] Process 16118 (tar -cvf results.tar headers.txt count.txt) executed
[info] Verification complete
```
