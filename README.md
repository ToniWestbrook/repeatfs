# RepeatFS
RepeatFS: a file system providing reproducibility through provenance and automation

RepeatFS is a Python, FUSE-based file system with the goal of promoting scientific informatics reproducibility by recording all file and IO operations during analysis.  This provenance record can then be exported and used to replicate the analysis on other systems.  During replication, RepeatFS will verify that all software versions, command line parameters, and other relevant attributes match, and will report any deviation from the original record.  While other provenance software often involves learning scripting languages or migrating your workflow to a confined platform, RepeatFS operates invisibly at the file system level and is compatible with virtually all Linux command-line software.

In addition to replication and verification, RepeatFS also provides provenance visualization.  It is capable of generating a webpage visualizing the complete provenance history, including all programs that wrote to that file, all files read by those programs, all programs that wrote to those programs, etc.

Lastly, RepeatFS provides Virtual Dynamic Files (VDFs).  These VDFs automatically execute commonly performed tasks (such as converting file types) in a systematic and uniform fashion.  Each supported file will have a corresponding VDF presented on disk.  Upon accessing this file, RepeatFS will run the appropriate task, and populate the file on the fly with the correct information.  These files are then cached in memory, so subsequently accessing them does not require the process to run a second time.  VDFs may be chained together for combining operations and may be copied to turn them into normal files.

INSTALLATION
--
**Dependencies**
* Python 3 (and pip)
* FUSE 2 (and libfuse)
* GraphViz

Dependencies are available in all popular system package managers.  Python 3, Libfuse, and GraphViz are also available within Anaconda (you may be able to find a channel with FUSE available as well):

* Debian/Ubuntu: `sudo apt install python3 python3-pip fuse2 libfuse2 graphviz libgraphviz-dev`
* RHEL/Fedora: `sudo yum install python3 python3-pip fuse2 fuse2-libs graphviz graphviz-devel`
* Anaconda: `conda install -c conda-forge python=3 pip libfuse=2 graphviz`

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

**Path to a file's provenance graph** - like the provenance record, this is also a VDF.  RepeatFS visualizes provenance by generating an HTML file that can be viewed in any browser:
```
<RepeatFS mount directory>/<any sub directories>/<file name>+/<file name>.provenance.html
```



REPLICATION EXAMPLES
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
cd results.tar+
firefox results.tar.provenance.html
````

![Example 1](https://raw.githubusercontent.com/ToniWestbrook/repeatfs/master/images/example1.png)

RepeatFS can also replicate these steps to recreate `results.tar` using the `results.tar.provenance.json` file.  You can use this file (or distribute it to others) to reproduce your work.  In the following example, we've copied the provenance record into our home directory.  We then mount a directory with RepeatFS and replicate the work (and save stdout and stderr into log files):

```
repeatfs mount ~/replicate ~/mnt
cd ~/mnt

repeatfs replicate ~/results.tar.provenance.json --stdout stdout.log --stderr stderr.log
```

RepeatFS will execute and verify each step. Version mismatches or other errors will be reported:

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

RepeatFS can also simply list the commands that would be used during replication (using the `-l` argument):

```
repeatfs replicate ~/results.tar.provenance.json -l
```

This will list each command that will be run, in order.  It will also list ID(s) next to each command, which can be used during replication (using the `-e` argument) to reconstruct a missing shell script.

```
[turing|1591668563.97|1652] wget ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz
[turing|1591668614.24|1695] gzip -d uniprot_sprot.fasta.gz
[turing|1591668694.23|1756] grep --color=auto > uniprot_sprot.fasta > /tmp/mnt/headers.txt
[turing|1591669111.77|2165, turing|1591669111.77|2166] cat uniprot_sprot.fasta | wc -l > /tmp/mnt/count.txt
[turing|1591669123.5|2175] tar -cvf results.tar headers.txt count.txt
```

VIRTUAL DYNAMIC FILES
--
Provenance and replication are only a part of RepeatFS's capabilities.  RepeatFS can also automate commonly performed tasks using VDFs. VDFs are special files representing the output of some operation, such as converting file formats, extracting text, indexing a reference, etc.  Whenever RepeatFS detects a file that is a valid input for one of these types of operations, it will also show a corresponding output file.  When this output file is accessed (opened, copied, read), RepeatFS will automatically run the program necessary to perform the action, and populate the output file in realtime.  These VDFs look and act just like normal files, though they are stored in memory.  VDFs may be converted into normal files simply by copying them to another directory.

VDFs are configured within the RepeatFS configuration file as follows:

```
# Comments start with #
# The following entry creates a VDF that shows a corresponding FASTA for any FASTQ.
# Accessing the FASTA will automatically run seqtk

[entry]                   # Each VDF entry starts with [entry]
match=\.fastq$            # This regular expression controls which files are valid input
ext=.fasta                # This extension will be appended to the end of the VDF
cmd=seqtk seq -A {input}  # This is the command that will be run when accessing the VDF
```

All VDF files, including system-provided ones like provenance, or user-defined ones like the FASTQ->FASTA example above, can be found by placing a plus sign `+` after the input filename.  So if we had a FASTQ file located at `~/mnt/example.fastq`, listing the contents of `~/mnt/example.fastq+` would show the following files:

```
example.fastq.fasta
example.fastq.provenance.html
example.fastq.provenance.json
```

Note that since VDFs are treated as normal files, they also can be used as inputs to other VDFs.  In this way, multiple VDFs can be chained together to perform automated operations in a modular fashion.  Using the above example, if we define the following VDF:

```
[entry]
match=.*
ext=.count
cmd=wc -l {input}
```

This would create a corresponding `.count` for every file in the RepeatFS mount, containing the number of lines the original file contained (even if that original file was a VDF).  In this way, we can view the number of lines in a FASTA converted from the FASTQ file above:

```
less ~/mnt/example.fastq+/example.fastq.fasta+/example.fastq.fasta.count
```

While VDFs are not required to record provenance in RepeatFS, they do represent a powerful tool to aid in improving reproducibility by performing common tasks in a uniform, documented fashion.

PLUGINS
--
RepeatFS supports expanding its functionality through the use of plugins. Plugins are enabled by including the `plugins` option in the configuration file and specifying the names, comma delimited, of the plugins you wish to enable:

```
plugins=snapshot
```

Currently, the `snapshot` plugin is the only publicly available plugin, although others are planned. The snapshot plugin will capture the contents of specific files and file types when those files are accessed. The file contents are saved into the provenance database and can be viewed/downloaded using the provenance web interface noted above. After selecting the file node in the provenance graph, the info window on the right will display links for the dates and times of each available snapshot. This is especially useful when multiple modifications were made to an analysis script, and you want to verify what particular version of a script was used to generate a result file.

To configure which files you'd like to snapshot, include the `snapshot.select` option in the configuration file, specifying a regular expression of all the file names/types you'd like to capture.

```
snapshot.select=\.(py|sh|txt|r|m)$
```
> [!CAUTION]
> Be careful not to select file types that may be very large, as this may significantly slow down processing while capturing the snapshot and will cause the database size to grow quickly.

If you wish to develop your own plugin, see the `template.txt` in the `plugins` subdirectory for further instructions.

QUESTIONS
--
RepeatFS is actively being developed and improved all the time.  If you encounter any issue at all, please don't hesitate to reach out and file a bug report here: https://github.com/ToniWestbrook/repeatfs/issues

PUBLICATIONS
--
For further understanding of RepeatFS, please refer to the following publications:

### Conference Proceedings

* Anthony Westbrook, Bocheng Cui, Lakshmi Ongolu, & Elizabeth Varki (2026). *Active Reproducibility for File-Based Exploratory Science*. **Proceedings of the ACM Conference on Reproducibility and Replicability (ACM REP '26).** [PDF](supplemental/acmrep2026/ACToR.pdf) [[source]](https://dl.acm.org/doi/10.1145/3820002.3828580)

  <details><summary>BibTeX</summary>

  ```bibtex
  @inproceedings{10.1145/3820002.3828580,
  author = {Westbrook, Anthony and Cui, Bocheng and Ongolu, Lakshmi and Varki, Elizabeth},
  title = {Active Reproducibility for File-Based Exploratory Science},
  year = {2026},
  isbn = {9798400727788},
  publisher = {Association for Computing Machinery},
  address = {New York, NY, USA},
  url = {https://doi.org/10.1145/3820002.3828580},
  doi = {10.1145/3820002.3828580},
  abstract = {Computational reproducibility is commonly understood as regenerating a file’s exact contents by re-executing the workflow that produced it. In exploratory science, however, workflows are rarely explicit: computation evolves through backtracking, parameter tuning, and abandoned execution paths, so the operations that produce a final result are often not represented in any single, coherent workflow description. As a result, irreproducibility is not merely a failure of documentation or user discipline, but a systems problem of recovering the correct workflow from unconstrained execution. We introduce Active Reproducibility, a model that generalizes this goal beyond byte-identical output, defining reproducibility through three complementary guarantees: exact replay in the original environment, semantic equivalence across environments, and computational fidelity under changing inputs. We realize this model in RepeatFS, a file system that records unconstrained computational activity and reconstructs a minimal causal workflow for each output file. It then replays this workflow across machines by automatically resolving duplicate processes and translating hardcoded local paths, without containers or manual setup. In a multi-day bioinformatics experiment, RepeatFS isolates 18 relevant processes from 579 exploratory tasks while bypassing more than 114,000 concurrent system processes. Across a benchmark of 18 published experiments, it successfully replicates 13 of 14 viable workflows from their recorded lineage, including on machines other than the one where they were recorded. Its overhead is concentrated in I/O-intensive workloads, while compute-bound tasks experience minimal slowdown.},
  booktitle = {Proceedings of the 4th ACM Conference on Reproducibility and Replicability},
  pages = {21–32},
  numpages = {12},
  keywords = {Computational Reproducibility, Replicability, Active Reproducibility, Data Provenance, Causal Lineage, User-Space File Systems, Workflow Reconstruction, Exploratory Science},
  location = {Delft, Netherlands},
  series = {ACM REP '26}
  }
  ```
  </details>

* Anthony Westbrook, Lakshmi Ongolu, Bocheng Cui, & Elizabeth Varki (2026). *Point-and-Click Reproducibility for Command-Line Data Analysis*. **Proceedings of the ACM Conference on Reproducibility and Replicability (ACM REP '26).** [PDF](supplemental/acmrep2026/VDF.pdf) [[source]](https://dl.acm.org/doi/10.1145/3820002.3828586)

  <details><summary>BibTeX</summary>

  ```bibtex
  @inproceedings{10.1145/3820002.3828586,
  author = {Westbrook, Anthony and Ongolu, Lakshmi and Cui, Bocheng and Varki, Elizabeth},
  title = {Point-and-Click Reproducibility for Command-Line Data Analysis},
  year = {2026},
  isbn = {9798400727788},
  publisher = {Association for Computing Machinery},
  address = {New York, NY, USA},
  url = {https://doi.org/10.1145/3820002.3828586},
  doi = {10.1145/3820002.3828586},
  abstract = {In data-intensive research, a file rarely exists in isolation: a single source file typically implies a lineage of derived files, from processing raw satellite imagery to analyzing brain scans. While point-and-click consumer applications maintain these relationships automatically (e.g., photo software generating thumbnails), exploratory scientific computing is primarily command-line driven, and the dependencies between source and derived files are managed by the researcher. Build systems and workflow managers can automate this, but only once dependencies are explicitly declared, a step such analysis rarely takes, leaving the manual loop prone to error and state drift, where derived files become stale relative to their source. To address this, we introduce point-and-click reproducibility for exploratory command-line data analysis, and implement it in RepeatFS, a file system where derived files are Virtual Dynamic Files (VDFs). By constraining derivation to a predefined set of valid operations and recording it in the file path, the model makes any accessible file the consistent, reproducible output of a valid workflow. A VDF is a transient store kept consistent with its source and the computations that produced it; its lineage is scaffolded as a virtual directory tree, so standard shell commands become menu actions: listing a directory opens a menu, reading a file clicks an item, and traversal chains them. The structure of the path is the structure of the workflow, executed just-in-time whenever the accessed file is missing or stale. By bringing this automation to the command line, VDFs make the reproducibility of single-source derived files an intrinsic, transparent property of the file system rather than an afterthought. Our evaluation shows that RepeatFS’s in-memory streaming architecture avoids repeated disk materialization, achieving up to a 4.5 {\texttimes} speedup over traditional file-based workflows.},
  booktitle = {Proceedings of the 4th ACM Conference on Reproducibility and Replicability},
  pages = {100–111},
  numpages = {12},
  keywords = {Point-and-Click Reproducibility, File Systems, Virtual Dynamic Files, Provenance, Data Lineage, Reproducibility, Exploratory Data Analysis},
  location = {Delft, Netherlands},
  series = {ACM REP '26}
  }
  ```
  </details>

### Journal Articles

* Anthony Westbrook, Elizabeth Varki, W Kelley Thomas, **RepeatFS: a file system providing reproducibility through provenance and automation**, Bioinformatics, Volume 37, Issue 9, May 2021, Pages 1292–1296, https://doi.org/10.1093/bioinformatics/btaa950
  - [Oxford Academic](https://academic.oup.com/bioinformatics/article/37/9/1292/5952659)
  - [National Library of Medicine (PubMed Central)](https://pmc.ncbi.nlm.nih.gov/articles/PMC8189677/)

### Doctoral Dissertation

* Westbrook, Anthony Stephen, "RepeatFS: A File System Providing Reproducibility Through Provenance and Automation" (2021). Doctoral Dissertations. 2640.
https://scholars.unh.edu/dissertation/2640
  - [UNH Scholars Repository](https://scholars.unh.edu/dissertation/2640/)
  - [ProQuest Open Dissertation](https://www.proquest.com/openview/000c6ec94890bcac9457621f700a6e4c/1?pq-origsite=gscholar&cbl=18750&diss=y)
