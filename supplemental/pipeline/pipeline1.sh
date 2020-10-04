#! /bin/bash
touch pipeline1-1.txt
mkdir reads
cd reads
fastq-dump --split-files SRR9847874
touch ../pipeline1-2.txt
trimmomatic PE SRR9847874_1.fastq SRR9847874_2.fastq out_fwd_paired.fq out_fwd_unpaired.fq out_rev_paired.fq out_rev_unpaired.fq ILLUMINACLIP:TruSeq3-PE.fa:2:30:10 LEADING:3 TRAILING:3 SLIDINGWINDOW:4:15 MINLEN:36 -threads 6

touch ../pipeline1-3.txt
cd ..
mkdir assembly
cd assembly
spades.py --pe1-1 ../reads/out_fwd_paired.fq --pe1-2 ../reads/out_rev_paired.fq --pe1-s ../reads/out_fwd_unpaired.fq --pe1-s ../reads/out_rev_unpaired.fq -k 77 --tmp-dir /tmp/spades -o output

touch ../pipeline1-4.txt
cd ..
mkdir annotation
cd annotation
prokka ../assembly/output/scaffolds.fasta --centre C --locustag L --force


