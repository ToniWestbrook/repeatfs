#! /bin/bash
touch pipeline2-1.txt
mkdir reference
cd reference
wget https://www.arb-silva.de/fileadmin/silva_databases/current/Exports/SILVA_138_SSURef_NR99_tax_silva.fasta.gz

touch pipeline2-2.txt
gzip -d SILVA_138_SSURef_NR99_tax_silva.fasta.gz

touch pipeline2-3.txt
bioawk -c fastx '$comment ~ /;Yersinia;/ { print ">" $name " " $comment "\n" $seq  }' SILVA_138_SSURef_NR99_tax_silva.fasta  > yersinia.fasta

touch pipeline2-4.txt
cd ..
mkdir tree
cd tree
mafft --thread 8 ../reference/yersinia.fasta > aligned.fasta

touch pipeline2-5.txt
raxmlHPC-PTHREADS -s aligned.fasta -n yersinia -m GTRCAT -T 8 -p 1
