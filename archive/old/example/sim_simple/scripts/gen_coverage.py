import sys
import argparse
import time
import pipes
import subprocess, shlex


def increment(depths,start,end,count):
    for i in range(start,end):
        depths[i]+=count
    return depths

genome_len = 48502
depths = [0]*genome_len

for ln in sys.stdin:
    ln = ln.rstrip()
    toks = ln.split("\t")
    assert len(toks)==4
    _, start, end, count = toks[0], int(toks[1]), int(toks[2]), int(toks[3])
    depths = increment(depths,start,end,count)

cov_file = open("coverages.txt",'w')
for i in range(0,genome_len):
    line = "%d\t%d\n"%(i,depths[i])
    cov_file.write(line)
cov_file.close()
