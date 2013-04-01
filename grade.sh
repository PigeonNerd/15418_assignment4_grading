#!/bin/bash
mkdir -p $HOME/local/lib/python2.6/site-packages
export PYTHONPATH=$HOME/local/lib/python2.6/site-packages:$PYTHONPATH
easy_install --prefix=$HOME/local poster
python scripts/launch_amazon.py --qsub 4 --test grading_burst.txt
python scripts/launch_amazon.py --qsub 4 --test grading_nonuniform1.txt
python scripts/launch_amazon.py --qsub 4 --test grading_nonuniform2.txt
python scripts/launch_amazon.py --qsub 4 --test grading_nonuniform3.txt
python scripts/launch_amazon.py --qsub 4 --test grading_random.txt
python scripts/launch_amazon.py --qsub 4 --test grading_compareprimes.txt
