#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import subprocess
from itertools import izip

bad_de = re.compile(r'_|<\+PUNCT>|<\+CARD>|<\+SYMBOL>|<\+NPROP>|<GUESSER>|<\^ABBR>')
bad_tr = re.compile(r'\*UNKNOWN\*|\+Punct|\+Punc')


alphabet = re.compile(r'^[a-zA-ZßäöüÄÖÜçÇşŞğĞıİ\-\'\.]+$')


def check_tr(freq_file, output_file, min_freq):
    print 'reading words...'
    d = {}
    words = []
    for line in open(freq_file):
        items = line.strip().split()
        w, f = items[0], int(items[1])
        if f < min_freq:
            break
        d[w] = f
        words.append(w)

    print 'number of words with freq >= %d: %d' % (min_freq, len(words))

    print 'checking morphology'
    # title() or lower() or original?
    # input_str = '\n'.join(w.decode('utf-8').title().encode('utf-8') for w in words) + '\n'
    input_str = '\n'.join(words) + '\n'
    # cmd = './bin/lookup -d -q -f bin/checker.script'
    cmd = './bin/Morph-Pipeline/lookup -d -q -f bin/Morph-Pipeline/test-script.txt'
    lookup = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    output = lookup.communicate(input=input_str)[0]
    # print output
    morphs = output.strip().split('\n\n')
    assert len(words) == len(morphs)

    out = open(output_file, 'w')

    for (w, m) in izip(words, morphs):
        if alphabet.match(w) and not bad_tr.search(m):
            out.write('%s\t%d\n' % (w, d[w]))
    out.close()


def check_de(freq_file, output_file, min_freq):
    print 'reading words...'
    d = {}
    words = []
    for line in open(freq_file):
        items = line.strip().split()
        w, f = items[0], int(items[1])
        if f < min_freq:
            break
        d[w] = f
        words.append(w)

    print 'number of words with freq >= %d: %d' % (min_freq, len(words))

    print 'checking morphology'
    # title() or lower() or original?
    # input_str = '\n'.join(w.decode('utf-8').title().encode('utf-8') for w in words) + '\n'
    input_str = '\n'.join(words) + '\n'
    cmd = './bin/_run_smor.sh 2> /dev/null'
    lookup = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    output = lookup.communicate(input=input_str)[0]
    morphs = output.strip().split('\n\n')
    assert len(words) == len(morphs)

    out = open(output_file, 'w')

    for (w, m) in izip(words, morphs):
        if alphabet.match(w) and not bad_de.search(m):
            out.write('%s\t%d\n' % (w, d[w]))
    out.close()



if __name__ == '__main__':
    lang = sys.argv[1]
    freq_file = sys.argv[2]
    output_file = sys.argv[3]
    # min_freq = int(sys.argv[3])
    min_freq = 5
    if lang == 'tr':
        check_tr(freq_file, output_file, min_freq)
    elif lang == 'de':
        check_de(freq_file, output_file, min_freq)
    else:
        print 'wrong argument!'





