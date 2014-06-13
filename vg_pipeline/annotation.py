from subprocess import Popen
import itertools

from nltk.tag.stanford import NERTagger


PIPE = -1


def split_tt_line(line):
    word, tag, lemma = line.split('\t')
    tag = tag.split('_')

    return word, tag, lemma


def annotate_treetagger(sentences):
    p = Popen(['/Users/stinky/Work/tools/treetagger/bin/tree-tagger',
               '-token', '-lemma', '/Users/stinky/Work/vg-pipeline/models/tree_tagger_2014-03-05.tree_tagger_model'],
              shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE)

    p.stdin.write('\n'.join(list(itertools.chain(*sentences))))
    (stdout, stderr) = p.communicate()
    treetagger_output = stdout

    if p.returncode != 0:
        print stderr
        raise OSError('TreeTagger command failed!')

    tagged_sents = [split_tt_line(line) for line in treetagger_output.split('\n') if line != ""]

    return tagged_sents


def annotate_ner(sentences):
    ner = NERTagger('models/nob-ner-model.ser.gz',
                    '../tools/stanford-ner-2014-01-04/stanford-ner.jar', encoding='utf-8')
    return ner.batch_tag(sentences)
