from subprocess import Popen
import itertools

from nltk.tag.stanford import NERTagger


PIPE = -1


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

    return [line.split('\t') for line in treetagger_output.split('\n')]


def annotate_ner(sentences):
    ner = NERTagger('models/nob-ner-model.ser.gz',
                    '../tools/stanford-ner-2014-01-04/stanford-ner.jar')
    return ner.batch_tag(sentences)
