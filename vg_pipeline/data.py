import codecs
import logging
import re


def obt_word_line(line):
    m = re.match('^"<(\S+)>"', line)

    if m:
        return m.group(1)
    else:
        return None


def obt_tag_line(line):
    return re.match('^\t"\S+"', line)


def get_ner_tag(line):
    if '&pe*' in line:
        return 'PERSON'
    elif '&or*' in line:
        return 'ORG'
    elif '&st*' in line:
        return 'PLACE'
    elif '&an*' in line:
        return 'OTHER'
    elif '&he*' in line:
        return 'EVENT'
    elif '&ve*' in line:
        return 'WORK'
    else:
        return None


def convert_ner_corpus(in_fn, out_fn):
    with codecs.open(in_fn, 'r', 'utf-8') as in_f:
        with codecs.open(out_fn, 'w', 'utf-8') as out_f:
            cur_word = None
            cur_tag = None
            sent_end = None

            for line in in_f:
                if obt_word_line(line):
                    word = obt_word_line(line)

                    if cur_word:
                        out_f.write("%s\t%s\n" % (cur_word, cur_tag or "O"))

                    if sent_end:
                        out_f.write('\n')

                    cur_word = word
                    cur_tag = None
                    sent_end = None

                if obt_tag_line(line):
                    tag = get_ner_tag(line)

                    if tag and cur_tag and tag != cur_tag:
                        logging.warn("Inconsistent NER tags %s - %s for %s" % (tag, cur_tag, cur_word))

                    if not cur_tag:
                        cur_tag = tag

                    if '<<<' in line:
                        sent_end = True


if __name__ == '__main__':
    convert_ner_corpus('../ner/aviser-utf8.sy', '../ner/aviser-utf8.vrt')
    convert_ner_corpus('../ner/ukeblader-utf8.sy', '../ner/ukeblader-utf8.vrt')
    convert_ner_corpus('../ner/skj-litt-utf8.sy', '../ner/skj-litt-utf8.vrt')
