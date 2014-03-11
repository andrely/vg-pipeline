import cPickle
import os

from nltk import TreebankWordTokenizer


NO_PUNKT_MODEL = os.path.join('models', 'punkt-norwegian-open2.pickle')

_punkt_tokenizer = None


def get_punkt_tokenizer():
    global _punkt_tokenizer

    if _punkt_tokenizer:
        return _punkt_tokenizer

    with open(NO_PUNKT_MODEL) as f:
        _punkt_tokenizer = cPickle.load(f)

    return _punkt_tokenizer


def split_sentences(text):
    tokenizer = get_punkt_tokenizer()

    return tokenizer.tokenize(text, realign_boundaries=True)


def tokenize(text):
    return [token.encode('utf-8') for token in TreebankWordTokenizer().tokenize(text)]


def preprocess(text):
    return [tokenize(sent) for sent in split_sentences(text)]
