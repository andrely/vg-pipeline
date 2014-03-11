import itertools

from gensim.corpora import Dictionary
from gensim.models import LdaModel, TfidfModel
from gensim.utils import deaccent
from nltk.corpus import stopwords


def article_to_bow(article):
    tokens = [deaccent(tok).lower() for tok in list(itertools.chain(*article))
              if tok not in stopwords.words('norwegian') and tok.isalpha()]

    return tokens


def train_lda_model(articles, num_topics=10):
    docs = [article_to_bow(a) for a in articles]

    dict = Dictionary(docs)
    dict.filter_extremes()
    dict.compactify()

    corpus = [dict.doc2bow(article_to_bow(a)) for a in articles]

    tfidf = TfidfModel(corpus=corpus, id2word=dict)

    w_corpus = [tfidf[doc] for doc in corpus]

    lda = LdaModel(corpus=w_corpus, num_topics=num_topics,
                   update_every=0, passes=20, id2word=dict)

    return lda, tfidf, dict


def update_lda_model(lda, tfidf, dict, articles):
    corpus = [tfidf[dict.doc2bow(article_to_bow(a))] for a in articles]

    lda.update(corpus)

    return lda
