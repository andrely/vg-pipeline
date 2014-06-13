from collections import Sequence, Iterable
import logging
from optparse import OptionParser
import os

from numpy import linspace, unique
from numpy.ma import power
from sklearn.cross_validation import train_test_split
from sklearn.externals import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import f1_score, precision_score, recall_score, \
    jaccard_similarity_score, confusion_matrix
from sklearn.pipeline import Pipeline
from sklearn.svm import SVC
from sklearn.utils.multiclass import unique_labels

from store import ArticleStore


TOPICS = {"Nyheter", "Sport", "Rampelys"}


class ArticleSequence(Sequence):
    def __init__(self, store, key='cooked_doc'):
        self.store = store
        self.art_ids = list(store.article_ids())
        self.key = key

    def _get_index(self, index):
        article = self.store.get_article_by_id(self.art_ids[index])

        return article[self.key]

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self._get_index(i) for i in xrange(*index.indices(len(self)))]
        elif isinstance(index, Iterable):
            return [self._get_index(i) for i in index]
        elif isinstance(index, int):
            return self._get_index(index)
        else:
            raise TypeError

    def __len__(self):
        return len(self.art_ids)


class FilteredSequence(Sequence):
    def __init__(self, base_sequence, included_indices):
        self.base_sequence = base_sequence
        self.included_indices = included_indices

    def __len__(self):
        return len(self.included_indices)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self._get_index(i) for i in xrange(*index.indices(len(self)))]
        elif isinstance(index, Iterable):
            return [self._get_index(i) for i in index]
        elif isinstance(index, int):
            return self._get_index(index)
        else:
            raise TypeError

    def _get_index(self, index):
        return self.base_sequence[self.included_indices[index]]


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    parser = OptionParser()
    parser.add_option('-s', '--store')
    parser.add_option('-m', '--model-file')
    parser.add_option('-p', '--num-processes')

    opts, args = parser.parse_args()

    if opts.store:
        store = os.path.abspath(opts.store)
    else:
        raise ValueError('--store argument is required')

    if opts.model_file:
        model_fn = opts.model_file
    else:
        model_fn = None

    if opts.num_processes:
        p = int(opts.num_processes)
    else:
        p = 1

    logging.info("Reading data from store %s" % store)
    store = ArticleStore(store)

    content = ArticleSequence(store)
    tags = ArticleSequence(store, key='tags')

    topic_indices = []
    topics = []

    for i, top in enumerate(tags):
        topic = list(TOPICS & set(top))

        if topic:
            topics += topic
            topic_indices.append(i)

    content = list(FilteredSequence(content, topic_indices))
    tags = topics

    logging.info("%d articles in dataset" % len(content))
    # logging.info("Average %.4f tags pr. article" % (len(list(chain(*tags))) / float(len(tags))))
    # logging.info("%d different tags" % len(unique(list(chain(*tags)))))
    logging.info("%d different tags" % len(unique(tags)))

    train_idx, test_idx = train_test_split(range(len(content)))
    content_train = FilteredSequence(content, train_idx)
    content_test = FilteredSequence(content, test_idx)

    tags_train = FilteredSequence(tags, train_idx)
    tags_test = FilteredSequence(tags, test_idx)

    pipeline = Pipeline([('vect', TfidfVectorizer(strip_accents='unicode', lowercase=True, max_df=0.5, min_df=2,
                                                  smooth_idf=True, sublinear_tf=True)),
                         ('svm', SVC(kernel='linear'))])

    logging.info("Running meta parameter grid search")
    grid = GridSearchCV(pipeline, {'svm__C': power(10, linspace(-5, 4, num=10))}, verbose=1, n_jobs=p, cv=5)

    grid.fit(content_train, tags_train)

    c = grid.best_params_['svm__C']

    logging.info("Best score %.4f with C = %f" % (grid.best_score_, c))

    pipeline = Pipeline([('vect', TfidfVectorizer(strip_accents='unicode', lowercase=True, max_df=0.5, min_df=2,
                                                  smooth_idf=True, sublinear_tf=True)),
                         ('svm', SVC(kernel='linear', C=c))])
    pipeline.fit(content_train, tags_train)
    pred = pipeline.predict(content_test)

    logging.info("Held out performence F1=%.4f, p=%.4f, r=%.4f, jaccard=%.4f" %
                 (f1_score(tags_test, pred), precision_score(tags_test, pred),
                  recall_score(tags_test, pred), jaccard_similarity_score(tags_test, pred)))

    logging.info("Confusion matrix\n%s\n%s" % (unique_labels(tags_test, pred), confusion_matrix(tags_test, pred)))

    logging.info("Training full model")

    pipeline = Pipeline([('vect', TfidfVectorizer(strip_accents='unicode', lowercase=True, max_df=0.5, min_df=2,
                                                  smooth_idf=True, sublinear_tf=True)),
                         ('svm', SVC(kernel='linear', C=c))])
    pipeline.fit(content, tags)

    if model_fn:
        logging.info("Saving full model to %s" % model_fn)
        joblib.dump(pipeline, model_fn, compress=9)
