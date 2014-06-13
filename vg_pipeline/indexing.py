import codecs
import os

import lucene

from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
from org.apache.lucene.util import Version
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig, DirectoryReader
from org.apache.lucene.document import FieldType, Document, Field
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import QueryParser
from java.io import File


def get_writer(index='index'):
    store = SimpleFSDirectory(File(index))

    analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
    analyzer = LimitTokenCountAnalyzer(analyzer, 1048576)

    config = IndexWriterConfig(Version.LUCENE_CURRENT, analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)

    writer = IndexWriter(store, config)

    return writer


def index_article(writer, art_id, art_body):
    art_id_field = FieldType()
    art_id_field.setIndexed(True)
    art_id_field.setStored(True)
    art_id_field.setTokenized(False)
    art_id_field.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS)

    art_body_field = FieldType()
    art_body_field.setIndexed(True)
    art_body_field.setStored(True)
    art_body_field.setTokenized(True)
    art_body_field.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

    doc = Document()
    doc.add(Field("art_id", str(art_id), art_id_field))
    doc.add(Field("art_body", art_body, art_body_field))

    writer.addDocument(doc)


def is_article_indexed(art_id, index='index'):
    store = SimpleFSDirectory(File(index))
    searcher = IndexSearcher(DirectoryReader.open(store))
    analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)

    query = QueryParser(Version.LUCENE_CURRENT, 'art_id', analyzer).parse(str(art_id))

    docs = searcher.search(query, 1).scoreDocs

    return len(docs) > 0


def init_lucene():
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])


def update_index(index_dir, stored_index):
    writer = get_writer(index_dir)

    for art_id, raw_fn in stored_index.items():
        if not is_article_indexed(art_id, index_dir):
            art_path = os.path.dirname(raw_fn)
            cooked_fn = os.path.join(art_path, "%d-cooked.txt" % art_id)

            with codecs.open(cooked_fn, 'r', 'utf-8') as f:
                body = f.read()

            index_article(writer, art_id, body)

    writer.commit()
    writer.close()


def search(term, n_docs=10, index='index'):
    store = SimpleFSDirectory(File(index))
    searcher = IndexSearcher(DirectoryReader.open(store))
    analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)

    query = QueryParser(Version.LUCENE_CURRENT, 'art_body', analyzer).parse(term)

    # str(query.getClass().toString()) == "class org.apache.lucene.search.TermQuery"

    score_docs = searcher.search(query, n_docs).scoreDocs

    return [(score_doc.score, unicode(searcher.doc(score_doc.doc).get('art_body'))) for score_doc in score_docs]