import logging
import os
import sqlite3

from indexing import init_lucene, get_writer, index_article


SQL_SELECT_ART_ID = 'select art_id from content, metadata where content.id = metadata.content_id'
SQL_SELECT_ART_ID_CONTENT_NOT_EMPTY = 'select art_id from content, metadata ' \
                                      'where content.id = metadata.content_id and cooked != "" and art_id is not null'
SQL_SELECT_ARTICLE_BY_ID = 'select art_id, cooked, summary, title, tags, date from metadata, content ' \
                           'where metadata.content_id = content.id and metadata.art_id = ?'
SQL_INSERT_METADATA = 'insert into metadata (content_id, art_id, summary, title, tags, date) values (?, ?, ?, ?, ?, ?)'
SQL_INSERT_CONTENT = 'insert into content (cooked, raw) values (?, ?)'
SQL_HAS_ARTICLE = 'select art_id from metadata, content where metadata.content_id = content.id and metadata.art_id = ?'
SQL_INDEX_CONTENT_ID = 'create unique index if not exists content_id on content (id)'
SQL_INDEX_METADATA_ART_ID = 'create unique index if not exists metadata_art_id on metadata (art_id)'
SQL_INDEX_METADATA_ID_ = 'create unique index if not exists metadata_id on metadata (id)'
SQL_TABLE_CONTENT = 'create table if not exists content (id integer primary key autoincrement, ' \
                    'cooked text, raw text)'
SQL_TABLE_METADATA = 'create table if not exists metadata (id integer primary key autoincrement, content_id integer, ' \
                     'art_id integer, summary text, title text, tags text, date text, ' \
                     'foreign key (content_id) references content(id))'
SQL_ARTICLE_COUNT = "select count() from content"

TERM_INDEX_ROOT = 'term_index'

STORE_DB_FN = 'store.db'


def _store_db_fn(path):
    return os.path.join(path, STORE_DB_FN)


def _init_store_db(path):
    conn = _db_conn(path)

    conn.execute(SQL_TABLE_CONTENT)
    conn.execute(SQL_TABLE_METADATA)
    conn.execute(SQL_INDEX_METADATA_ID_)
    conn.execute(SQL_INDEX_METADATA_ART_ID)
    conn.execute(SQL_INDEX_CONTENT_ID)

    conn.commit()
    conn.close()


def _term_index_path(path):
    return os.path.join(path, TERM_INDEX_ROOT)


def _init_term_index(path):
    try:
        init_lucene()
    except ValueError:
        # Lucene already initialized. Ignore exception.
        pass

    writer = get_writer(path)
    writer.commit()
    writer.close()

    return path


def _init_store(path):
    if not os.path.exists(path):
        os.makedirs(path)

    _init_store_db(path)

    _init_term_index(_term_index_path(path))


def _db_conn(path):
    return sqlite3.connect(_store_db_fn(path))


def _has_article(path, art_id):
    conn = _db_conn(path)
    cur = conn.cursor()

    cur.execute(SQL_HAS_ARTICLE, (art_id, ))
    result = cur.fetchone()
    conn.close()

    return result


def _add_article(path, article):
    conn = _db_conn(path)
    cur = conn.cursor()

    cur.execute(SQL_INSERT_CONTENT, (article.get('cooked_doc'), article.get('raw_doc')))
    content_id = cur.lastrowid
    cur.execute(SQL_INSERT_METADATA, (content_id, article['art_id'], unicode(article.get('summary', '')),
                                      unicode(article.get('title', '')), unicode('|'.join(article.get('tags', []))),
                                      unicode(article.get('date'))))

    conn.commit()
    conn.close()

    writer = get_writer(_term_index_path(path))

    index_article(writer, article['art_id'], article['cooked_doc'])

    writer.commit()
    writer.close()

    return article


def _get_article_by_id(path, art_id):
    conn = _db_conn(path)
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row

    cur.execute(SQL_SELECT_ARTICLE_BY_ID, (art_id, ))

    result = []

    for row in cur:
        article = {'art_id': row['art_id'], 'cooked_doc': row['cooked'], 'summary': row['summary'],
                   'title': row['title'], 'tags': row['tags'].split('|'), 'date': row['date']}

        result.append(article)

    conn.close()

    return result


def _article_ids(path, filter_empty):
    conn = _db_conn(path)
    cur = conn.cursor()

    if filter_empty:
        cur.execute(SQL_SELECT_ART_ID_CONTENT_NOT_EMPTY)
    else:
        cur.execute(SQL_SELECT_ART_ID)

    for row in cur:
        yield row[0]

    conn.close()


def _article_count(path):
    conn = _db_conn(path)

    cur = conn.cursor()
    cur.execute(SQL_ARTICLE_COUNT)
    count = int(cur.fetchone()[0])

    conn.close()

    return count


class ArticleStore(object):
    def __init__(self, path):
        self.path = path

        _init_store(path)

    def add_article(self, article):
        _add_article(self.path, article)

        return article['art_id']

    def get_article_by_id(self, art_id, duplicates=False):
        articles = _get_article_by_id(self.path, art_id)

        if not duplicates and len(articles) > 1:
            logging.warn("Duplicate articles for id %d" % art_id)

        if duplicates:
            return articles
        else:
            return articles[0]

    def has_article(self, art_id):
        return _has_article(self.path, art_id)

    def article_ids(self, filter_empty=True):
        return _article_ids(self.path, filter_empty)

    def __len__(self):
        return _article_count(self.path)

