import codecs
from glob import glob
import logging
import os
import re
import urllib2

import feedparser
import justext


HTTP_OK = 200

ingestion_dir = os.path.join(os.getcwd(), 'articles')

feed_address_all = 'http://www.vg.no/export/Alle/rdf.hbs'


def build_stored_index(ingestion_dir):
    logging.info("Building index of ingested articles in %s" % ingestion_dir)

    index = {}

    for full_fn in glob(os.path.join(ingestion_dir, '*-raw.html')):
        fn, _ = os.path.splitext(os.path.basename(full_fn))
        art_id, _ = fn.split('-')

        index[int(art_id)] = full_fn

    return index


def extract_article_id(url):
    m = re.search('artid=(\d+)', url)

    if m:
        return int(m.group(1))
    else:
        return None


def ingest_feed(feed_url, ingestion_dir, stored_index):
    logging.info("Ingesting feed from URL %s" % feed_url)

    if not os.path.exists(ingestion_dir):
        os.makedirs(ingestion_dir)

    read_art_ids = []

    feed_doc = feedparser.parse(feed_url)

    if feed_doc['status'] != HTTP_OK:
        logging.error("RSS ingestion URL returned code %d" % feed_doc['status'])

    if 'entries' not in feed_doc.keys():
        logging.error("No entries key in RSS parse")
        return None

    for feed_entry in feed_doc['entries']:
        entry_url = feed_entry['link']
        art_id = extract_article_id(entry_url)

        if art_id in stored_index.keys():
            continue

        opener = urllib2.build_opener()
        f = opener.open(entry_url)
        entry_raw = f.read().decode('latin1')

        entry_fn = os.path.join(ingestion_dir, "%d-raw.html" % art_id)

        with codecs.open(entry_fn, 'w', 'utf-8') as f:
            f.write(unicode(entry_raw))

        stored_index[art_id] = entry_fn

        read_art_ids.append(art_id)

    for art_id in read_art_ids:
        cooked_article(art_id, stored_index)

    return read_art_ids


def extract_article_text(raw_html):
    doc = justext.justext(raw_html, justext.get_stoplist('Norwegian_Bokmal'))
    good_paragraphs = [paragraph.text for paragraph in doc if paragraph.class_type == 'good']

    return '\n\n'.join(good_paragraphs)


def cooked_article(art_id, stored_index):
    if art_id not in stored_index.keys():
        logging.error("Article id %d not ingested" % art_id)

        return None

    raw_fn = stored_index[art_id]
    ingestion_dir = os.path.dirname(raw_fn)

    cooked_fn = os.path.join(ingestion_dir, "%d-cooked.txt" % art_id)

    with codecs.open(cooked_fn, 'w', 'utf-8') as out_f:
        with codecs.open(raw_fn, 'r', 'utf-8') as in_f:
            out_f.write(extract_article_text(in_f.read()))

    return cooked_fn
