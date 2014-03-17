import logging
import os
import re
import urllib2

import feedparser

import justext


HTTP_OK = 200

ingestion_dir = os.path.join(os.getcwd(), 'articles')

feed_address_all = 'http://www.vg.no/export/Alle/rdf.hbs'


def extract_article_id(url):
    m = re.search('artid=(\d+)', url)

    if m:
        return int(m.group(1))
    else:
        return None


def metadata_from_rss_entry(feed_entry):
    return {'summary': feed_entry.get('summary'),
            'tags': [tag['term'] for tag in feed_entry.get('tags', []) if tag.has_key('term')],
            'title': feed_entry.get('title'),
            'date': feed_entry.get('published')}


def ingest_feed(feed_url, store):
    logging.info("Ingesting feed from URL %s" % feed_url)

    if not os.path.exists(ingestion_dir):
        os.makedirs(ingestion_dir)

    read_art_ids = []

    feed_doc = feedparser.parse(feed_url)

    if feed_doc.has_key('status') and feed_doc['status'] != HTTP_OK:
        logging.error("RSS ingestion URL returned code %d" % feed_doc['status'])

    if 'entries' not in feed_doc.keys():
        logging.error("No entries key in RSS parse")
        return None

    for feed_entry in feed_doc['entries']:
        entrydata = metadata_from_rss_entry(feed_entry)

        entrydata['url'] = feed_entry.get('link')
        entrydata['art_id'] = extract_article_id(entrydata['url'])

        if not entrydata['url']:
            logging.warn("RSS entry with no link url")
            continue

        if entrydata['art_id'] and store.has_article(entrydata['art_id']):
            logging.info("Article id %d already in store" % entrydata['art_id'])
            continue

        opener = urllib2.build_opener()
        f = opener.open(entrydata['url'])
        entrydata['raw_doc'] = f.read().decode('latin1')
        f.close()

        entrydata['cooked_doc'] = extract_article_text(entrydata['raw_doc'])

        store.add_article(entrydata)

        read_art_ids.append(entrydata['art_id'])

    return read_art_ids


def extract_article_text(raw_html):
    doc = justext.justext(raw_html, justext.get_stoplist('Norwegian_Bokmal'))
    good_paragraphs = [paragraph.text for paragraph in doc if paragraph.class_type == 'good']

    return '\n\n'.join(good_paragraphs)
