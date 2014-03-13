import logging
from optparse import OptionParser
import os
import time

from ingestion import ingest_feed, feed_address_all
from store import ArticleStore


INGESTION_INTERVAL = 60

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    parser = OptionParser()
    parser.add_option('-r', '--root')

    opts, args = parser.parse_args()

    if opts.root:
        store_root = os.path.abspath(opts.root)
    else:
        raise ValueError('--root option is required')

    logging.info("Ingesting to store in %s" % store_root)
    store = ArticleStore(store_root)

    while True:
        art_ids = ingest_feed(feed_address_all, store)
        logging.info("Ingested %d articles" % len(art_ids))

        time.sleep(INGESTION_INTERVAL)
