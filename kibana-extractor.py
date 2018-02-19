#!/usr/bin/env python
import argparse
import json
import logging
import requests



log = logging.getLogger(__file__)


class Kibana(object):
    def __init__(self, es_url, user=None, password=None):
        self.es_url = es_url
        self.auth = requests.auth.HTTPBasicAuth(user, password)

    def do_export(self, filename):
        log.debug("Start export")
        query = []
        for name in ('search', 'dashboard', 'visualization'):
            log.debug("Procesing type %s" % name)
            for search in self._get_index(name):
                log.debug("Procesing %s.%s" % (search['_id'], search['_type']))
                query.append(dict(_id=search['_id'], _type=search['_type']))
        content = self._mget(query)

        log.debug("Writting export data to %s" % filename)
        with open(filename, 'w+') as fd:
            fd.write(content)
        log.info("Data exported to %s" % filename)

    def do_import(self, filename):
        log.info("Start importing file %s" % filename)
        log.debug("Loading file %s" % filename)
        with open(filename) as fd:
            content = json.load(fd)

        log.debug("Processing data")
        for doc in content['docs']:
            log.debug("Processing doc %s" % doc['_id'])
            response = requests.post('%s/.kibana/search/%s' % (self.es_url, doc['_id']), json=doc['_source'], auth=self.auth)
            response.raise_for_status()

    def _get_index(self, name):
        response = requests.post('%s/.kibana/%s/_search?size=1000' % (self.es_url, name), auth=self.auth)
        response.raise_for_status()
        for hit in response.json()['hits']['hits']:
            yield hit

    def _mget(self, query):
        response = requests.post('%s/.kibana/_mget?pretty' % self.es_url, json=dict(docs=query), auth=self.auth)
        response.raise_for_status()
        return response.content


def process(args):
    kibana = Kibana(args.es_uri, args.user, args.password)
    log.info("Selecting action %s" % args.action)
    if args.action == 'export':
        kibana.do_export(args.filename)
    elif args.action == 'import':
        kibana.do_import(args.filename)
    

def main():
    parser = argparse.ArgumentParser(description='Import and export Kibana information')
    parser.add_argument(
        'action',
        choices=('import', 'export'),
        help='Action to be performed: import|export'
    )
    parser.add_argument(
        '--filename',
        default='data.json',
        help='File to be imported/exported'
    )
    parser.add_argument(
        '--es-uri',
        default='http://localhost:9200',
        help='URL to Elastic Search'
    )
    parser.add_argument(
        '--user',
        help='username to be used with elasticsearch'
    )
    parser.add_argument(
        '--password',
        help='password to be used with elasticsearch'
    )
    parser.add_argument("-v", "--verbose", dest="verbose_count",
        action="count",
        default=0,
        help="increases log verbosity for each occurence."
    )

    args = parser.parse_args()
    LOGS = (logging.WARNING, logging.INFO, logging.DEBUG)
    log_level = LOGS[min(len(LOGS) - 1, args.verbose_count)]
    logging.basicConfig(level=log_level, format='%(name)s (%(levelname)s): %(message)s')
    process(args)

if __name__ == '__main__':
    main()


