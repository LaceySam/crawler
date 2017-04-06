import argparse
import json
import logging
import re
import requests
import urlparse

from requests import ConnectionError

from pybloomfilter import BloomFilter

MAX_URLS = 1000000


# Get an instance of a logger
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Crawler(object):

    # Of the form:
    #{
    #    'url' : {
    #        'static': ['x', 'y'],
    #        'links': ['a','b']
    #    },
    #    'status': 'alive'
    #}
    urls = {}

    # If we want to use this on a really big website, will need to use a bloom
    # filter to keep things speedy.
    urls_bf = BloomFilter(MAX_URLS, 0.01, 'filter.bloom')

    def __init__(self, url):
        parsed_url = urlparse.urlparse(url)
        self.netloc = parsed_url[1]

    def filter_links(self, hits, enforce_slash=False):
        links = []

        for hit in hits:
            url = urlparse.urlparse(hit)
            scheme = url[0]
            netloc = url[1]
            path = url[2]

            # Don't bother with / or ''
            if path == '/' or path == '':
                continue

            # Don't bother with dups
            if path in links:
                continue

            # If no scheme assume internal link
            if not scheme:
                links.append(path)
                continue

            # Filter out any mailto schemes etc
            if scheme != 'http' and scheme != 'https':
                continue

            # Only take those from here on the correct domain
            if self.netloc != netloc:
                continue

            links.append(path)

        return links

    # TODO(Sam): Are you sure you want to do this, this way?
    def pop_assets_from_links(self, links):
        sanitised_links = [link for link in links if '.' not in link]
        new_assets = [link for link in links if '.' in link]

        return sanitised_links, new_assets

    def scrape_page(self, url):
        logger.info('Scraping: %s', url)
        try:
            response = requests.get(url)
        except ConnectionError:
            logger.info('Connection error for %s', url)
            return [], [], 'dead'

        if response.status_code < 200 or response.status_code > 300:
            logger.info(
                'Non 2xx recieved on: %s, %d',
                url,
                response.status_code
            )
            return [], [], 'dead'

        # Find all urls
        link_matches = re.findall(' href="?\'?([^"\'>]*)', response.text)

        # And all static assets
        asset_matches = re.findall(' src="?\'?([^"\'>]*)', response.text)

        links = self.filter_links(link_matches, enforce_slash=True)
        assets = self.filter_links(asset_matches)

        links, new_assets = self.pop_assets_from_links(links)

        # Avoid duplicated by assuming page/ == page
        slashed_links = []
        for link in links:
            if not link.endswith('/'):
                link += '/'

            slashed_links.append(link)

        for asset in new_assets:
            if asset not in assets:
                assets.append(asset)

        return slashed_links, assets, 'alive'

    def crawl(self, url):
        self.urls_bf.add(url)

        links, static, status = self.scrape_page(url)
        for link in links:
            url_to_crawl = urlparse.urlunparse((
                'http', self.netloc, link, '', '', ''
            ))

            if url_to_crawl in self.urls_bf:
                continue

            self.crawl(url_to_crawl)

        self.urls[url] = {'links': links, 'static': static, 'status': status}

    def output(self):
        return self.urls


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Crawl a webpage')
    parser.add_argument(
        '--url',
        required=True,
        help='URL to scrape, eg. https://example.com'
    )

    parser.add_argument(
        '--dump-to',
        help='File to dump site map to',
        default='/tmp/site-map.json',
    )
    args = parser.parse_args()

    crawler = Crawler(args.url)
    crawler.crawl(args.url)

    site_map = json.dumps(
        crawler.output(),
        sort_keys=True,
        indent=4,
        separators=(',', ': '),
    )

    f = open(args.dump_to, 'w')
    print >>f, site_map
    f.close()
