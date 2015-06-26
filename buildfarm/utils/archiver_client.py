#! /usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#
# Script name:   archiver_client.py
# Author(s):     Jordan Lund <jlund@mozilla.com>
# Target:        Python 2.7.x
#
"""
    calls relengapi archiver, downloads returned s3 url, and unpacks it locally
"""
import logging
import os
import random
import tarfile
import time
import urllib2
import json
from optparse import OptionParser

SUCCESS_CODE = 0
# This is not an infra error and we can't recover from it
FAILURE_CODE = 1
# When an infra error happens we want to turn purple and
# let sheriffs determine if re-triggering is needed
INFRA_CODE = 3

ARCHIVER_CONFIGS = {
    'mozharness': {
        'url_format': "archiver/hgmo/{repo}/{rev}?&preferred_region={region}&suffix={suffix}",
        # the root path from within the archive
        'extract_root_dir': "{repo}-{rev}",
        # the subdir path from within the root
        'default_extract_subdir': "testing/mozharness",
    }
}

RELENGAPI_HOST = {
    'staging': 'https://api-pub-build.allizom.org/',
    'production': 'https://api.pub.build.mozilla.org/'
}

logging.basicConfig(format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)


# This has been copied from lib.python.util.retry
def retrier(attempts=5, sleeptime=7, max_sleeptime=300, sleepscale=1.5, jitter=1):
    """ It helps us retry """
    for _ in range(attempts):
        log.debug("attempt %i/%i", _ + 1, attempts)
        yield
        if jitter:
            sleeptime += random.randint(-jitter, jitter)
        if _ == attempts - 1:
            # Don't need to sleep the last time
            break
        log.debug("sleeping for %.2fs (attempt %i/%i)",
                  sleeptime, _ + 1, attempts)
        time.sleep(sleeptime)
        sleeptime *= sleepscale
        if sleeptime > max_sleeptime:
            sleeptime = max_sleeptime


def get_task_result(url):
    task_response = urllib2.urlopen(url)
    task_content = task_response.read()
    task_response.close()
    return json.loads(task_content)['result']


def get_response_from_task(url, options):
    """
    gets and returns response from archiver task when the task is complete or retries are
    exhausted. Complete being the result's 'state' equals 'SUCCESS'

    :param url: archiver sub task url
    :param options: script options
    :return: response obj
    """
    task_result = {}
    for _ in retrier(attempts=options.max_retries, sleeptime=options.sleeptime,
                     max_sleeptime=options.max_retries * options.sleeptime):
        task_result = get_task_result(url)
        if task_result['state'] == "SUCCESS":
            break
        log.info("current task status: " + task_result['status'])

    if task_result.get('state') == "SUCCESS":
        if task_result.get('s3_urls'):
            # grab a s3 url using the preferred region if available
            s3_url = task_result['s3_urls'].get(options.region, task_result['s3_urls'].values()[0])
            print s3_url
            return urllib2.urlopen(s3_url)
        else:
            log.error("An s3 URL could not be determined even though archiver task completed. Check"
                      "archiver logs for errors. Task status: {}".format(task_result['status']))
            exit(FAILURE_CODE)
    else:
        log.error("Archiver's task could not be resolved. Check archiver logs for errors. Task "
                  "status: {}".format(task_result['status']))
        exit(FAILURE_CODE)


def get_url_response(url, options):
    """
    queries archiver endpoint and parses response for s3_url. if archiver returns a 202,
    a sub task url is polled until that response is complete.

    :param url: archiver get url with params
    :param options: script options
    :return: response obj
    """
    num = 0
    response = None
    for _ in retrier(attempts=options.max_retries, sleeptime=options.sleeptime,
                     max_sleeptime=options.max_retries * options.sleeptime):
        try:
            log.info("Getting archive location from {}".format(url))
            response = urllib2.urlopen(url)

            if response.code == 202:
                # archiver is taking a long time so it started a sub task
                response = get_response_from_task(response.info()['location'], options)

            if response.code == 200:
                break
            else:
                log.debug("got a bad response. response code: {}".format(response.code))

        except (urllib2.HTTPError, urllib2.URLError) as e:
            if num == options.max_retries - 1:
                log.exception(e)
                exit(INFRA_CODE)
        num += 1

    if not response.code == 200:
        content = response.read()
        log.error("could not determine a valid url response. return code: '{}'"
                  "return content: '{}".format(response.code, content))
        exit(INFRA_CODE)
    return response


def download_and_extract_archive(response, extract_root, destination):
    """downloads and extracts an archive from the archiver response.

    If the archive was based on an archive, only files and directories from within the subdir are
    extracted. The extraction takes place in the destination, overriding paths with the same name.

    :param response: the archiver response pointing to an archive
    :param extract_root: the root path to be extracted
    :param destination: the destination path to extract to
    """
    # make sure our extracted root path has a trailing slash
    extract_root = os.path.join(extract_root, '')

    try:
        with tarfile.open(fileobj=response, mode='r|gz') as tar:
            for member in tar:
                if not member.name.startswith(extract_root):
                    continue
                member.name = member.name.replace(extract_root, '')
                tar.extract(member, destination)
    except tarfile.TarError as e:
        log.exception("Could not download and extract archive. See Traceback:")
        log.exception(e)
        exit(INFRA_CODE)


def archiver(url, config_key, options):
    """
    1) obtains valid s3 url for archive via relengapi's archiver
    2) downloads and extracts archive
    """
    archive_cfg = ARCHIVER_CONFIGS[config_key]  # specifics to the archiver endpoint

    response = get_url_response(url, options)
    for h in response.headers:
        print 'header: {}, {}'.format(h, response.headers[h])
    print response.code

    # get the root path within the archive that will be the starting path of extraction
    subdir = options.subdir or archive_cfg.get('default_extract_subdir')
    extract_root = archive_cfg['extract_root_dir'].format(
        repo=os.path.basename(options.repo), rev=options.rev
    )
    if subdir:
        extract_root = os.path.join(extract_root, subdir)

    destination = options.destination or os.path.join(os.getcwd(), config_key)

    download_and_extract_archive(response, extract_root, destination)


def options_args():
    """
    Validate options and args and return them.
    """
    parser = OptionParser(__doc__)
    parser.add_option("--repo", dest="repo", default='mozilla-central',
                      help="The repository the archive is based on.")
    parser.add_option("--rev", dest="rev", default='tip',
                      help="The revision the archive is based on.")
    parser.add_option("--region", dest="region", default='us-west-2',
                      help="The preferred region of the s3 archive.")
    parser.add_option("--subdir", dest="subdir",
                      help="The path within the root of the archive of where to start extracting "
                           "from. If not supplied, the entire archive will be extracted.")
    parser.add_option("--destination", dest="destination",
                      help="The path location of where to extract the archive to.")
    parser.add_option("--staging", dest='staging', action='store_true', default=False,
                      help="Use staging relengapi")
    parser.add_option("--timeout", dest="timeout", type="float", default=30,
                      help="Used to specify how long to wait until timing out "
                           "for network requests.")
    parser.add_option("--max-retries", dest="max_retries", type="int",
                      default=10,
                      help="A maximum number of retries for network requests.")
    parser.add_option("--sleeptime", dest="sleeptime", type="int", default=10,
                      help="How long to sleep in between network requests.")
    parser.add_option("--debug", dest="debug", action="store_true",
                      default=False, help="Enable debug logging.")

    options, args = parser.parse_args()

    if not len(args) == 1:
        parser.error("archiver_client.py requires exactly 1 argument: the archiver config. "
                     "Valid configs: {}".format(str(ARCHIVER_CONFIGS.keys())))

    if options.debug:
        log.setLevel(logging.DEBUG)
        log.info("Setting DEBUG logging.")

    config = args[0]
    if not ARCHIVER_CONFIGS.get(config):
        log.error("Config argument is unknown. "
                  "Given: '{}', Valid: {}".format(config, str(ARCHIVER_CONFIGS.keys())))
        exit(FAILURE_CODE)

    return options, args


def main():
    options, args = options_args()
    config = args[0]

    api_url = RELENGAPI_HOST['staging' if options.staging else 'production']
    api_url += ARCHIVER_CONFIGS[config]['url_format'].format(
        rev=options.rev, repo=options.repo, region=options.region, suffix='tar.gz'
    )
    subdir = options.subdir or ARCHIVER_CONFIGS[config].get('default_extract_subdir')
    if subdir:
        api_url += "&subdir={}".format(subdir)

    archiver(url=api_url, config_key=config, options=options)

    exit(SUCCESS_CODE)


if __name__ == '__main__':
    main()
