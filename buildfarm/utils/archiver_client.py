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
    calls relengapi archiver endpoint, downloads returned s3 url, and unpacks it locally
"""

import logging
import shutil
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

EXIT_CODE = FAILURE_CODE

ENDPOINT_CONFIG = {
    'mozharness': {
        'url_format': "archiver/{endpoint}/{rev}?repo={repo}&region={region}&suffix={suffix}",
        'archive_root': "{repo}-{rev}",
        'archive_subdir': "testing/mozharness",
    }
}

SUBDIR_ROOT = {
    "mozharness": "mozharness/"
}

RELENGAPI_HOST = {
    'staging': 'https://api-pub-build.allizom.org/',
    'production': 'https://api.pub.build.mozilla.org/'
}

logging.basicConfig(format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)


# This has been copied from lib.python.util.retry
def retrier(attempts=5, sleeptime=10, max_sleeptime=300, sleepscale=1.5, jitter=1):
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
    response = None
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
            response = urllib2.urlopen(s3_url)
        else:
            log.error("An s3 URL could not be determined even though archiver task completed. Check"
                      "archiver logs for errors. Task status: {}".format(task_result['status']))
    else:
        log.error("Archiver's task could not be resolved. Check archiver logs for errors. Task "
                  "status: {}".format(task_result['status']))
    return response


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
                if not response:
                    return INFRA_CODE

            if response.code == 200:
                break

        except urllib2.HTTPError as e:
            if num == options.max_retries - 1:
                log.exception("HTTPError Exception: {}".format(e))
                return INFRA_CODE
        except urllib2.URLError as e:
            if num == options.max_retries - 1:
                log.exception("URLError for {}. Exception: {}".format(url, e))
                return INFRA_CODE
        num += 1

    if not response.code == 200:
        content = response.read()
        log.error("could not determine a valid url response. return code: '{}'"
                  "return content: '{}".format(response.code, content))
        return INFRA_CODE
    return response


def download_and_extract_archive(response, archive_name):
    log.info("downloading archive: {}".format(archive_name))
    try:
        with open(archive_name, 'wb') as archivef:
            archivef.write(response.read())
    except IOError, e:
        log.exception("IOError: {}".format(e))
        return INFRA_CODE

    log.info("extracting downloaded archive: {}".format(archive_name))
    try:
        with tarfile.open(archive_name) as tar:
            tar.extractall()
    except tarfile.TarError as e:
        log.exception("TarError: {}".format(e))
        return INFRA_CODE

    return SUCCESS_CODE


def copy_archive_to_destination(source, destination):
    """
    copies extracted source contents of an archive into a destination path. If the destination
    has relatively similiar path names, those destination paths will be overwritten.
    """
    if not os.path.exists(source):
        log.error("Can't determine source directory where archive was extracted to. "
                  "Expected: {}".format(source))
        return INFRA_CODE
    try:
        if not os.path.exists(destination):
            os.makedirs(destination)

        log.info("Moving extracted path's contents: '{}'"
                 "to destination: '{}'".format(source, destination))
        for src_path in os.listdir(source):
            abs_src_path = os.path.join(source, src_path)
            abs_dest_path = os.path.join(destination, src_path)
            if os.path.exists(abs_dest_path):
                if os.path.isdir(abs_dest_path):
                    shutil.rmtree(abs_dest_path)
                else:
                    os.remove(abs_dest_path)
            shutil.move(abs_src_path, abs_dest_path)
    except IOError, e:
        log.exception("IOError: {}".format(e))
        return INFRA_CODE
    except shutil.Error as e:
        log.exception("shutil.Error: {}".format(e))
        return INFRA_CODE

    return SUCCESS_CODE


def clean_up(archive_path, extracted_root_source):
    """
    cleans up archive and extracted archive source.
    """
    log.info("Cleaning up archive and archive remnants.")
    log.info("Removing paths '{}' and '{}".format(archive_path, extracted_root_source))
    try:
        os.remove(archive_path)
        shutil.rmtree(extracted_root_source)
    except IOError, e:
        log.exception("IOError: {}".format(e))
        return INFRA_CODE
    except shutil.Error as e:
        log.exception("shutil.Error: {}".format(e))
        return INFRA_CODE

    return SUCCESS_CODE


def get_archive(url, endpoint, options):
    """
    1) obtains valid s3 url for archive via relengapi's archiver endpoint
    2) downloads and extracts archive
    3) merges contents of extracted archive into destination, overwriting duplicate paths
    4) cleans up archive and leftover extracted paths
    """
    archive_cfg = ENDPOINT_CONFIG[endpoint]  # specifics to the the archiver endpoint
    archive_path = "{}.tar.gz".format(endpoint)  # filename of downloaded archive
    # determine the extracted archive source and dest paths
    extracted_root_source = archive_cfg['archive_root'].format(repo=os.path.basename(options.repo),
                                                               rev=options.rev)
    if archive_cfg.get('archive_subdir'):
        extracted_full_source = os.path.join(extracted_root_source, archive_cfg['archive_subdir'])
    else:
        extracted_full_source = extracted_root_source
    extracted_destination = options.destination or os.path.join(os.getcwd(), endpoint)

    response = get_url_response(url, options)

    return_code = download_and_extract_archive(response, archive_path)

    if return_code == SUCCESS_CODE:
        return_code = copy_archive_to_destination(source=extracted_full_source,
                                                  destination=extracted_destination)

        if return_code == SUCCESS_CODE:
            return_code = clean_up(archive_path, extracted_root_source)

    return return_code


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
        parser.error("archiver_client.py requires exactly 1 argument: the archiver endpoint. "
                     "Valid endpoints: {}".format(str(ENDPOINT_CONFIG.keys())))

    if options.debug:
        log.setLevel(logging.DEBUG)
        log.info("Setting DEBUG logging.")

    return options, args


def main():
    options, args = options_args()
    endpoint = args[0]

    if not ENDPOINT_CONFIG.get(endpoint):
        log.error("endpoint argument is unknown. "
                  "Given: '{}', Valid: {}".format(endpoint, str(ENDPOINT_CONFIG.keys())))
        exit(FAILURE_CODE)

    api_url = RELENGAPI_HOST['staging' if options.staging else 'production']
    api_url += ENDPOINT_CONFIG[endpoint]['url_format'].format(
        endpoint=endpoint, rev=options.rev, repo=options.repo, region=options.region, suffix='tar.gz'
    )

    exit(get_archive(url=api_url, endpoint=endpoint, options=options))


if __name__ == '__main__':
    main()
