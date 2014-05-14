#!/usr/bin/python
from util.post_file import post_multipart
import sys
import string
try:
    import json
except ImportError:
    import simplejson as json


class GraphPost(object):

    def __init__(self, server, selector, branch, resultsname, testresult,
                 sourcestamp, buildid, timestamp):
        self.server = server
        self.selector = selector
        self.branch = branch
        self.resultsname = resultsname
        self.sourcestamp = sourcestamp
        self.buildid = buildid
        self.timestamp = timestamp
        self.testresult = testresult

    def doTinderboxPrint(self, contents, testlongname, testname, prettyval):
        # If there was no error, process the log
        lines = contents.split('\n')
        found = False
        for line in lines:
            if "RETURN" in line:
                tboxPrint =  'TinderboxPrint: ' + \
                    '<a title="%s" href=\'http://%s/%s\'>%s:%s</a>\n' % \
                    (testlongname, self.server, line.split("\t")[3],
                    testname, prettyval)
                print tboxPrint
                found = True
        if not found:
            print >> sys.stderr, "results not added, response: \n" + contents
            raise Exception("graph server did not add results successfully")

    def constructString(self, testname, val):
        info_format = "%s,%s,%s,%s,%s,%s\n"
        str = ""
        str += "START\n"
        str += "AVERAGE\n"
        str += info_format % (self.resultsname, testname, self.branch,
                              self.sourcestamp, self.buildid, self.timestamp)
        str += "%.2f\n" % (float(val))
        str += "END"
        return str

    def postResult(self):
        testname, testlongname, testval, prettyval = self.testresult
        testval = str(testval).strip(string.letters)
        data = self.constructString(testlongname, testval)
        content = post_multipart(self.server, self.selector,
                                 [("key", "value")],
                                 [("filename", "data", data)])
        self.doTinderboxPrint(content, testlongname, testname, prettyval)


def main():
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("--server", dest="server")
    parser.add_option("--selector", dest="selector")
    parser.add_option("--branch", dest="branch")
    parser.add_option("--buildid", dest="buildid")
    parser.add_option("--timestamp", dest="timestamp")
    parser.add_option("--sourcestamp", dest="sourcestamp")
    parser.add_option("--resultsname", dest="resultsname")
    parser.add_option("--properties-file", dest="propertiesFile")
    parser.add_option("--testresults", dest="testresults")

    options, args = parser.parse_args()

    # TODO: check params
    if options.testresults:  # we explicitly pass in testresults
        # Bug 858797 mozharness has access to buildbot properties initially
        # through buildprops.json and then saves further build props in
        # individual files (not json) and in an obj dict. Here it makes sense
        # to just pass testresults rather than creating a properties.json
        testresults = options.testresults
    else:  # we will use options.propertiesFile to obtain testresults
        # in buildbot, we save build properties in a json properties file
        # as the steps progress. 
        properties = json.load(open(options.propertiesFile))
        testresults = properties['properties']['testresults']

    for testresult in testresults:
        gp = GraphPost(server=options.server, selector=options.selector,
                       branch=options.branch, resultsname=options.resultsname,
                       testresult=testresult,
                       sourcestamp=options.sourcestamp, buildid=options.buildid,
                       timestamp=options.timestamp)
        gp.postResult()

if __name__ == '__main__':
    main()
