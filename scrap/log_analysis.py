
LOG_PATTERN='\[([\w]+)[/]([\w]+)[/]([\w]+)[:]([\d]+)[:]([\d]+)[:]([\d]+) ([+\-]\d{4})\] (\d{0,3}[.]\d{0,3}[.]\d{0,3}[.]\d{0,3}) (\d{0,3}[.]\d{0,3}[.]\d{0,3}[.]\d{0,3}) (\S+) (\S+) (\d+) (\S+) (\S+)\s*(\S*) (\d{3}) ([-]|[\d]+) (\d+)'
#print re.search(LOG_PATTERN,'[24/Jul/2015:13:04:25 +0200] 10.109.125.199 10.109.125.199 - - 8444 POST /cuic/report/reportviewer/ReportViewer.htmx  HTTP/1.1 302 - 1')




import re
import datetime
import sys
import os
from test_helper import Test
from pyspark.sql import Row
import pyspark
sc = pyspark.SparkContext()
LOG_PATTERN='\[([\w]+)[/]([\w]+)[/]([\w]+)[:]([\d]+)[:]([\d]+)[:]([\d]+) ([+\-]\d{4})\] (\d{0,3}[.]\d{0,3}[.]\d{0,3}[.]\d{0,3}) (\d{0,3}[.]\d{0,3}[.]\d{0,3}[.]\d{0,3}) (\S+) (\S+) (\d+) (\S+) (\S+)\s*(\S*) (\d{3}) ([-]|[\d]+) (\d+)'

def parseApacheLogLine(logline):    
    match = re.search(LOG_PATTERN, logline)
    print match
    if match is None:
        return (logline, 0)
    temp = match.group(17)
    if temp == '-':
        size_sent = long(0)
    else:
        size_sent = long(match.group(17))
    temp = match.group(10)
    if temp == '-':
        id_remote = int(0)
    else:
        id_remote = int(match.group(10))
    temp = match.group(11)
    if temp == '-':
        id_logon = int(0)
    else:
        id_logon = int(match.group(11))
    temp = match.group(18)
    if temp == '-':
        resp_time = int(0)
    else:
        resp_time = int(match.group(18))

    return (Row(
        dayDate=(match.group(1)),
        year=(match.group(2)),
        month=match.group(3),
        hour=(match.group(4)),
        minute=(match.group(5)),
        seconds=(match.group(6)),
        diffGmt=match.group(7),
        ip1=match.group(8),
        ip2=match.group(9),
        idRemoteMachine=id_remote,
        idLocalLogon=id_logon,
        webserverPort=int(match.group(12)),
        method=match.group(13),
        reqResource=match.group(14),
        clientProtocol=match.group(15),
        statusCode=(match.group(16)),
        bytesSent=size_sent,
        responseTime=resp_time
    ), 1)


logFile = os.path.join('test1.PROJECT')

def parseLogs():
    """ Read and parse log file """
    parsed_logs = (sc.textFile(logFile).map(parseApacheLogLine).cache())
#   parse_count = parsed_logs.count()
    
    access_logs = (parsed_logs.filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())
    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs.count()
        for line in failed_logs.take(20):
            print 'Invalid logline: %s' % line
    
    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
#    print 'Read %d lines, successfully parsed %d lines' % parsed_logs.count(),access_logs.count()
    return parsed_logs, access_logs,failed_logs



from operator import add

parsed_logs, access_logs,failed_logs = parseLogs()
resourceCount = access_logs.map(lambda log: (log.reqResource,1)).reduceByKey(lambda a,b:a+b)
print resourceCount.take(10)
requestCount=access_logs.map(lambda log:(log.ip1,1)).reduceByKey(lambda a,b:a+b)
print requestCount.take(100)


#No. of request for each resource
#IPs accesing and their counts
#Status code request count
resourceCountList=resourceCount.reduceByKey(lambda a, b : a + b).take(1)
print 'No. of request for each resource',resourceCountList
ipsAccessingCount=access_logs.map(lambda log:(log.ip1,1))
ipsAccessingCountList=ipsAccessingCount.reduceByKey(lambda a,b:a+b).take(1)
print 'IPs accesing and their counts',ipsAccessingCountList

responseCodeToCount = (access_logs
                       .map(lambda log: (log.statusCode, 1))\
                       .reduceByKey(lambda a, b : a + b)\
                       .cache())
responseCodeToCountList = responseCodeToCount.take(10)
print 'Status code request count ',responseCodeToCountList
hostCountPairTuple = access_logs.map(lambda log: (log.ip1, 1))
hostSum = hostCountPairTuple.reduceByKey(lambda a, b : a + b)
hostMoreThan10 = hostSum.filter(lambda s: s[1] > 10)
#hostsPick20 = (hostMoreThan10
#               .map(lambda s: s[0])
 #              .take(20))




#used for graph
labels = responseCodeToCount.map(lambda (x, y): x).collect()
print labels
count = access_logs.count()
fracs = responseCodeToCount.map(lambda (x, y): (float(y) / count)).collect()
print fracs



"""
import matplotlib.pyplot as plt

def pie_pct_format(value):    
    return '' if value < 7 else '%.0f%%' % value

fig = plt.figure(figsize=(4.5, 4.5), facecolor='white', edgecolor='white')
colors = ['yellowgreen', 'lightskyblue']
explode = ( 0.1, 0)
patches, texts, autotexts = plt.pie(fracs, labels=labels, colors=colors,
                                    explode=explode, autopct=pie_pct_format,
                                    shadow=True,  startangle=125
                                   )
for text, autotext in zip(texts, autotexts):
    if autotext.get_text() == '':
        text.set_text('')  # If the slice is small to fit, don't show a text label
plt.legend(labels, loc=(0.80, -0.1), shadow=True)
pass
"""



# Average response time
response_time=access_logs.collect()
sum1=0
for l1 in response_time:
    sum1=sum1 + l1[13]
average=1.0*sum1/access_logs.count()
print "average:",average

