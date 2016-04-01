import os
import json
import logging
from flask import Flask, render_template, request, redirect, url_for, send_from_directory
from werkzeug import secure_filename
from logging.handlers import RotatingFileHandler

import pyspark
sc = pyspark.SparkContext()
# Initialize the Flask application
app = Flask(__name__)

# This is the path to the upload directory
app.config['UPLOAD_FOLDER'] = 'uploads/'

app.config['ALLOWED_EXTENSIONS'] = set(['txt', 'pdf','PROJECT'])

filename = "log1000-1.txt"
# For a given file, return whether it's an allowed type or not
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in app.config['ALLOWED_EXTENSIONS']
           

@app.route('/layout')
def layout():
    return render_template('layout.html')

@app.route('/')
def index():
    rdd = sc.parallelize(['Hello','World'])
    words = sorted(rdd.collect())
    print "sumthin happened"
    return render_template('index.html')


# Route that will process the file upload
@app.route('/upload', methods=['POST'])
def upload():
    global filename
    # Get the name of the uploaded file
    file = request.files['file']
    # Check if the file is one of the allowed types/extensions
    if file and allowed_file(file.filename):
        # Make the filename safe, remove unsupported chars
        filename = secure_filename(file.filename)
        # Move the file form the temporal folder to
        # the upload folder we setup
        print "type:",type(filename)
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        # Redirect the user to the uploaded_file route,        
        return redirect(url_for('uploaded_file',
                                filename=filename))
        #return redirect(url_for('/parse'))

#Shows file name for now
@app.route('/uploads/<filename>')
def uploaded_file(filename):
    testret = parseLogs()
    return send_from_directory(app.config['UPLOAD_FOLDER'],
                               filename)
    #return redirect(url_for('/parse'))
                               

@app.route('/parse',methods=['POST','GET'])
def chart_parse():
    result = parseLogs()
    data = result
    
    if request.method == 'POST':   
        parse = request.form['parse']

        if parse == "logparse":
            return render_template('test.html',**locals())
            pass
        elif parse =="resource":
            resourceCountList = resource()
            fo = open("foo.txt", "a")
            fo.write("resourceCountList" + str((resourceCountList)))
            fo.write("\n") 
            resourcejson = json.dumps(resourceCountList)
            return render_template('test2.html',result =resourcejson)        
            pass  
        elif parse == "ips":
            ipsAccessingCountList = ipcount() 
            fo = open("foo.txt", "a")
            fo.write("ipsAccessingCountList" +str( (ipsAccessingCountList)))
            fo.write("\n") 
            ipsAccessingCountListjson =  json.dumps(ipsAccessingCountList)                  
            return render_template('test2.html',result = ipsAccessingCountListjson)
        elif parse == "responseCode":
            responseCodeToCountList = responseCodeCount()
            fo = open("foo.txt", "a")
            fo.write("responseCodeToCountList" + str((responseCodeToCountList)))
            fo.write("\n")            
            responseCodeToCountListjson = json.dumps(responseCodeToCountList)            
            return render_template('test2.html',result = responseCodeToCountListjson)  
        elif parse == "avgResponseTime":
            avgResponsetime = avgResponseTime()
            fo = open("foo.txt", "a")
            fo.write("avgResponseTime(sec)" + str( (avgResponsetime)))
            fo.write("\n")           
            avgResponsetimejson = json.dumps(avgResponsetime)
            return render_template('test2.html',result = avgResponsetime)  
        elif parse == "bytesTrans":
            BytesTransferred = bytesTransferred()
            fo = open("foo.txt", "a")
            fo.write("BytesTransferred (bytes)" +str( (BytesTransferred)))
            fo.write("\n")            
            BytesTransferredjson = json.dumps(BytesTransferred)
            return render_template('test2.html',result = BytesTransferredjson)
        elif parse == "gmt":
            ips_gmt = gmt()
            fo = open("foo.txt", "a")
            fo.write("IPs from same GMT" +str( (ips_gmt)))
            fo.write("\n")
            ips_gmt_json = json.dumps(ips_gmt)
            return render_template('test2.html',result = ips_gmt_json)
        elif parse == "getPost":
            getPost = GetPostRequests()
            fo = open("foo.txt", "a")
            fo.write(" No of GetPost requests" +str( (getPost)))
            fo.write("\n")            
            getPostjson = json.dumps(getPost)
            return render_template('test2.html',result = getPostjson)





    #return render_template('test1.html',**locals(),data=map(json.dumps, data))
    return render_template('test1.html',data=map(json.dumps, data))

# coding: utf-8

# In[1]:

import re
import datetime

from pyspark.sql import Row

#import pyspark
#sc = pyspark.SparkContext()

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

def parse_apache_time(s):
    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parseApacheLogLine(logline):
    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        #size = long(match.group(9))
               
        return (Row(
                dayDate=match.group(1),
                year=match.group(2),
                month=match.group(3),
                hour=match.group(4),
                minute=(match.group(5)),
                seconds=(match.group(6)),
                diffGmt=match.group(7),
                ip1=match.group(8),
                ip2=match.group(9),
                idRemoteMachine=match.group(10),
                idLocalLogon=match.group(11),
                webserverPort=int(match.group(12)),
                method=match.group(13),
                reqResource=match.group(14),
                clientProtocol=match.group(15),
                statusCode=(match.group(16)),
                bytesSent=(match.group(17)),
                temp1=match.group(18)       
        ),1)


# In[2]:

# A regular expression pattern to extract fields from the log line
#APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
APACHE_ACCESS_LOG_PATTERN = '\[([\w]+)[/]([\w]+)[/]([\w]+)[:]([\d]+)[:]([\d]+)[:]([\d]+) ([+\-]\d{4})\] (\d{0,3}[.]\d{0,3}[.]\d{0,3}[.]\d{0,3}) (\d{0,3}[.]\d{0,3}[.]\d{0,3}[.]\d{0,3}) (\S+) (\S+) (\d+) (\S+) (\S+)\s*(\S*) (\d{3}) ([-]|[\d]+) (\d+)'


# In[3]:

import sys
import os
from test_helper import Test

baseDir = os.path.join('uploads')
#inputPath = os.path.join('cs100', 'lab2', 'apache.access.log.PROJECT')
inputPath = os.path.join(filename)
#logFile = os.path.join(baseDir, inputPath)
logFile = os.path.join('uploads/log1000.txt')
#@app.route('/uploads/<filename>')
def parseLogs():
    """ Read and parse log file """
    global filename
    
    parsed_logs = (sc
                   .textFile(logFile)
                   .map(parseApacheLogLine)
                   .cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
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
    
    print parsed_logs.count()
    #return "successfully parsed"+str(parsed_logs.count())
    log_result = [parsed_logs.count(), access_logs.count(), failed_logs.count()]
    return log_result
    #return parsed_logs

#parsed_logs, access_logs, failed_logs = parseLogs()
#parsed_logs = parseLogs()
def parseLogsCollect():
    """ Read and parse log file """
    global filename
    
    parsed_logs = (sc
                   .textFile(logFile)
                   .map(parseApacheLogLine)
                   .cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
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
    
    print parsed_logs.count()
    #return "successfully parsed"+str(parsed_logs.count())
    log_result = [parsed_logs.count(), access_logs.count(), failed_logs.count()]
    return parsed_logs,access_logs,failed_logs

@app.route('/resource',methods=['POST','GET'])
def resource():
    from operator import add

    parsed_logs,access_logs,failed_logs = parseLogsCollect()

    resourceCount = access_logs.map(lambda log: (log.reqResource,1)).reduceByKey(lambda a,b:a+b)
    print resourceCount.take(10)
    requestCount=access_logs.map(lambda log:(log.ip1,1)).reduceByKey(lambda a,b:a+b)
    print requestCount.take(100)


    #No. of request for each resource
    #IPs accesing and their counts
    #Status code request count
    resourceCountList=resourceCount.reduceByKey(lambda a, b : a + b).take(1)
    
    #with open(outputfilename, 'wb') as outfile:
    #   json.dump(row, outfile)
    resourcejson = json.dumps(resourceCountList)
    #fo.write(resourcejson)
    print 'No. of request for each resource',resourceCountList
    

    

    return resourceCountList
    #return render_template('test1.html',resourceCountList=resourcejson)
def responseCodeCount():
    parsed_logs,access_logs,failed_logs = parseLogsCollect()
    responseCodeToCount = (access_logs
                           .map(lambda log: (log.statusCode, 1))\
                           .reduceByKey(lambda a, b : a + b)\
                           .cache())
    responseCodeToCountList = responseCodeToCount.collect()
    print 'Status code request count ',responseCodeToCountList
    hostCountPairTuple = access_logs.map(lambda log: (log.ip1, 1))
    hostSum = hostCountPairTuple.reduceByKey(lambda a, b : a + b)
    hostMoreThan10 = hostSum.filter(lambda s: s[1] > 10)

    #used for graph
    labels = responseCodeToCount.map(lambda (x, y): x).collect()
    print labels
    count = access_logs.count()
    fracs = responseCodeToCount.map(lambda (x, y): (float(y) / count)).collect()
    print fracs
    fracs = [i * 100 for i in fracs]
    return responseCodeToCountList

def ipcount():
    parsed_logs,access_logs,failed_logs = parseLogsCollect()
    ipsAccessingCount=access_logs.map(lambda log:(log.ip1,1))
    ipsAccessingCountList=ipsAccessingCount.reduceByKey(lambda a,b:a+b).take(1)
    print 'IPs accesing and their counts',ipsAccessingCountList
   
    return ipsAccessingCountList

def avgResponseTime():
    parsed_logs,access_logs,failed_logs = parseLogsCollect()
    response_time=access_logs.collect()
    sum_response_time=0.0
    for l in response_time:
        sum_response_time=sum_response_time + float(l[13])
    average_response_time=1.0*sum_response_time/access_logs.count()
    print average_response_time
    return average_response_time

def bytesTransferred():
    parsed_logs,access_logs,failed_logs = parseLogsCollect()
    bytes_transferred=access_logs.collect()
    sum_bytes=0
    for l in bytes_transferred:
        sum_bytes=sum_bytes + l[16]
    print sum_bytes,"bytes"
    return sum_bytes

def gmt():
    # IPs at same location
    parsed_logs,access_logs,failed_logs = parseLogsCollect()
    gmt_location=access_logs.map(lambda log:((log.diffGmt,log.ip1),1)).reduceByKey(lambda x,y:x+y)
    gmt_location_list=gmt_location.collect()
    for l in gmt_location_list:
        print l    
    return gmt_location_list

def GetPostRequests():
    # No. of GET and POST requests
    parsed_logs,access_logs,failed_logs = parseLogsCollect()
    requests=access_logs.map(lambda log:(log.method,1)).reduceByKey(lambda x,y:x+y)
    requests_list=requests.collect()
    for l in requests_list:
        print l
    return requests_list

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







if __name__ == '__main__':
    handler = RotatingFileHandler('foo.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)
    app.run(
        host="0.0.0.0",
        port=int("2251")
        #debug=True , Do not uncomment this #issue1
    )
