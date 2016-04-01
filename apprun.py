import os
import json
import logging
from flask import Flask, render_template, request, redirect, url_for, send_from_directory
from werkzeug import secure_filename
from logging.handlers import RotatingFileHandler

import re
import datetime

import sys
import os
from test_helper import Test

from pyspark.sql import Row
import pyspark
sc = pyspark.SparkContext()
# Initialize the Flask application
app = Flask(__name__)

# This is the path to the upload directory
app.config['UPLOAD_FOLDER'] = 'uploads/'

app.config['ALLOWED_EXTENSIONS'] = set(['txt', 'pdf','PROJECT','log'])

filename = "log1000.txt"
#APACHE_ACCESS_LOG_PATTERN = '\[([\w]+)[/]([\w]+)[/]([\w]+)[:]([\d]+)[:]([\d]+)[:]([\d]+) ([+\-]\d{4})\] (\d{0,3}[.]\d{0,3}[.]\d{0,3}[.]\d{0,3}) (\d{0,3}[.]\d{0,3}[.]\d{0,3}[.]\d{0,3}) (\S+) (\S+) (\d+) (\S+) (\S+)\s*(\S*) (\d{3}) ([-]|[\d]+) (\d+)'
APACHE_ACCESS_LOG_PATTERN = ''

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
@app.route('/regex',methods=['POST'])
def regex():
    global APACHE_ACCESS_LOG_PATTERN
    text = request.form['text']
    APACHE_ACCESS_LOG_PATTERN = text
    return render_template('index.html')

#Shows file name for now
@app.route('/uploads/<filename>')
def uploaded_file(filename):
    testret = parseLogs()
    return send_from_directory(app.config['UPLOAD_FOLDER'],
                               filename)

@app.route('/resourcecountjson',methods=['POST','GET'])
def resourcecountjson():     
    resourceCountList = resource()
    fo = open("foo.txt", "a")
    fo.write("resourceCountList" + str((resourceCountList)))
    fo.write("\n") 
    resourcejson = json.dumps(resourceCountList)
    return resourcejson       
           
@app.route('/ipcountjson',methods=['POST','GET'])           
def ipcountjson():
    ipsAccessingCountList=ipcount()
    fo = open("foo.txt", "a")
    fo.write("ipsAccessingCountList"+str((ipsAccessingCountList)))
    fo.write("\n")
    ipcountjson=json.dumps(ipsAccessingCountList)
    return ipcountjson

@app.route('/responsecodecountjson',methods=['POST','GET'])           
def responsecodecountjson():
    responseCodeToCountList=responseCodeCount()
    fo = open("foo.txt", "a")
    fo.write("responseCodeToCountList"+str((responseCodeToCountList)))
    fo.write("\n")
    responseCodeCountJson=json.dumps(responseCodeToCountList)
    return responseCodeCountJson

@app.route('/avgresponsetimejson',methods=['POST','GET'])           
def avgresponsetimejson():
    avgResponseTimeValue=avgResponseTime()
    fo = open("foo.txt", "a")
    fo.write("avgResponsetimeValue"+str((avgResponseTimeValue)))
    fo.write("\n")
    avgResponseTimeJson=json.dumps(avgResponseTimeValue)
    return avgResponseTimeJson


@app.route('/bytestransferredjson',methods=['POST','GET'])           
def bytestransferredjson():
    bytesTransferredValue=bytesTransferred()
    fo = open("foo.txt", "a")
    fo.write("bytesTransferredValue"+str((bytesTransferredValue)))    
    fo.write("\n")
    bytesTransferredJson=json.dumps(bytesTransferredValue)
    return bytesTransferredJson

@app.route('/gmtlocationjson',methods=['POST','GET'])           
def gmtlocationjson():
    gmtLocationList=gmt()
    fo = open("foo.txt", "a")
    fo.write("gmtLocationList"+str((gmtLocationList)))    
    fo.write("\n")
    gmtLocationJson=json.dumps(gmtLocationList)
    return gmtLocationJson

@app.route('/getpostrequestjson',methods=['POST','GET'])               
def getpostrequestjson():
    getPostRequestList=GetPostRequests()
    fo = open("foo.txt", "a")
    fo.write("getPostRequestList"+str((getPostRequestList)))    
    fo.write("\n")
    getPostRequestJson=json.dumps(getPostRequestList)
    return getPostRequestJson

@app.route('/parse',methods=['POST','GET'])
def chart_parse():
    global filename
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

    
    return render_template('test1.html',data=map(json.dumps, data))

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
    global APACHE_ACCESS_LOG_PATTERN
    
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:               
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









def parseLogs():
    """ Read and parse log file """
    global filename
    baseDir = os.path.join('uploads')
    inputPath = os.path.join(filename)
    filepath = 'uploads/'+filename
    logFile = os.path.join(filepath)
        
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
    log_result = [parsed_logs.count(), access_logs.count(), failed_logs.count()]
    return log_result

def parseLogsCollect():
    """ Read and parse log file """
    global filename    
    baseDir = os.path.join('uploads')
    inputPath = os.path.join(filename)
    filepath = 'uploads/'+filename
    logFile = os.path.join(filepath)
    
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
    log_result = [parsed_logs.count(), access_logs.count(), failed_logs.count()]
    return parsed_logs,access_logs,failed_logs

@app.route('/resource',methods=['POST','GET'])
def resource():
    #No. of request for each resource
    from operator import add

    parsed_logs,access_logs,failed_logs = parseLogsCollect()

    resourceCount = access_logs.map(lambda log: (log.reqResource,1)).reduceByKey(lambda a,b:a+b)

    
    resourceCountList=resourceCount.reduceByKey(lambda a, b : a + b).take(1)
    resourcejson = json.dumps(resourceCountList)

    print 'No. of request for each resource',resourceCountList

    return resourceCountList
    
def responseCodeCount():
    #Status code request count
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
    return responseCodeToCountList

def ipcount():
    #IPs accesing and their counts
    parsed_logs,access_logs,failed_logs = parseLogsCollect()
    ipsAccessingCount=access_logs.map(lambda log:(log.ip1,1))
    ipsAccessingCountList=ipsAccessingCount.reduceByKey(lambda a,b:a+b).take(1)
    print 'IPs accesing and their counts',ipsAccessingCountList   
    return ipsAccessingCountList

def avgResponseTime():
    #Average response time
    parsed_logs,access_logs,failed_logs = parseLogsCollect()
    response_time=access_logs.collect()
    sum_response_time=0.0
    for l in response_time:
        sum_response_time=sum_response_time + float(l[13])
    average_response_time=1.0*sum_response_time/access_logs.count()
    print average_response_time
    return average_response_time

def bytesTransferred():
    #Total number of bytes transferred
    parsed_logs,access_logs,failed_logs = parseLogsCollect()
    bytes_transferred=access_logs.collect()
    sum_bytes=0
    for l in bytes_transferred:
        sum_bytes=sum_bytes + l[16]
    print sum_bytes,"bytes"
    return sum_bytes

def gmt():
    # IPs at same location or same region (with same GMT)
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


if __name__ == '__main__':
    handler = RotatingFileHandler('foo.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)
    app.run(
        host="0.0.0.0",
        port=int("2253")
        #debug=True , Do not uncomment this #issue1
    )