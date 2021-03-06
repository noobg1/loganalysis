import os

from flask import Flask, render_template, request, redirect, url_for, send_from_directory
from werkzeug import secure_filename

import pyspark
sc = pyspark.SparkContext()
# Initialize the Flask application
app = Flask(__name__)

# This is the path to the upload directory
app.config['UPLOAD_FOLDER'] = 'uploads/'

app.config['ALLOWED_EXTENSIONS'] = set(['txt', 'pdf','PROJECT'])

filename = "test1.PROJECT"
# For a given file, return whether it's an allowed type or not
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in app.config['ALLOWED_EXTENSIONS']
           

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
    return render_template('test.html',**locals())

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
logFile = os.path.join(baseDir, inputPath)

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


# In[ ]:






if __name__ == '__main__':
    app.run(
        host="0.0.0.0",
        port=int("2251")
        #debug=True , Do not uncomment this #issue1
    )
