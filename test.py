import sys
import os
from test_helper import Test

baseDir = os.path.join('uploads')
#inputPath = os.path.join('cs100', 'lab2', 'apache.access.log.PROJECT')
inputPath = os.path.join('test.txt')
logFile = os.path.join(baseDir, inputPath)

print type(logFile),logFile
try:
   fh = open(logFile, "w")
   print "opened"
   fh.write("This is my test file forefe exception frteet handling!!")
except IOError:
   print "Error: can\'t find file or read data"