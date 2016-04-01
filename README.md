*** Log Analysis (phase1)***

1. setup spark (this test run is on linux)
2. Run ==> {path-to-spark-build-dir} ./bin/spark-submit runapp.py
3. open browser to 0.0.0.0:2253 (does not work with ports less than 1250)
4. Python errors are logged in foo.log
5. Text outputs in foo.txt
6. apis for localhost log
	ip/resourcecountjson
	ip/ipcountjson
	ip/responsecodecountjson
	ip/avgresponsetimejson
	ip/bytestransferredjson
	ip/gmtlocationjson
	ip/getpostrequestjson