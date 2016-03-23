*** Log Analysis (phase1)***

1. setup spark (this test run is on linux)
2. Run ==> {path-to-spark-build-dir} ./bin/spark-submit app.py
3. open browser to 0.0.0.0:2251 (remember does not work with ports less than 1250 with sudo run [#linux dist issue] )
4. Port num always more than 1250 in app1.py
5. Debug mode == false #issue0
6. Python errors are logged in foo.log
7. Text outputs in foo.txt
9. Above example takes 1000 log lines from file log1000.txt from /uploads [upload function not working yet]
8. PS: Please ignore naming conventions and code structue for now(Will be fixed soon)

NOTE : place log file under upload folder. Change the filename in app1.py under variable logFile (line 220) 