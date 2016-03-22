# CQELS (Continuous Query Evaluation over Linked Data)

This repository is a fork of <https://github.com/KMax/cqels> for the purpose of testing CQELS performance with [SRBench](https://www.w3.org/wiki/SRBench) on lightweight computers. 
It was orginially forked from the CQELS respository on [Google Code](https://code.google.com/p/cqels/).

## Running SRBench

1. `git clone https://github.com/eugenesiow/cqels.git`
2. You need to have [maven](https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html) installed.
3. `cd cqels`
4. `mvn dependency:copy-dependencies package`
5. `cd target`
6. `chmod 775 run.sh`
7. Download station ALPHA's data [here](https://github.com/eugenesiow/cqels/releases/download/data/ALPHA.csv.zip) and place it in the `../knoesis_observations_ike_csv/` or edit the run.sh accordingly if you place it in a different directory.
8. `./run.sh 1 10 1000` where the first 2 parameters are the queries to run from (e.g. q1 to q10) and the 3rd parameter is the delay (in ms) between events. 
9. Results will be output as `output/q1.txt` and so on. Each line in the output is the time taken from the event being sent to the stream to the result being received through the callable.

### Other Projects
* [LSD-ETL](https://github.com/eugenesiow/lsd-ETL)
* [sparql2sql](https://github.com/eugenesiow/sparql2sql)
* [sparql2stream](https://github.com/eugenesiow/sparql2stream)
* [sparql2sql-server](https://github.com/eugenesiow/sparql2sql-server)
* [Linked Data Analytics](http://eugenesiow.github.io/iot/)