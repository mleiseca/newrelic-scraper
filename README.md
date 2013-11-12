newrelic-scraper
================

newrelic-scraper

Steps for usage:
---------------

* copy config-example.json to config.json
* edit config.json to include
    * your NewRelic accountId
    * your NewRelic api key
    * The names of the apps that you would like to pull data for


### Execute
perl pull_dbstats.pl "2013-11-10T22:00:00Z" "2013-11-11T03:00:00Z" output.csv
