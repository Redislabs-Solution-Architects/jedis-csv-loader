
<h2>Misc Utility Applications for Data Generation, Loading, Search etc</h2>


<b>Redis Data Loader</b>

This app uses Jedis (Jedis Pipelining) to load data from a CSV file into Redis Hash or JSON objects.

It can add 1 level of nesting in JSON, to support 1 to Many use cases. e.g. Order Header > Order Line Items

See sample file Subscriptions-10.csv

Note: This code is for demo / learning purposes only. Use the RIOT utility to load data into Redis.

To run the application.

#1 Download the repository from GIT (this uses maven as the build tool)

#2 Create a .properties with connection details (use config.properties as the template)

#3 Execute App.java for loading sample data, buld deletion etc

#4 Execute AppSearch.java for creating a search index and running search queries.