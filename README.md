
<h2>Misc Utility Applications for Data Generation, Loading, Search etc</h2>


<b>Redis Data Loader</b>

This app uses Jedis (Jedis Pipelining) to load data into a Redis DB as Hash or JSON objects from a template file.

See sample file <i>data-template-stock-trades.json</i>

To run the application.

#1 Download the repository from GIT (check pom.xml for maven dependencies)

#2 Create a .properties with connection details (use config.properties as the template)

#3 Execute App.java for loading sample data, bulk deletion etc

#4 Execute AppSearch.java for creating a search index from a template file and running search queries.

See sample file <i>index-def-trades.json</i> for Search Index definitions.