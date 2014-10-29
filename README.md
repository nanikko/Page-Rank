###Implementation of PageRank algorithm
--
__Main.java__ The Main() method of the program is located in this file. The execution starts from here. It collects the command line arguments and calls the subsequent jobs from the main function. Initially Graph properties job is called first, RankInitializer second, PageRank third and ListTop10Nodes fourth.

__GraphProperties.java__ This class creates a job, Mapper & Reducer for calculating graph properties like Minimum out-degree, Maximum out- degree, Average out-degree, Number of nodes and Number of edges. It creates the job in GraphPropertiesJob() function.

__RankInitializer.java__ This class contains a job, Mapper & Reducer for initializing the page ranks to all nodes. Call to mapper is made from the job function, InitializeRank().

__PageRank.java__ This class contains a job, Mapper & Reducer for calculating the Page Ranks for all the nodes. It also considers the Damping factor while calculating the rank for next iteration.

__ListTop10Nodes.java__ This class contains a job, Mapper & Reducer implementations for getting the Top 10 nodes in the result from Page Rank job. The ranks are ordered in Descending order.


