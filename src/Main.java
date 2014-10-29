/*
 * Main class for the PageRank Project
 * In the main method we call and calculate the time taken for CheckingGraphProperties, Initializing graph with ranks, calculating Page rank for nodes
 * and for listing the top 10 nodes .
 * 
 * Input
 * args[0]: input file path
 * args[1]: outputfile path
 * args[2]: damping factor
 * 
 * Output: 
 * 	separate folders will be created for each of below functionalities:
 * 		1 output folder for graph properties
 * 		1 output folder for initialization of Ranks 
 * 		Multiple output folders for page rank calculation
 * 		1 Output Folder for listing top 10 nodes.
 * 	
 * 
 * */
public class Main {

	public static void main(String[] args) throws Exception{
		
		String inputFile = args[0];
		String outFile = args[1];
		String dampFactor = args[2];
		long startTime;
		long endTime;
		int iteration;
		startTime =System.currentTimeMillis();
		GraphProperties.checkGraphProperties(inputFile, outFile+"OfGraphProperties");
		endTime = System.currentTimeMillis();
		System.out.println("Time Taken For checking Graph Properties "+(endTime-startTime)+" ms");
		
		startTime =System.currentTimeMillis();
		RankInitializer.initializeRank(inputFile, outFile+"1");
		endTime = System.currentTimeMillis();
		System.out.println("Time Taken For Initializing Rank for graph "+(endTime-startTime)+" ms");
		
		startTime =System.currentTimeMillis();
		
		for (iteration = 2; iteration < 70; iteration++) {
			long[] x = PageRank.CalculateRank(outFile+(iteration - 1) +"/", outFile+iteration,dampFactor);
			if(iteration==11){
				endTime = System.currentTimeMillis();
				System.out.println("Time Taken For 10 iterations in calculating PageRank for graph "+(endTime-startTime)+" ms");
			}
			if (x[0] == 0) {
				break;
			}

	}
		
		
		startTime =System.currentTimeMillis();
		ListTop10Nodes.listTop10Nodes(outFile+(iteration-1)+"/",outFile+"OfFinalTop10" );
		endTime = System.currentTimeMillis();
		System.out.println("Time Taken For listingTop10Nodes "+(endTime-startTime)+" ms");
		
		
		

}
}
