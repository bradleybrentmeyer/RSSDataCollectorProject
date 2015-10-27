package dataCollection;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.Date;

public class RSSDriver {
	
	private static final int NTHREDS = 105; //514 in total  
	
	RSSDriver(){
		
	}
	
    public static void main(String[] args) throws IOException{ 

    	System.out.println((new Date()).toString() + " Starting RSS server on directory " + System.getProperty("user.dir"));
        
    	// set time interval between RSS pulls
		long minute = 60000;
  	  	long sleepFor = minute * 30;
  	    int prevHr = 25;
        int hr = 0;
          	    
  	  	// load the rss feeds list
    	String inFile = "rssFeedsList.txt";
        BufferedReader inputFile = null;
  	  	ArrayList<String> rssFeeds = new ArrayList<String>();
  	    String line = "";
  	    inputFile = new BufferedReader (new FileReader (inFile));
  	    while ((line = inputFile.readLine()) != null) {
  	    	rssFeeds.add(line);
  	    }	
  	  	int rssCnt = rssFeeds.size();
  	    inputFile.close();
  	  	
        // kick off all of the RSS feed collector threads using a thread pool and get stock quotes
        try{
        	while (true){
        		System.out.println((new Date()).toString() + " Generating worker threads");	
	        	ExecutorService executor = Executors.newFixedThreadPool(NTHREDS);
	        	List<Future<Long>> list = new ArrayList<Future<Long>>();
	        	String delimiter = ",";
        	    
	        	String tblName = "quotes";
	        	HBaseTbl quotesTbl = new HBaseTbl(tblName);
	        	quotesTbl.getTable(tblName);
		            
	        	for(String rf : rssFeeds) {
	        		String[] rssFeed = rf.split(delimiter);
	        	    
	        		// create and submit an executable thread
	        		Callable<Long> worker = new RSSConsumer(rssFeed[0], rssFeed[1]);
	        		Future<Long> submit = executor.submit(worker);
	        	    list.add(submit);
	        	    // get a corresponding company stock quote for each rss item
	        	    String[] lines = {"",""};
	        	    lines = rssFeed[0].split("-");
		        	String index = lines[0]; 
		        	if (index.equals("S&P500")){
		        		// only get stock quotes in the S&P 500
		        		String quoteName = lines[1];
		        		try {
		        			Quote q = new Quote();
		        			int res = q.get(quoteName);
		        			if (res == 0){
		        				q.insert(quotesTbl);
		        			}
		        		} catch (Exception e) {
		        			System.out.println((new Date()).toString() + " Error inserting quotes " + quoteName + " "  + e.getMessage());
		        			// only report, try again on next pass to see if error resolves on its own
		        		}
		        	}
	        	}
	            
	        	// get the corresponding S&P500 index and volatility quotes 
	        	try {
	        	   	Quote q1 = new Quote();
		        	int res = q1.get("SPY");
		        	if (res == 0){
		        	    q1.insert(quotesTbl);
		        	}
		        	Quote q2 = new Quote();
		        	res = q2.get("VXX");
		        	if (res == 0){
		        	    q2.insert(quotesTbl);
		        	}
	        	} catch (Exception e) {
	    			System.out.println((new Date()).toString() + " Error inserting Hbase quotes table"  + e.getMessage());
	    		    // only report, try again on next pass to see if error resolves on its own
	        	}
	        	
	        	// get the executor thread resulte of the RSS pulls 
	        	System.out.println((new Date()).toString() + " Gathering worker results");	
	        	long insertCnt = 0;
	            for (Future<Long> future : list) {
	                try {
	                  insertCnt += future.get();
	                } catch (InterruptedException e) {
	                  e.printStackTrace();
	                } catch (ExecutionException e) {
	                  e.printStackTrace();
	                }
	            }
	            //System.out.println((new Date()).toString() + " " + rssCnt + " rss pulls resulted in " + insertCnt + " newsItems inserts");
	            // This will make the executor accept no new threads and finish all existing threads in the queue
	            executor.shutdown();
	            // Wait for all threads are finished
	            executor.awaitTermination(1, TimeUnit.SECONDS);
	       	    
	            // email results, row counts of the 'newsItems' table
	            System.out.println((new Date()).toString() + " Emailing current newsItems counts");
	            String msgTxt = "";
	         	// only send one e-mail per hour
	            try{
	      	  		int rowCnt = 0;
	      	  		HBaseTbl tbl = new HBaseTbl("newsItems");
	      	  		tbl.getTable("newsItems");
	      	  		rowCnt = tbl.getRowCount();
	      	  		msgTxt = (new Date()).toString() + " " + rssCnt + " rss pulls resulted in " + insertCnt + " new rows inserted into Hbase table newsItems.  \n" +  
	      	  				 (new Date()).toString() + " " + "Current row count of HBase table newsItems is " + rowCnt;
	      	  	    Calendar now = Calendar.getInstance();
	      	  		hr = now.get(Calendar.MINUTE);
	                System.out.println("hr = " + hr + " prevhr =  " + prevHr);
	      	  		if(hr != prevHr){
	      	  			System.out.println((new Date()).toString() + " Emailing current newsItems counts");
		                Mail sm = new Mail(args[0], args[1], args[2]);
		                sm.send(msgTxt);
	      	  		}else{
	      	  			System.out.println((new Date()).toString() + " Skipping email");
	      	  		}
	                prevHr = hr;
	            } catch (IOException e) {
		           	e.printStackTrace();
		        }	            
	      	            
	            System.out.println(msgTxt);
	            System.out.println((new Date()).toString() + " Finished all threads, going to sleep for " + (sleepFor/minute) + " minutes");	
	   	        Thread.sleep(sleepFor);
	        	System.out.println((new Date()).toString() + " Done sleeping restarting rss pull cycle");	
        	}
        }catch (Exception e) {
        	System.out.println((new Date()).toString() + " Exception in main thread:" + e.getMessage()); 
        }
    }
}
