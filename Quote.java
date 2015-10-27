package dataCollection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Date;

public class Quote {

	private Date dt;
	private String symbol = "";
	private String price = "0.0";
	private String volume = "0";
	
	Quote(){
		
	}
	
	Quote(String symbol, String price, String volume){
		
		this.dt =  new Date();
		this.symbol = symbol;
		this.price = price;
		this.volume = volume;
	}
	
	public int get(String symbol){
	
	  	try{
	 	    URL stockURL = new URL("http://finance.yahoo.com/d/quotes.csv?s=" + symbol + "&f=nabv");
    	    BufferedReader in = new BufferedReader(new InputStreamReader(stockURL.openStream()));
    	    String[] lines = in.readLine().split(",");
    	    this.dt = new Date();
    	    this.symbol = symbol;
    	    this.price = lines[1];
    	    this.volume = lines[3];
       	}catch(Exception e){
    		System.out.println((new Date()).toString() + " Error fetcing stock quote" + e.getMessage());
			return -1;
    	}
    	
    	return 0;
    }
	
    public int insert(HBaseTbl tbl){

    	try{
	        // assign a row key, source + the title of the story
			String key = this.dt.toString() + "-" + this.symbol;
	    	tbl.addKey(key);
			// date/time
			tbl.addCol("cf", "dt", this.dt.toString());
	  		// symbol
			tbl.addCol("cf", "symbol", this.symbol);
			// price
			tbl.addCol("cf", "price", this.price);
			// volume
			tbl.addCol("cf", "volume", this.volume);
			// commit the row to HBase
			tbl.putRow();
       		System.out.println((new Date()).toString() + " quote inserted with key " + key);
	    } catch(IOException e) {
            System.out.println((new Date()).toString() + " IOException during HBase insert: " + e.getMessage());
			return -1;
	    } catch(Exception e) {
		    System.out.println((new Date()).toString() + " IOexception occurred: " + e.getMessage());
			return -1;
	    }
	    
		return 0;
    } 	    		 
      
    public static void main(String[] args) throws IOException{

    	// get hbase table
    	String tblName = "quotes";
    	HBaseTbl tbl = new HBaseTbl(tblName);
    	try {
    	    tbl.getTable(tblName);
		} catch (IOException e) {
			System.out.println((new Date()).toString() + " Hbase table fetch failed"  + e.getMessage());
			System.exit(-1);
		}
    	// get the quotes
    	Quote q1 = new Quote();
    	int res = q1.get("SPY"); 
    	if (res == 0){
    	    q1.insert(tbl);
    	}
    	Quote q2 = new Quote();
    	res = q2.get("VXX");
    	if (res == 0){
    	    q2.insert(tbl);
    	}
    	
    }
}
