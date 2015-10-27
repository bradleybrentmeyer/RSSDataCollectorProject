//package dataCollection;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;

public class HBaseTbl {
  
	String tblName = "";
	Configuration config = null;
	HTable table = null;
	Put p = null;
	
	public HBaseTbl(String tblName) {
		
		 this.config = HBaseConfiguration.create();
		 this.tblName = tblName;
	}
  
	public HTable getTable(String atable) throws IOException{
		
		  this.table = new HTable(this.config, atable);
		  return this.table; 
	}
	
	public void addKey(String akey) throws IOException{
		
		  this.p = new Put(Bytes.toBytes(akey));
	}
	
	public void addCol(String acolFam, String acol, String avalue) throws IOException{
		
		  this.p.add(Bytes.toBytes(acolFam), Bytes.toBytes(acol),Bytes.toBytes(avalue));
	}
	
	public void addNumCol(String acolFam, String acol, Double avalue) throws IOException{
		
		  this.p.add(Bytes.toBytes(acolFam), Bytes.toBytes(acol),Bytes.toBytes(avalue));
	}
	
	public void putRow() throws IOException{
		
		  this.table.put(p);
			
	}
	
	public ResultScanner scan() throws IOException{

		Scan scan = new Scan();
		ResultScanner result = this.table.getScanner(scan);
		return result;
	}
	
	public int getRowCount()throws IOException{
		
		int i = 0;
		ResultScanner rs = this.scan(); 
		for (Result r: rs){
			i++;	
		}
		return i;	
	}
		
	public ArrayList<String> decodeScanner(ResultScanner scanner, String colFam, String col){
		
		ArrayList<String> results = new ArrayList<String>();
		int i = 0;
		for (Result result: scanner) {
            // decode bytes to sent
			results.add(Bytes.toString(result.getValue(toBytes(colFam), toBytes(col))));
			i++; 
		}
		return results;
	}
}
