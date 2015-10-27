package dataCollection;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.apache.log4j.PropertyConfigurator;

import java.util.concurrent.Callable;

public class RSSConsumer implements Callable<Long>{
  
	private static final Logger logger = Logger.getLogger(RSSConsumer.class);
	
	private static final String LOG_PROPERTIES_FILE = "resources/log4j.properties";
	
	private String rssFeed = "";
	private String source = "";
	
	RSSConsumer(String source, String newsSiteUrl){
		
		this.rssFeed = newsSiteUrl;
		this.source = source;
	}
	
	private void initializeLogger()
	{
		// currently setup to roll up to 10 5MB logs 
		Properties logProperties = new Properties();
	 	
	    try
	    {
	      // load our log4j properties / configuration file
	      File file = new File (LOG_PROPERTIES_FILE);
	      logProperties.load(new FileInputStream(file));
	      PropertyConfigurator.configure(logProperties);
	      logger.info("Logging initialized.");
	    }
	    catch(IOException e)
	    {
	      System.out.println((new Date()).toString() + " Unable to load logging property " + LOG_PROPERTIES_FILE + e.toString());
	    }
	}
		
	public Long call() {

	  this.initializeLogger();	
	
	  long insertTot = 0;
	  try {
		  HBaseTbl tbl = new HBaseTbl("newsItems");
		  try {
			  tbl.getTable("newsItems");
			  //System.out.println("Hbase table fetch successful");
			  //logger.info("Hbase table fetch successful");
		  } catch (IOException e) {
			  logger.error("Hbase table fetch failed: " + e.getMessage());
			  System.out.println((new Date()).toString() + " Hbase table fetch failed");
			  return insertTot;
		  }
		  
		  DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		  URL u = new URL(this.rssFeed);
		  logger.info("Pulling from rss feed " + this.rssFeed);
		  Document doc = null;
		  try{
			  doc = builder.parse(u.openStream());
		  }catch(SAXException e) {
			  // log and skip rss pull, this occurs when the stream is empty
			  logger.error("Parsing stream error opening RSS stream feed: " + this.rssFeed + " exception : " + e.getMessage());
			  return insertTot; 
		  }catch(Exception e) {
			  // all other exceptions log and skip rss pull
			  logger.error("Parsing stream error opening RSS stream feed: " + this.rssFeed + " exception : " + e.getMessage());
			  return insertTot;
		  }
		  NodeList nodes = doc.getElementsByTagName("item");
		  // iterate through the news items
		  for(int i=0;i<nodes.getLength();i++) {
		  	  Element element = (Element)nodes.item(i);
			  // assign a row key, source + the title of the story
			  String item = this.getElementValue(element,"title");
			  String key = source + "-" + item;
			  // test if news item already in Hbase
			  if (this.keyExistsInTbl(tbl.table, key)){
				  // row already in table, skip
				  //System.out.println("Duplicate found skipping");
				  continue;
			  }
              try{
				  // not in table add key
				  tbl.addKey(key);
				  // fill columns (column family, column name and column value), printing them out and then adding to put object
				  // title
				  tbl.addCol("cf", "title", item);
				  // date/time
				  item = (new Date()).toString();
				  tbl.addCol("cf", "dateTime", item);
	  			  // url
				  //item = this.getElementValue(element,"link");
				  //String newsItemStory = getNewsItemStory(item);
				  //System.out.println(newsItemStory);
				  tbl.addCol("cf", "link", item);
				  // published date
				  item = this.getElementValue(element,"pubDate");
				  tbl.addCol("cf", "pubDate", item);
				  // author
				  item = this.getElementValue(element,"dc:creator");
				  tbl.addCol("cf", "creator", item);
				  // comments
				  item = this.getElementValue(element,"wfw:comment");
				  tbl.addCol("cf", "comments", item);
				  // description
				  item = this.getElementValue(element,"description");
				  tbl.addCol("cf", "description", item);
				  // source
				  tbl.addCol("cf", "source", source);
				  // commit the row to HBase
				  tbl.putRow();
	       		  logger.info("newsItems entry inserted with key " + key);
	       		  insertTot++;
              } catch(IOException e) {
        		  logger.error(e.getMessage());
        		  System.out.println((new Date()).toString() + " IOException during HBase insert: " + e.getMessage());
              }
        }
	  } catch(Exception e) {
		  logger.error(e.getStackTrace());
		  System.out.println((new Date()).toString() + " RSSConsumer exception occurred: " + e.getMessage());
	  }
	  return insertTot;
	}
	
	private String getNewsItemStory(String url){
		
		String newsItemStory = "";
		
		InputStream is = null;
		DataInputStream dis;
		String s;
		URL u;
		System.out.println(" url of the newsItem is " + url + " story follows ");
		
		try{
	      u = new URL(url);
	      is = u.openStream();
	      dis = new DataInputStream(new BufferedInputStream(is));
	      while ((s = dis.readLine()) != null){
	        //System.out.println(s);
	        newsItemStory += s;
	      }
	    }catch (MalformedURLException e){
	      System.err.println("Malformed URL exception.  Skipping news story.");
	    }
	    catch (IOException e){
	      System.err.println("IO exception.  Skipping news story.");
	      
	    }
	    finally{
	      try{
	        is.close();
	      }
	      catch (IOException e){
	      }
	    }
	 		
        return newsItemStory;
	}
	  
	private boolean keyExistsInTbl(HTable table, String title) throws IOException{
		
		Get getObject = new Get(toBytes(title));             
        Result getResult = table.get(getObject);
        if (getResult.isEmpty())
			return false;
        else
        	return true;
	}
	private String getCharacterDataFromElement(Element e) {
		
		// try to cast to character
		try {
			Node child = e.getFirstChild();
			if(child instanceof CharacterData) {
			   CharacterData cd = (CharacterData) child; 
			   return cd.getData();
			}
		} catch(Exception ex) {
		  	}
		  return "";
	  }
  
	  protected float getFloat(String value) {
	    
		  // try cast to numeric
		  if(value != null && !value.equals("")) {
			  return Float.parseFloat(value);
		  }
		  return 0;
	  }
	  
	  protected String getElementValue(Element parent,String label) {
		  
		  return getCharacterDataFromElement((Element)parent.getElementsByTagName(label).item(0));
	  }

}
