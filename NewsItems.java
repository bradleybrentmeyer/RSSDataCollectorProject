package dataCollection;

public class NewsItems {

	  private String key = "";
	  private String source = "";
	  private String title = "";;
    private String url = "";
    private String pubDate = "";
    private String creator = "";
    private String comments = "";
    private String description = "";
    
    NewsItems(){
    	
    }
    
    NewsItems(String key, String source, String title, String url, String pubDate, String creator, String comments, String description){
    	
    	this.key = key;
    	this.source = source;
    	this.title = title;
    	this.url = url;
    	this.pubDate = pubDate;
    	this.creator = creator;
    	this.comments = comments;
    	this.description = description;
    }
}
