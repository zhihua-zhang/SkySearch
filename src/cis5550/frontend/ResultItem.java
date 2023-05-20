package cis5550.frontend;

public class ResultItem {
	private double pageRank = 1.0;
	private double score = 1.0;
    private int ranking = 10;
    private String url = "";
	private String hits = "";
	private String title = "";
	private String id = "";


    
    
    public ResultItem(String url, int ranking, double score, double pageRank, String hits, String title, String id) {
    	this.url = url;
		this.ranking = ranking;
		this.score = score;
    	this.pageRank = pageRank;
		this.hits = hits;
		this.title = title;
		this.id = id;
	}
}
