package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class NewsArticleDPHScore implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7311464349415469277L;
	
	Query query;
	String id;
	List<String> title;
	List<String> contents;
	double dphScore;
	
	public NewsArticleDPHScore() {}

	public NewsArticleDPHScore(Query query, String id, List<String> title, List<String> contents, double dphScore) {
		super();
		this.query = query;
		this.id = id;
		this.title = title;
		this.contents = contents;
		this.dphScore = dphScore;
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getTitle() {
		return title;
	}

	public void setTitle(List<String> title) {
		this.title = title;
	}

	public List<String> getContents() {
		return contents;
	}

	public void setContents(List<String> contents) {
		this.contents = contents;
	}

	public double getDphScore() {
		return dphScore;
	}

	public void setDphScore(double dphScore) {
		this.dphScore = dphScore;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	};
	

	
	

}
