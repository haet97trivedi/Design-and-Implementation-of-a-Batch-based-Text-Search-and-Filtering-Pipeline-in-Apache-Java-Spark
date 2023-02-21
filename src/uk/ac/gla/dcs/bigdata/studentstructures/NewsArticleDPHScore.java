package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class NewsArticleDPHScore implements Serializable, Comparable<NewsArticleDPHScore> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7311464349415469277L;
	
	Query query;
	String id;
	List<String> title;
	double dphScore;
	NewsArticle article;
	
	public NewsArticleDPHScore() {}

	public NewsArticleDPHScore(Query query, String id, List<String> title, double dphScore, NewsArticle article) {
		super();
		this.query = query;
		this.id = id;
		this.title = title;
		this.dphScore = dphScore;
		this.article = article;
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

	public double getDphScore() {
		return dphScore;
	}

	public void setDphScore(double dphScore) {
		this.dphScore = dphScore;
	}

	public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	@SuppressWarnings("deprecation")
	@Override
	public int compareTo(NewsArticleDPHScore o) {
		return new Double(dphScore).compareTo(o.dphScore);
	}
	

	
	

}
