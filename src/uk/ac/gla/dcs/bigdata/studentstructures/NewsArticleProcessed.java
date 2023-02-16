package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;


public class NewsArticleProcessed implements Serializable {
/**
	 * 
	 */
	private static final long serialVersionUID = -1361193371758844533L;

	    String id; // unique article identifier
		List<String> title; // article title
		List<String> contents; // the contents of the article body
		
		public NewsArticleProcessed() {}

		public NewsArticleProcessed(String id, List<String> title, List<String> contents) {
			super();
			this.id = id;
			this.title = title;
			this.contents = contents;
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

		public static long getSerialversionuid() {
			return serialVersionUID;
		}
		
			
		
		}
