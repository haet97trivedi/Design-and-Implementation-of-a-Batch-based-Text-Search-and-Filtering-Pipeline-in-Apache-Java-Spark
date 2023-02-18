package uk.ac.gla.dcs.bigdata.studentstructures;

import scala.Tuple2;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;



public class NewsArticleProcessed implements Serializable {
/**
	 * 
	 */
	private static final long serialVersionUID = -1361193371758844533L;

	    String id; // unique article identifier
		List<String> title; // article title
		List<String> contents; // the contents of the article body
		//List<Tuple2<String, Integer>> queryTermFreqList;
		public static HashMap<String, Integer> queryTermMap = new HashMap<>();

		
		public NewsArticleProcessed() {}

		public NewsArticleProcessed(String id, List<String> title, List<String> contents, HashMap<String, Integer> queryTermMap) {
			super();
			this.id = id;
			this.title = title;
			this.contents = contents;
			this.queryTermMap = queryTermMap;
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

		public static HashMap<String, Integer> getQueryTermMap() {
			return queryTermMap;
		}

		public static void setQueryTermMap(HashMap<String, Integer> queryTermMap) {
			NewsArticleProcessed.queryTermMap = queryTermMap;
		}

		public static long getSerialversionuid() {
			return serialVersionUID;
		}

		
		
		}
