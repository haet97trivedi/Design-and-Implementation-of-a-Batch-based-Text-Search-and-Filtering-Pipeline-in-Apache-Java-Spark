package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;

public class QueryTermFreq implements FlatMapFunction<NewsArticleProcessed,Tuple2<String,Integer>>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4759363066377166085L;
	
	public static List<Tuple2<String, Integer>> queryTermFreqList;
	public static HashMap<String, Integer> queryTermMap = new HashMap<String, Integer>();
	public static int termFreq=0;
	
	List<Query> queries;
	List<String> queryTermList;
	
	public QueryTermFreq(List<Query> queries) {
		super();
		this.queries = queries;
		this.queryTermList = convertSetToList();
		
	}
	
	private List<String> convertSetToList()
	{
		Set<String> queryTermSet = new HashSet<>();
		for (Query query: this.queries) {
			Set<String> querySet = new HashSet<>(query.getQueryTerms());
			queryTermSet.addAll(querySet);
			
		}
		List<String> queryTermList = new ArrayList<>(queryTermSet);
		//System.out.println(queryTermList);
		return queryTermList;
	}


	@Override
	public Iterator<Tuple2<String, Integer>> call(NewsArticleProcessed article) throws Exception {
			
		List<String> document = new ArrayList<>();
		document.addAll(article.getTitle());
		document.addAll(article.getContents());

		System.out.println("-----------------------------------------------------------------");			
		System.out.println("Query Term List --- " + queryTermList );			
		System.out.println("document --- " + document );
		
		
		for(String queryTerm: queryTermList) {
			int termFreq =  Collections.frequency(document, queryTerm);
			System.out.println("Query -> " + queryTerm + " |  Freq -> " + termFreq);
			queryTermMap.put(queryTerm, queryTermMap.getOrDefault(queryTerm, 0) + termFreq);
		}
		
		System.out.println("OUTPUT MAP -->"+ queryTermMap);
		
		List<Tuple2<String, Integer>> queryTermFreqList = queryTermMap.entrySet()
			    .stream()
			    .map(entry -> new Tuple2<String, Integer>(entry.getKey(), entry.getValue()))
			    .collect(Collectors.toList());
		
		return queryTermFreqList.iterator();
	}

}
