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

import java.util.function.Function;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;


public class NewsPreprocessor implements MapFunction<NewsArticle,NewsArticleProcessed>{

	
		/**
	 * 
	 */
	    private static final long serialVersionUID = -6952801201940912354L;
	
		private transient TextPreProcessor processor;
		
		LongAccumulator totalDocumentLength;
		LongAccumulator totalCorpusDocuments;
        LongAccumulator rowCount;
       
		Broadcast<List<Query>> queries;
		Long rowCounts;
		List<String> queryTermList;
		//public static List<Tuple2<String, Integer>> queryTermFreqList;
		public static HashMap<String, Integer> queryTermMap = new HashMap<String, Integer>();
		public int termFreq=0;
		
		public NewsPreprocessor() {}
		
		public NewsPreprocessor(LongAccumulator totalDocumentLength,LongAccumulator totalCorpusDocuments, Broadcast<List<Query>> queries) {
			super();
			this.totalDocumentLength = totalDocumentLength;
			this.totalCorpusDocuments = totalCorpusDocuments;
           // this.rowCounts = rowCounts;
			this.queries = queries;
			this.queryTermList = convertSetToList();
			//this.rowCount = rowCount;
			//rowCount.add(1);
		}
		
		private List<String> convertSetToList()
		{
			Set<String> queryTermSet = new HashSet<>();
			for (Query query: this.queries.getValue()) {
				Set<String> querySet = new HashSet<>(query.getQueryTerms());
				queryTermSet.addAll(querySet);
				
			}
			List<String> queryTermList = new ArrayList<>(queryTermSet);
			//System.out.println(queryTermList);
			return queryTermList;
		}
		
		public HashMap<String, Integer> queryTermFreq(List<String> titleProcessed, List<String> contentProcessed) throws Exception {
			List<String> document = new ArrayList<>();
			document.addAll(titleProcessed);
			document.addAll(contentProcessed);
	        //System.out.println(document);

			//System.out.println("-----------------------------------------------------------------");			
			//System.out.println("Query Term List ---> " + queryTermList );			
			//System.out.println("document ---> " + document );
			
			for(String queryTerm: queryTermList) {
		        termFreq =  Collections.frequency(document, queryTerm);
				//System.out.println("Query -> " + queryTerm + " |  Freq -> " + termFreq);
				queryTermMap.put(queryTerm, queryTermMap.getOrDefault(queryTerm, 0) + termFreq);
			}
			
			//System.out.println("OUTPUT MAP -->"+ queryTermMap);
			
//			 queryTermFreqList = queryTermMap.entrySet()
//				    .stream()
//				    .map(entry -> new Tuple2<String, Integer>(entry.getKey(), entry.getValue()))
//				    .collect(Collectors.toList());
			
			return queryTermMap;
			
		}


		@Override
		public NewsArticleProcessed call(NewsArticle value) throws Exception {
		
			if (processor==null) processor = new TextPreProcessor();
	
			String uniqueId = null;
			List<String> titleProcessed = new ArrayList<String>();
			List<String> contentProcessed = new ArrayList<String>();
	
			uniqueId = (value.getId());
			titleProcessed = processor.process(value.getTitle());

			
			List<ContentItem> contents = value.getContents();
			List<String> document = new ArrayList<String>();
			
			int count = 0;
			for(ContentItem items: contents)
			{
				
				String subtype = items.getSubtype();
	     		if(subtype !=null && !subtype.isEmpty() && subtype.equals("paragraph") && count <5 )
				{
					contentProcessed.addAll(processor.process(items.getContent()));
					count = count +1;
				}
			}
			
			//System.out.println(rowCount +":"+ rowCounts);
//			if(rowCount.value()<= rowCounts)
//			{
			queryTermMap = queryTermFreq(titleProcessed, contentProcessed);
			
			//System.out.println("Query term map"+queryTermMap);
			//rowCount.add(1);
//			}
			//System.out.println("Query Term List ----> " + queryTermFreqList);
			
			NewsArticleProcessed newsArticle = new NewsArticleProcessed(uniqueId, titleProcessed, contentProcessed, queryTermMap);
			
			totalDocumentLength.add(titleProcessed.size() + contentProcessed.size());
			//totalCorpusDocuments.add(1);
			//System.out.println("Hello Haet");
			return newsArticle;
			

			}
				
			
		
		}
