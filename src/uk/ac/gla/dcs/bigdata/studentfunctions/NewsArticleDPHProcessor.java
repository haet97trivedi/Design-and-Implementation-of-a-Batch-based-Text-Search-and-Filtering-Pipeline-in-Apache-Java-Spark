package uk.ac.gla.dcs.bigdata.studentfunctions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleDPHScore;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;

public class NewsArticleDPHProcessor implements FlatMapFunction<NewsArticleProcessed,NewsArticleDPHScore>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4688429466094545724L;
	
	//Broadcast<List<Query>> broadcastQueries;
	
	List<Query> queries;
	LongAccumulator totalCorpusTerms;
	LongAccumulator totalCorpusDocuments;
	
	public NewsArticleDPHProcessor(List<Query> queries, LongAccumulator totalCorpusTerms, LongAccumulator totalCorpusDocuments) {
		this.queries = queries;
		this.totalCorpusDocuments = totalCorpusDocuments;
		this.totalCorpusTerms = totalCorpusTerms;
	}
	
	@Override
	public Iterator<NewsArticleDPHScore> call(NewsArticleProcessed value) throws Exception {
		
		
		
		//System.out.println(queries.size());
		
		List<NewsArticleDPHScore> newsArticleDPHScoreList = new ArrayList<NewsArticleDPHScore>();	
		
		double avgDPHScore = 0;
		
		for (int i=0; i<queries.size(); i++) {
			int termFrequencyInCurrentDocument=0;
			 Query query = queries.get(i);
			// System.out.println("HAETTTTT" + query.getQueryTerms());
			 
			
			List<String> contents = value.getContents();
			String id = value.getId();
			List<String> title = value.getTitle();
			int currentDocumentLength = contents.size() + title.size();

//			for (String queryTerm: query.getQueryTerms()) {
//				if(contents.contains(queryTerm)) {
//					System.out.println("Query Matched! - "+queryTerm + ", " + contents);
//					
//					termFrequencyInCurrentDocument++; // Change this logic
//				
//				}
				
//				double dphscore = DPHScorer.getDPHScore(
//						termFrequencyInCurrentDocument,
//						totalTermFrequencyInCorpus, // WILL THIS BE TERM WISE OR QUERY WISE AND HOW?
//						currentDocumentLength,
//						averageDocumentLengthInCorpus,
//						totalCorpusDocuments);
			}
			

			
		 //   NewsArticleDPHScore newsDPHScores = new NewsArticleDPHScore(query, id, title, contents, dphscore);
			
		//	newsArticleDPHScoreList.add(newsDPHScores);

		//}
		
		
		//for(Query q:queries)
		//{
		//	System.out.println(q.getQueryTerms());
		//}
		
		return newsArticleDPHScoreList.iterator();
		
	}

	
}
