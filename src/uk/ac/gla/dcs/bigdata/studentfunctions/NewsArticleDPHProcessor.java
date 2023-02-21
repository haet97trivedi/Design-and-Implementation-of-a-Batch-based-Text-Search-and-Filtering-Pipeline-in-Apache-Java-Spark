package uk.ac.gla.dcs.bigdata.studentfunctions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleDPHScore;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsPreprocessor;

public class NewsArticleDPHProcessor implements FlatMapFunction<NewsArticleProcessed,NewsArticleDPHScore>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4688429466094545724L;
	Integer count = 0;
	//Broadcast<List<Query>> broadcastQueries;
	List<Integer> docsize = new ArrayList<>();
	Broadcast<List<Query>> queries;
	LongAccumulator totalDocumentLength;
	Long totalCorpusDocuments;
	double averageDocumentLengthInCorpus;
	HashMap<String, Integer> queryTermMapValue;
	
	List<RankedResult> RankedResultsList = new LinkedList<RankedResult>();
	List<DocumentRanking> DocumentRankingList = new LinkedList<DocumentRanking>();

	public NewsArticleDPHProcessor() {}
	
	public NewsArticleDPHProcessor(Broadcast<List<Query>> queries, LongAccumulator totalDocumentLength, Long totalCorpusDocuments, HashMap<String, Integer> queryTermMapValue) {
		this.queries = queries;
		this.totalCorpusDocuments = totalCorpusDocuments;
		this.totalDocumentLength = totalDocumentLength;
		
		this.averageDocumentLengthInCorpus = totalDocumentLength.value()*1.0/totalCorpusDocuments;
		
		this.queryTermMapValue = queryTermMapValue;
	}
	
	@Override
	public Iterator<NewsArticleDPHScore> call(NewsArticleProcessed value) throws Exception {
		
			
		//List<Tuple2<String,Integer>> totalQueryTermFrequency = value.getQueryTermFreqList();
		//HashMap<String,Integer> totalQueryTermFrequency = value.getQueryTermMap();
		HashMap<String,Integer> totalQueryTermFrequency = queryTermMapValue;

		List<NewsArticleDPHScore> newsArticleDPHScoreList = new ArrayList<NewsArticleDPHScore>();				

		//System.out.println("---------------------------------------------------------------------");
		//System.out.println("All Query Terms Frequency in Corpus  --> " + totalQueryTermFrequency );
	
		
		for (int i=0; i<queries.getValue().size(); i++) {
			Query query = queries.getValue().get(i);			 
			
			String id = value.getId();
			List<String> title = value.getTitle();
			List<String> contents = value.getContents();
			int currentDocumentLength = contents.size() + title.size();
			
			List<String> document = new ArrayList<>();
			document.addAll(title);
			document.addAll(contents);
			
			
			
			
			Double avgDPHScore = 0.0;
			List<Double> termDPHScoreList = new ArrayList<Double>();
			//System.out.println("Document -> "+document);

			for (String queryTerm: query.getQueryTerms()) {
				
				short termFreqinDocument = (short) Collections.frequency(document, queryTerm);
			
//				int totalTermFrequencyInCorpus = totalQueryTermFrequency.stream()
//										                .filter(tuple -> tuple._1().equals(queryTerm))
//										                .map(tuple -> tuple._2())
//										                .findFirst()
//										                .orElse(null);
				
				int totalTermFrequencyInCorpus = totalQueryTermFrequency.getOrDefault(queryTerm, null);
				
				//System.out.println("Query Term is -> "+ queryTerm+" | Frequency in Document -> "+termFreqinDocument+" | Frequency in Corpus -- " + totalTermFrequencyInCorpus );
              //  System.out.println("Query Term is -> "+ queryTerm+"Currentdoc: "+ termFreqinDocument + "Totalterm:" + totalTermFrequencyInCorpus+ "CurrentDocLen:"+ currentDocumentLength+"Avgdoclen:"+averageDocumentLengthInCorpus+"totaldoc:"+totalCorpusDocuments);
				double termdphscore = DPHScorer.getDPHScore(
						termFreqinDocument,
						totalTermFrequencyInCorpus,
						currentDocumentLength,
						averageDocumentLengthInCorpus,
						totalCorpusDocuments);
				
				if(Double.isNaN(termdphscore))
				{
					termdphscore = 0.0;
					termDPHScoreList.add(termdphscore);
				}
				else
				{
					termDPHScoreList.add(termdphscore);
				}
				//System.out.println("Query Term -> "+queryTerm+" | DPH -> "+termdphscore);
				
			}
			
			avgDPHScore = termDPHScoreList.stream().mapToDouble(d -> d).average().orElse(0.0);
			
			//System.out.println("Query Term -> "+query.getOriginalQuery()+" | Average DPH -> "+avgDPHScore);
            if(avgDPHScore > 0)
            {
			NewsArticleDPHScore newsDPHScores = new NewsArticleDPHScore(query, id, title, avgDPHScore, value.getArticle());
            //System.out.println("Query: " + query.getOriginalQuery() + "avgDPHScore: "+ avgDPHScore);
			newsArticleDPHScoreList.add(newsDPHScores);
			
			RankedResult rankedResult  = new RankedResult(value.getId(), value.getArticle(), avgDPHScore);

            }

		}
		//System.out.println("Hello Taritro");
		return newsArticleDPHScoreList.iterator();


		}		
		
	}
