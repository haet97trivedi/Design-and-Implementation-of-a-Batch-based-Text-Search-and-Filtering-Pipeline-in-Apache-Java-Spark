package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleDPHScore;

public class QueryGroupsRanking implements FlatMapGroupsFunction<Query, NewsArticleDPHScore, DocumentRanking> {

	@Override
	public Iterator<DocumentRanking> call(Query key, Iterator<NewsArticleDPHScore> values) throws Exception {
		
		List<NewsArticleDPHScore> naDPHScoreList = new ArrayList<>();
		List<NewsArticleDPHScore> naDPHScoreListFiltered = new ArrayList<>();
		
		List<RankedResult> RankedResultsList = new LinkedList<RankedResult>();
		List<DocumentRanking> DocumentRankingList = new LinkedList<DocumentRanking>();

		while (values.hasNext()) {
		    NewsArticleDPHScore score = values.next();
		    naDPHScoreList.add(score);
		}
		
		Collections.sort(naDPHScoreList);
		Collections.reverse(naDPHScoreList);
		
//     	for(NewsArticleDPHScore na: naDPHScoreList) {
//     		System.out.println("Keyy" + key +" | Sorted -> "+na.getQuery().getOriginalQuery() + " | DPH -> "+na.getDphScore());
//     	}
     	
     	int articleCounter = 0;
// 		Iterator<NewsArticleDPHScore> iterator = naDPHScoreList.iterator();
// 		while(iterator.hasNext()) {
// 			List<String> curArticle = iterator.next().getTitle();
// 			
// 		}
		TextDistanceCalculator tc = new TextDistanceCalculator();

		NewsArticleDPHScore naFiltered = new NewsArticleDPHScore();

    	
		naDPHScoreListFiltered.add(naDPHScoreList.get(0));
		RankedResult rankedResult  = new RankedResult(naDPHScoreList.get(0).getId(), naDPHScoreList.get(0).getArticle(), naDPHScoreList.get(0).getDphScore());
    	RankedResultsList.add(rankedResult);
    	
//			System.out.println("LIST SIZE --> "+ naDPHScoreListFiltered.size());
			outerloop:
			for(int i = 0; i < naDPHScoreList.size() -1 ; i++) {
			    NewsArticleDPHScore current = naDPHScoreList.get(i);
			    
	            String currentString = String.join("",current.getTitle());
	            
			    for(int j = i+1; j < naDPHScoreList.size(); j++) {
			        	NewsArticleDPHScore next = naDPHScoreList.get(j);
			            String nextString = String.join("",next.getTitle());
			            
			            double similarityScore = tc.similarity(currentString, nextString);
			            		
			            if(similarityScore >= 0.5) {
				            System.out.println("Similarity Score between "+currentString+" and "+nextString+" is -> "+similarityScore);

			            	naDPHScoreListFiltered.add(next);
			        		RankedResult rankedResultNext  = new RankedResult(next.getId(), next.getArticle(), next.getDphScore());
			            	RankedResultsList.add(rankedResultNext);
			            }
			    		if(naDPHScoreListFiltered.size() > 9) {
			    			break outerloop;
			    		}

			    }
			}
		

     	for(NewsArticleDPHScore na: naDPHScoreListFiltered) {
     		System.out.println("Keyy TOP 10 -> " + key +" | Sorted -> "+na.getQuery().getOriginalQuery() + " | DPH -> "+na.getDphScore());
     	}
     	
		DocumentRanking documentRanks = new DocumentRanking(key, RankedResultsList);
		DocumentRankingList.add(documentRanks);
		
		//return naDPHScoreListFiltered.iterator();
		return DocumentRankingList.iterator();

	}

}
