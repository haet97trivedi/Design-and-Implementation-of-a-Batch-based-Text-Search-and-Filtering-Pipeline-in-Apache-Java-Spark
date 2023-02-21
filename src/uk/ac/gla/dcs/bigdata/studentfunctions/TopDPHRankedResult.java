package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleDPHScore;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;

public class TopDPHRankedResult implements FlatMapFunction<NewsArticleDPHScore,DocumentRanking>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5948545102821189687L;
	List<RankedResult> RankedResultsList = new LinkedList<RankedResult>();
	List<DocumentRanking> DocumentRankingList = new LinkedList<DocumentRanking>();
	
	public TopDPHRankedResult() {}
		
	@Override
	public Iterator<DocumentRanking> call(NewsArticleDPHScore value) throws Exception {
	
//		for(NewsArticle newsArticle:newsArticle)
//		{
//			
//		  NewsArticle article = new NewsArticle();
//		  if(newsArticle.getId().equals(value.getId()))
//		  {
//			 id = value.getId();
//			 article.setId(id);
//			 article.setArticle_url(newsArticle.getArticle_url());
//			 article.setTitle(newsArticle.getTitle());
//			 article.setAuthor(newsArticle.getAuthor());
//			 article.setPublished_date(newsArticle.getPublished_date());
//			 article.setContents(newsArticle.getContents());
//			 article.setType(newsArticle.getType());
//			 article.setSource(newsArticle.getSource());
//             break;
//		  }
//		}

		RankedResult rankedResult  = new RankedResult(value.getId(), value.getArticle(), value.getDphScore());
		
		//System.out.println("RANKED RESULT - > "+rankedResult.getDocid() + " | Score -> " + rankedResult.getScore());

		
		if(RankedResultsList.size() <=9) {
			RankedResultsList.add(rankedResult);
		}
		else {
			RankedResultsList.add(rankedResult);

			DocumentRanking documentRanks = new DocumentRanking(value.getQuery(), RankedResultsList);
			DocumentRankingList.add(documentRanks);
			RankedResultsList.clear();
		}

		//System.out.println("Document Ranking List - > "+ DocumentRankingList);
		return DocumentRankingList.iterator();
	}

	
}
