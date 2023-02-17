package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import java.util.function.Function;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
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
		
		LongAccumulator totalCorpusDocuments;
		
		public NewsPreprocessor(LongAccumulator totalCorpusDocuments) {
			super();
			this.totalCorpusDocuments = totalCorpusDocuments;
			
			
		}



		@Override
		public NewsArticleProcessed call(NewsArticle value) throws Exception {
		
			if (processor==null) processor = new TextPreProcessor();
	
			String uniqueId = null;
			List<String> titleProcessed = new ArrayList<String>();
			List<String> contentProcessed = new ArrayList<String>();
//			
//			
				uniqueId = (value.getId());
			//	System.out.println(uniqueId);
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
			
		
			
			NewsArticleProcessed newsArticle = new NewsArticleProcessed(uniqueId, titleProcessed, contentProcessed );
			
			totalCorpusDocuments.add(1);
			
			return newsArticle;
			

			}
				
			
		
		}
