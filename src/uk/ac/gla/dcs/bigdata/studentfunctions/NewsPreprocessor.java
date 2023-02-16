package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import com.fasterxml.jackson.databind.ObjectMapper;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;


public class NewsPreprocessor implements MapFunction<NewsArticle,NewsArticleProcessed>{

	
		/**
	 * 
	 */
	    private static final long serialVersionUID = -6952801201940912354L;
	
		private transient TextPreProcessor processor;
		
		@Override
		public NewsArticleProcessed call(NewsArticle value) throws Exception {
		
			if (processor==null) processor = new TextPreProcessor();
	
			String uniqueId = null;
			List<String> titleProcessed = new ArrayList<String>();
			List<String> contentProcessed = new ArrayList<String>();
			
			 
			if(value.getId() !=null && !value.getId().isEmpty() && value.getTitle() !=null && !value.getTitle().isEmpty()) {
			
				uniqueId = (value.getId());
				System.out.println(uniqueId);
				titleProcessed = processor.process(value.getTitle());
				
				
				System.out.println("Title " + titleProcessed);
				
			
			
			System.out.println("-----------------------------------------------------");
			
			
			List<ContentItem> contents = value.getContents();
			
			int count = 0;
			for(ContentItem items: contents)
			{
				
				String subtype = items.getSubtype();
	     		if(subtype !=null && !subtype.isEmpty() && subtype.equals("paragraph") && count <5 )
				{
					contentProcessed.addAll(processor.process(items.getContent()));
					
					
					count = count +1;
					//System.out.println(subtype);
					

			
				}
	     		
			}
			}
			
			System.out.println("Content" + contentProcessed);
			
			NewsArticleProcessed newsArticle = new NewsArticleProcessed(uniqueId, titleProcessed, contentProcessed );
			
			
		return newsArticle;
		
		}
}
