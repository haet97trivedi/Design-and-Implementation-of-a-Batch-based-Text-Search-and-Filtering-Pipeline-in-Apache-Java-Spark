package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleDPHScore;

public class QueryKeyFunction implements MapFunction<NewsArticleDPHScore,Query>{

	@Override
	public Query call(NewsArticleDPHScore value) throws Exception {

		//return value.getQuery().getOriginalQuery().toString();
		return value.getQuery();

	}
	
	
	
	

}
