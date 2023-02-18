package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import static org.apache.spark.sql.functions.*;

import scala.Tuple2;
import scala.collection.immutable.Seq;
import scala.jdk.Accumulator;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleDPHProcessor;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsPreprocessor;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryTermFreq;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleDPHScore;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
			
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		//System.out.println(queries.collectAsList());
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		Broadcast<List<Query>> query = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queries.collectAsList());

		
		Dataset<NewsArticle> filteredData = news.filter(col("id").isNotNull().and(col("title").isNotNull().and(col("contents.subtype").isNotNull().and(col("contents.content").isNotNull()))));
		
		LongAccumulator totalDocumentLength = spark.sparkContext().longAccumulator();
		LongAccumulator totalCorpusDocuments = spark.sparkContext().longAccumulator();
//		LongAccumulator rowCount = spark.sparkContext().longAccumulator();
        Long totalDocs = filteredData.count();
		//System.out.println(filteredData.count());
//		Long rowCounts = filteredData.count();
//        //Accumulator<Map<String, Integer>> mapAccumulator = spark.sparkContext().accumulator(new HashMap<>(), new MapAccumulator());
		
		NewsPreprocessor newsArticle = new NewsPreprocessor(totalDocumentLength, totalCorpusDocuments, query);
		Dataset<NewsArticleProcessed> newsArticleProcessed = filteredData.map(newsArticle, Encoders.bean(NewsArticleProcessed.class)); // this converts each row into a NewsArticle
		
		List<NewsArticleProcessed> list1 = newsArticleProcessed.collectAsList();
		
		//System.out.println("HAETBAKU" + list1.get(list1.size()-1).queryTermMap);
		
		HashMap<String, Integer> queryTermMapValue = list1.get(list1.size()-1).queryTermMap;
		//Dataset<NewsArticleProcessed> newsArticleFilterProcessed = newsArticleProcessed.filter(col("id").and(col("title").and(col("contents"))));
		
		//System.out.println("QYERYEYRYE"+queryTermMapValue);
		
		//;
//		for(NewsArticleProcessed na :  newsArticleProcessed.collectAsList()) {
//			System.out.println("News Article --> "+na.getQueryTermFreqList());
//
//		}
//		newsArticleProcessed.foreach(row -> {
//            System.out.println(row.getId() + "," + row.getQueryTermFreqList());
//        });
		//newsArticleProcessed.count();
		
		//Dataset<NewsArticleProcessed> NewsArticleProcessedList = (Dataset<NewsArticleProcessed>) newsArticleProcessed.collectAsList();		
//		// FlatMap Function
		//Encoder<Tuple2<String, Integer>> tupleEncoder = Encoders.tuple(Encoders.STRING(),Encoders.INT());
		//QueryTermFreq querytermfreq = new QueryTermFreq(queries.collectAsList());
		//Dataset<Tuple2<String, Integer>> queryTermFreqTuple = newsArticleProcessed.flatMap(querytermfreq, tupleEncoder); 
		//queryTermFreqTuple.collectAsList();
		
		
		NewsArticleDPHProcessor newsDPHScore = new NewsArticleDPHProcessor(query, totalDocumentLength, totalDocs, queryTermMapValue);
		Dataset<NewsArticleDPHScore> newsArticleDPH = newsArticleProcessed.flatMap(newsDPHScore, Encoders.bean(NewsArticleDPHScore.class)); 
		newsArticleDPH.collectAsList();
		//System.out.println("HAETBAKU" + newsArticleDPH.collectAsList());		
		//List<NewsArticleDPHScore> dph = newsArticleDPH.collectAsList();
		
//		Collections.sort(newsArticleDPHList);
//		Collections.reverse(newsArticleDPHList);
		
//		for(int ArticleDPHIndex=0; ArticleDPHIndex<10; ArticleDPHIndex++) {
//			newsArticleDPHList.get(ArticleDPHIndex)
//		}
		
		return null; // replace this with the the list of DocumentRanking output by your topology
	}
	
	
}
