package edu.bu.cs755;

import static java.util.Comparator.reverseOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.PropertyConfigurator;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;



public class Task2 {
	
	private Stream<String> streamlines;
	private List<String> Fivethousandwords;
	S3ObjectInputStream s3is;
	S3Object s3object;
	BufferedReader reader;
	
	public Task2 (List<String> top5000List) throws IOException{
		
		// custom input
		/*
		Fivethousandwords = new ArrayList<String>();
		Fivethousandwords.add("the");
		Fivethousandwords.add("is");
		Fivethousandwords.add("refer");

		String testInput = "<doc id=\"432019\" url=\"https://en.wikipedia.org/wiki?curid=432019\" title=\"Fresno scraper\">Fresno scraperThe Fresno Scraper is a machine pulled by horses used for constructing canals and ditches in sandy soil. design of the Fresno Scraper forms the basis of most modern earthmoving scrapers, having the ability to scrape and move a quantity of soil, and also to discharge it at a controlled depth, thus quadrupling the volume which could be handled manually.The Fresno scraper was invented in 1883 by James Porteous. Working with farmers in Fresno, California, he had recognised the dependence of the Central San Joaquin Valley on irrigation, and the need for a more efficient means of constructing canals and ditches in the sandy soil. In perfecting the design of his machine, Porteous made several revisions on his own and also traded ideas with William Deidrick, Frank Dusy, and Abijah McCall, who invented and held patents on similar scrapers. Porteous bought the patents held by Deidrick, Dusy, and McCall, gaining sole rights to the Fresno Scraper.Prior scrapers pushed the soil ahead of them, while the Fresno scraper lifted it into a C-shaped bowl where it could be dragged along with much less friction. By lifting the handle, the operator could cause the scraper to bite deeper. Once soil was gathered, the handle could be lowered to raise the blade off the ground so it could be dragged to a low spot, and dumped by raising the handle very high.This design was so revolutionary and economical that it has influenced the design of modern bulldozer blades and earth movers to this day.Between 1884 and 1910 thousands of Fresno scrapers were produced at the Fresno Agricultural Works which had been formed by Porteous, and used in agriculture and land levelling, as well as road and railroad grading and the construction industry. They played a vital role in the construction of the Panama Canal and later served the US Army in World War I.It was one of the most important agricultural and civil engineering machines ever made. In 1991 the Fresno Scraper was designated as an International Historic Engineering Landmark by the American Society of Mechanical Engineers. It is currently on display at the San Joaquin County Historical Society & Museum.</doc>\n"
				+ "<doc id=\"432067\" url=\"https://en.wikipedia.org/wiki?curid=432067\" title=\"Richard Rich (disambiguation)\">Richard Rich (disambiguation)Richard Rich, 1st Baron Rich (1496/7¡°1567) was Lord Chancellor of England.Richard Rich may also refer to:</doc>\n"
				+ "<doc id=\"432020\" url=\"https://en.wikipedia.org/wiki?curid=432019\" title=\"Fresno scraper\">Fresno scraperThe Fresno Scraper is a machine pulled by horses used for constructing canals and ditches in sandy soil. The design of the Fresno Scraper forms the basis of most modern earthmoving scrapers, having the ability to scrape and move a quantity of soil, and also to discharge it at a controlled depth, thus quadrupling the volume which could be handled manually.The Fresno scraper was invented in 1883 by James Porteous. Working with farmers in Fresno, California, he had recognised the dependence of the Central San Joaquin Valley on irrigation, and the need for a more efficient means of constructing canals and ditches in the sandy soil. In perfecting the design of his machine, Porteous made several revisions on his own and also traded ideas with William Deidrick, Frank Dusy, and Abijah McCall, who invented and held patents on similar scrapers. Porteous bought the patents held by Deidrick, Dusy, and McCall, gaining sole rights to the Fresno Scraper.Prior scrapers pushed the soil ahead of them, while the Fresno scraper lifted it into a C-shaped bowl where it could be dragged along with much less friction. By lifting the handle, the operator could cause the scraper to bite deeper. Once soil was gathered, the handle could be lowered to raise the blade off the ground so it could be dragged to a low spot, and dumped by raising the handle very high.This design was so revolutionary and economical that it has influenced the design of modern bulldozer blades and earth movers to this day.Between 1884 and 1910 thousands of Fresno scrapers were produced at the Fresno Agricultural Works which had been formed by Porteous, and used in agriculture and land levelling, as well as road and railroad grading and the construction industry. They played a vital role in the construction of the Panama Canal and later served the US Army in World War I.It was one of the most important agricultural and civil engineering machines ever made. In 1991 the Fresno Scraper was designated as an International Historic Engineering Landmark by the American Society of Mechanical Engineers. It is currently on display at the San Joaquin County Historical Society & Museum.</doc>\n";		
	    
		Streamlines = Arrays.asList(testInput.split(">\n")).stream();
		 */

		// get data from S3.
		
		PropertyConfigurator.configure("log4j.properties");
		String bucket_name= "metcs755";
		String key_name="WikipediaPages_oneDocPerLine_1000Lines_small.txt";
		String big_key_name="WikipediaPages_oneDocPerLine_1m.txt"; 
		
		AmazonS3 s3Client = AmazonS3Client.builder().withRegion("us-east-1").build();	// this takes some time.
		
		Fivethousandwords = top5000List;

		s3object = s3Client.getObject(bucket_name, key_name);	// this takes some time.
		s3is = s3object.getObjectContent();
		reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
		streamlines = reader.lines().parallel();
		
		/**
	     * Task 2
	     */
		
	    System.out.println("\n**** TASK 2 ****");
	}

	private String getTheKey(String e) {

		return e.substring(9, 15);
	}

	private Integer getTheValue(String v) {
		
	    List<String> words = Arrays.asList(v.split(">")[1].split(" "));
	    
	    // get only words from the 5000 list.
	    Predicate<String> predicate =
	    		e -> (Fivethousandwords.contains(e));
	    
		Integer resultvalue = 
	            words.stream()
	            	 .map(line -> line.replaceAll("<[^>]+>", ""))
	            	 .flatMap(line -> Arrays.stream(line.trim().split(" ")))	
	                 .map(String::toLowerCase)
	                 .filter(word -> word.length() > 0)
	                 .distinct()
	                 .filter(predicate)
	                 //.collect(groupingBy(identity(), counting()))
	                 .collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
	                 .entrySet().stream()
	                 .limit(5000)
	                 .map(e ->e.getValue())
	                 .reduce(0, (x,y) -> x + y);
	                 //.collect(toList());
		
	    return resultvalue.intValue();
	}
	
	public void top20ranklist() throws IOException{

		Map<String, Integer> donyoo = streamlines
				.collect(Collectors.toMap(e ->getTheKey(e), v -> getTheValue(v), (oldValue, newValue) -> oldValue, LinkedHashMap::new) )
				.entrySet().stream()
				.sorted(Map.Entry.<String, Integer> comparingByValue(reverseOrder()))
				.limit(20)		// limit top 20
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		System.out.println("Top20 List: " + donyoo);

		s3is.close();
	    streamlines.close();
		s3object.close();
	    reader.close();
	}
}
