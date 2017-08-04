package com.thefloow.challenge;

import java.util.Properties;
import java.util.TreeMap;

import com.mongodb.*;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

public class TextAnalyzer {    
	public static void main(String[] args) {
	    //Someone, for now, has magically told me how many servers there are and what is my position among
		//them
		final int NUM_WORKERS=10;
		final int MY_POSITION=5;
		// TODO Auto-generated method stub		
		java.util.concurrent.Semaphore sem = new java.util.concurrent.Semaphore(1, true);
		TreeMap<String, Integer> wordCounts = new TreeMap<String, Integer>();
		
		/*sem.acquireUninterruptibly();
		com.mongodb.MongoClient m = new com.mongodb.MongoClient("localhost", 27017);		
		MongoIterable<String> dblist=m.listDatabaseNames();
		MongoCursor<String> cur = dblist.iterator();
		MongoDatabase db = null;
		db=m.getDatabase("word_count");
		while (cur.hasNext()) {
			if (cur.next().equals("word_count")){
				db = m.getDatabase("word_count");
				break;
			}
		}
		if (db == null) {			
		}*/
	}

}
