package com.thefloow.challenge;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
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
		final String filePath = "..\\..\\Downloads\\enwiki-20170701-pages-articles-multistream.xml\\enwiki-20170701-pages-articles-multistream.xml";
		final long maxAllowedSubChunk = Math.round(Runtime.getRuntime().maxMemory()*0.75); 
	
		// TODO Auto-generated method stub		
		java.util.concurrent.Semaphore sem = new java.util.concurrent.Semaphore(1, true);
		TreeMap<String, Integer> wordCounts = new TreeMap<String, Integer>();
		System.out.println(System.getProperty("user.dir"));
		java.io.RandomAccessFile f=null;
		try {
			f = new java.io.RandomAccessFile(filePath,"r");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long chunkSize=maxAllowedSubChunk;
		try {
			chunkSize = f.length()/NUM_WORKERS;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long subChunkSize=chunkSize;
		int current_subchunk=0;
		if (chunkSize > maxAllowedSubChunk ) {
			subChunkSize = maxAllowedSubChunk;
		}		
		//Now i get the n-th chunk of data or the first subChunk of my share of data
		java.nio.channels.FileChannel fc = f.getChannel();
		MappedByteBuffer mbb=null;
		try {
			 mbb = fc.map(FileChannel.MapMode.READ_ONLY, MY_POSITION*chunkSize, subChunkSize);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//If i have memory-mapped my subchunk (i'm using memory-mapping for performance reasons) i can start spawning threads to do the real work
		if (mbb != null) {
			
		}		
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
