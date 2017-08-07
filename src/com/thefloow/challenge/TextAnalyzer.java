package com.thefloow.challenge;

import com.mongodb.client.MongoDatabase;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.StringTokenizer;
import java.util.TreeMap;




public class TextAnalyzer {
	static java.util.concurrent.locks.ReadWriteLock mutex;	
	static TreeMap<String, Integer> wordCounts;
	static MappedByteBuffer mbb;
	static int perThreadChunkSize;
	static short count;
	static CharBuffer cb;
	static java.util.concurrent.locks.Lock r,w;
	
	public static void main(String[] args) throws InterruptedException, IOException {
	    //Someone, for now, has magically told me how many servers there are and what is my position among
		//them
		final int NUM_WORKERS=10;
		final int MY_POSITION=5;
		final String filePath = "C:\\Users\\Public\\enwiki-20170701-pages-articles-multistream.xml\\enwiki-20170701-pages-articles-multistream.xml";
		final long maxAllowedSubChunk = Math.round(Runtime.getRuntime().maxMemory()*0.75); 
		mutex = new java.util.concurrent.locks.ReentrantReadWriteLock(true);
		r=mutex.readLock();
		w=mutex.writeLock();				
		wordCounts = new TreeMap<String, Integer>();		
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
		subChunkSize = Math.min(chunkSize, maxAllowedSubChunk);
		//This second comparison is done because while memory-mapping the chunk we cannot exceed 
		//Integer.MAX_VALUE bytes
		subChunkSize = Math.min(subChunkSize, Integer.MAX_VALUE);
		//Now i get the n-th chunk of data or the first subChunk of my share of data
		java.nio.channels.FileChannel fc = f.getChannel();				
		//If i have memory-mapped my subchunk (i'm using memory-mapping for performance reasons) i can start spawning threads to do the real work
		int counter=0;	
		mbb=fc.map(FileChannel.MapMode.READ_ONLY, MY_POSITION*chunkSize+counter*subChunkSize, subChunkSize);
		perThreadChunkSize=(int)subChunkSize/Runtime.getRuntime().availableProcessors();
		while (mbb != null) {
			try {
				 if (counter*subChunkSize < chunkSize) {
					 mbb = fc.map(FileChannel.MapMode.READ_ONLY, MY_POSITION*chunkSize+counter*subChunkSize, subChunkSize);
				 }
				 else{
					break; 
				 }				 
				 Charset c = Charset.forName("UTF-8");				 
				 CharsetDecoder cd = c.newDecoder();
				 cb = CharBuffer.allocate(mbb.limit()/2);				 
				 cd.decode(mbb,cb,false);						
					
			} catch (IOException e) {
				// TODO Auto-generated catch block
				mbb=null;
			}
			count = 0;
			cb.rewind();
			r.lock();
			while (cb.position() < (subChunkSize/2)) {
				r.unlock();
				System.out.println(count);
				//This is done to allow the thread to have time to read its own share of data
				//sem.acquireUninterruptibly();				
				WorkerThread wt = new WorkerThread(); 
				Thread t = new Thread(wt);
                                t.setName("Thread "+count);
				t.start();
				count++;
				r.lock();
			}					   
		   System.gc();
		   //Waiting for all of the threads to complete their work before going on with mine
		   Thread.currentThread().yield();
		   counter++;
		}		
		w.lock();
		com.mongodb.MongoClient m = new com.mongodb.MongoClient("localhost", 27017);				
		MongoDatabase db = m.getDatabase("word_count");		
		com.mongodb.client.MongoCollection<org.bson.Document> coll = db.getCollection("counters");
		coll.drop();
		java.util.HashMap<String,Object> supportMap = new java.util.HashMap<String,Object>(wordCounts);		
		org.bson.Document doc = new org.bson.Document(supportMap);
		coll.insertOne(doc);
		m.close();
		w.unlock();
		supportMap=null;
	}
        static class WorkerThread implements Runnable {
            public void run() {
                int id=-25;
                r.lock();
                id=count;
                int arrSiz = Math.min(perThreadChunkSize/2,cb.remaining());
                if (arrSiz == 0) {
                    cb.position(cb.position()+1);
                }
                char[] myCopy = new char[arrSiz];
                System.out.println("For thread nr. "+id+" the data size is "+arrSiz);
                for (int i=0; i < arrSiz; i++){
                    myCopy[i]=cb.get();
                }
                r.unlock();
                String pvtStr=new String(myCopy);
                myCopy=null;
                //System.gc();
                String [] splitString = pvtStr.split("\\P{Alpha}");
                StringTokenizer tok=new StringTokenizer(pvtStr, " ?.,:;");
                int j=0;
                while (j < splitString.length){
                    w.lock();
                    System.out.println("This is thread nr. "+id+" processing word "+j+" out of "+splitString.length);
                    if (wordCounts.containsKey(splitString[j].trim().toLowerCase())){
                        int count = wordCounts.get(splitString[j].trim().toLowerCase());
                        count++;
                        wordCounts.replace(splitString[j].trim().toLowerCase(), count);
                    }
                    else {
                        wordCounts.put(splitString[j].trim().toLowerCase(), 1);
                    }
                    w.unlock();
                    j++;
                }
            }
        }

}
