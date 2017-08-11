package com.thefloow.challenge;

import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;




public class TextAnalyzer {
	static java.util.concurrent.locks.ReentrantReadWriteLock mutex;        
	static TreeMap<String, Integer> wordCounts;
	static MappedByteBuffer mbb;
	static int perThreadChunkSize;
	static short count=0;
	public static CharBuffer cb;	
        static final java.util.concurrent.locks.Lock w = new ReentrantLock(true);
        static java.util.logging.Logger l;
	
	public static void main(String[] args) throws InterruptedException, IOException {
            java.util.logging.FileHandler f2 = new java.util.logging.FileHandler(".\\logfile.log");
            l = java.util.logging.Logger.getGlobal();
            l.setLevel(Level.ALL);
            l.addHandler(f2);            
	    //Someone, for now, has magically told me how many servers there are and what is my position among
		//them
		final int NUM_WORKERS=1;
		final int MY_POSITION=1;
		//final String filePath = "C:\\Users\\Paolo\\nuovo_test.xml";
                String filePath=null;
                for (int a=0; a < args.length; a++) {
                 if (args[a].equals("-dump")) {
                     File f = new File(args[a+1]);
                     if (f.exists()) {
                        filePath = f.getAbsolutePath();
                        l.info(filePath);
                     }
                 }
                }
		final long maxAllowedSubChunk = Math.round(Runtime.getRuntime().maxMemory()*0.75); 
		mutex = new java.util.concurrent.locks.ReentrantReadWriteLock(true);                
		wordCounts = new TreeMap<String, Integer>();		
		java.io.RandomAccessFile f=null;				
		try {
			f = new java.io.RandomAccessFile(filePath,"r");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
                        l.info(e.getMessage());
			e.printStackTrace();
		}		
		long chunkSize=maxAllowedSubChunk;
		try {
			chunkSize = f.length()/NUM_WORKERS;
		} catch (IOException e) {
			// TODO Auto-generated catch block
                        l.info(e.getMessage());
			e.printStackTrace();
		}
                l.info("La dimensione massima e' "+chunkSize+" bytes e la dimensione del file e' "+f.length()+" bytes");
		long subChunkSize=chunkSize;
		subChunkSize = Math.min(chunkSize, maxAllowedSubChunk);
		//This second comparison is done because while memory-mapping the chunk we cannot exceed 
		//Integer.MAX_VALUE bytes
		subChunkSize = Math.min(subChunkSize, Integer.MAX_VALUE);                
		//Now i get the n-th chunk of data or the first subChunk of my share of data
		java.nio.channels.FileChannel fc = f.getChannel();				
		//If i have memory-mapped my subchunk (i'm using memory-mapping for performance reasons) i can start spawning threads to do the real work
		int counter=0;	
		mbb=fc.map(FileChannel.MapMode.READ_ONLY, (MY_POSITION-1)*chunkSize+counter*subChunkSize, subChunkSize);
		perThreadChunkSize=(int)subChunkSize/Runtime.getRuntime().availableProcessors();
		java.util.LinkedList<Thread> threads = new java.util.LinkedList<Thread>();
                while (mbb != null) {
			try {
				 if (counter*subChunkSize < chunkSize) {
                                         l.info("parto dalla posizione "+(MY_POSITION-1)*chunkSize+counter*subChunkSize+" ed alloco "+subChunkSize+" bytes");
					 mbb = fc.map(FileChannel.MapMode.READ_ONLY,(MY_POSITION-1)*chunkSize+counter*subChunkSize , subChunkSize);
				 }
				 else{
					break; 
				 }				 
				 Charset c = Charset.forName("UTF-8");				 
				 CharsetDecoder cd = c.newDecoder();
				 cb = CharBuffer.allocate(mbb.limit()/2);				 
				 cd.decode(mbb,cb,true);                                  
                                 mbb=null;
                                 System.gc();
					
			} catch (IOException e) {
				// TODO Auto-generated catch block
                                StackTraceElement [] stack = e.getStackTrace();
                                for (int hh=0; hh < stack.length; hh++) {
                                    l.info("Errore alla riga : "+stack[hh].getLineNumber());
                                }
                                cb = CharBuffer.allocate(mbb.limit()/2);				 
				mbb=null;
                                System.exit(-30);
                                
			}
			count = 0;
			cb.rewind();                        
			w.lock();
                        long sPos = cb.position();                                
			while (cb.position() < (subChunkSize/2) && cb.remaining() >= perThreadChunkSize/2) {
                                sPos= cb.position(); 
                                l.info("Thread "+count);
				w.unlock();
				//This is done to allow the thread to have time to read its own share of data
				//sem.acquireUninterruptibly();                               
				TextAnalyzer.WorkerThread wt = new TextAnalyzer.WorkerThread();
                                Thread t = new Thread(wt);
                                t.setName("Thread "+count);                                
				t.start();				
                                threads.add(t);
                                //Synchro issues that the lock doesn't seem able to solve                                
                                w.lock();
                                count++;
				//w.lock();                                
//                                l.info("Starting the busy waiting");
//                                while ( cb.position() < ((perThreadChunkSize/2)+sPos) && cb.remaining() > 0){
//                                    l.info("The cb position is "+cb.position()+" and the limit is "+ ((perThreadChunkSize/2)+sPos));
//                                    l.info("The char buffer has "+cb.remaining()+" characters left to read");
//                                    System.out.flush();
//                                    Thread.currentThread().sleep(4000);
//                                }
//                                l.info("The thread should have copied its data");
//                                System.out.flush();
			}
                   cb=null;     
                   w.unlock();                   
		   System.gc();
                   for (int k=0; k < threads.size() ; k++) {                       
                       threads.get(k).join();                       
                   }
		   //Waiting for all of the threads to complete their work before going on with mine		   
		   counter++;                   
                   //w.unlock();
		}
                java.util.Set<String> keys=wordCounts.keySet();
                String [] keys_array = keys.toArray(new String[0]);                
                f.close();
                fc.close();                
		com.mongodb.MongoClient m = new com.mongodb.MongoClient("alamo",27017);				
		MongoDatabase db = m.getDatabase("word_count");		
		com.mongodb.client.MongoCollection<org.bson.Document> coll = db.getCollection("counters");                		
                Set<String> words=wordCounts.keySet();
                String [] words_array=words.toArray(new String [0]);
                for (int a=0; a < words_array.length; a++) {                
                    org.bson.Document doc = /*new org.bson.Document(supportMap);*/ new org.bson.Document();
                    doc.append(words_array[a],wordCounts.get(words_array[a]));
                    coll.insertOne(doc);
                }
		m.close();				
	}
        static class WorkerThread implements Runnable {              
            public void run() {                                
                    int id=-25;
                    char[] myCopy=null;                    
                    w.lock();
                try {
                    id=count;
                    int arrSiz = Math.min(perThreadChunkSize/2,(TextAnalyzer.cb != null)?TextAnalyzer.cb.remaining():0);
                    if (arrSiz == 0) {                        
                        return;
                    }
                    myCopy = new char[arrSiz];                    
                    for (int i=0; i < arrSiz; i++){
                        myCopy[i]=TextAnalyzer.cb.get();
                    }
                } 
                catch (Exception e) {
                     l.info(e.getMessage());
                     e.printStackTrace();
                }
                finally {
                    w.unlock();
                }
                    String pvtStr=new String(myCopy);
                    myCopy=null;
                    //System.gc();
                    String [] splitString = pvtStr.split("\\P{Alpha}");
                    //StringTokenizer tok=new StringTokenizer(pvtStr, " ?.,:;");                                        
                    int j=0;
                    while (j < splitString.length){
                        //l.info("Processing the "+j+"th string by the "+id+"th thread");                        
                        w.lock();
                        try {
                            //l.info("This is thread nr. "+id+" processing word "+j+" out of "+splitString.length);
                            if (wordCounts.containsKey(splitString[j].trim().toLowerCase())){
                                int count = wordCounts.get(splitString[j].trim().toLowerCase());
                                count++;
                                wordCounts.replace(splitString[j].trim().toLowerCase(), count);
                            }
                            else {
                                wordCounts.put(splitString[j].trim().toLowerCase(), 1);
                            }
                        } 
                        catch (Exception e) {
                         l.info(e.getMessage());
                         e.printStackTrace();
                        }
                        finally {
                            w.unlock();
                        }                        
                        j++;
                    }
                }            
            }
        }

