package com.thefloow.challenge;

import com.mongodb.MongoClientURI;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;




public class TextAnalyzer {
    static java.util.concurrent.locks.ReentrantReadWriteLock mutex;        
    static TreeMap<String, Integer> wordCounts;
    static MappedByteBuffer mbb;
    static int perThreadChunkSize;
    static short count=0;
    public static CharBuffer cb;	
    static final java.util.concurrent.locks.Lock w = new ReentrantLock(true);
    static java.util.logging.Logger l;
    static String [] words = null;
    static int wordsPerThread=0;
    static int wordsPos;
    
    public static void main(String[] args) throws InterruptedException, IOException {                        
	java.util.logging.FileHandler f2 = new java.util.logging.FileHandler("."+File.separator+"logfile.log");
	l = java.util.logging.Logger.getGlobal();
	l.setLevel(Level.ALL);
	l.addHandler(f2);                  
	l.warning(System.getProperty("java.class.path"));
	System.setProperty("java.class.path", System.getProperty("java.class.path")+";"+"mongo-java-driver-3.4.3.jar");
	l.warning(System.getProperty("java.class.path"));
	//I initialize the variables to a canary value, further on i will read the correct values from the MongoDB 
	//"Config" collection from the "ChallengeConfig" database            		
	int NUM_WORKERS=1;
	int MY_POSITION=1;		
	String filePath=null;
	String host="localhost";
	String myId=java.net.Inet4Address.getLocalHost().getHostAddress();
	int port=27017;
	for (int a=0; a < args.length; ) {
	    if (args[a].equals("-source")) {
		File f = new File(args[a+1]);
		if (f.exists()) {
		    filePath = f.getAbsolutePath();
		    l.info(filePath);
		}
		a+=2;
	    }
	    if (args[a].equals("-mongo")) {
		String [] params=new String[0];                 
		if (a+1 < args.length) {
		    params=args[a+1].split(":");                      
		}                     
		if (args[a+1].contains(":")) {
		    a+=2;
		}
		else {
		    a++;
		}
		if (params.length != 0 && !params[0].isEmpty()) {
		    host=params[0];
		}
		if (params.length == 2 && !params[1].isEmpty()) {
		    port = new Integer(params[1]);
		}
	    }
	    if (args[a].equals("-id")) {
                   myId = args[a+1];
                   a+=2;
	    }
	}
	//Pre-Processing                
	com.mongodb.MongoClient startupC = new com.mongodb.MongoClient(host,port);                                                                
	MongoDatabase confDB = startupC.getDatabase("ChallengeConfig");		
	com.mongodb.client.MongoCollection<org.bson.Document> configColl = confDB.getCollection("Config");                		
	org.bson.Document conf=configColl.find(Filters.exists("maxInstances")).first();                
	NUM_WORKERS=conf.getInteger("maxInstances");
	MY_POSITION=(int)configColl.count();
	l.info("I am in "+(MY_POSITION)+"th position");                
	org.bson.Document myData = new org.bson.Document();
	myData.append("worker_id", myId);
	configColl.insertOne(myData);
	startupC.close();
	//End of Pre-Processing
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
	long subChunkSize=chunkSize;
	subChunkSize = Math.min(chunkSize, maxAllowedSubChunk);
	//This second comparison is done because while memory-mapping the chunk we cannot exceed 
	//Integer.MAX_VALUE bytes
	subChunkSize = Math.min(subChunkSize, Integer.MAX_VALUE);                
	//Now i get the n-th chunk of data or the first subChunk of my share of data
	java.nio.channels.FileChannel fc = f.getChannel();				
	//If i have memory-mapped my subchunk (i'm using memory-mapping for performance reasons) i can start spawning threads to do the real work
	int counter=0;
	long startPosMapping=(MY_POSITION-1)*chunkSize+counter*subChunkSize;
	if (startPosMapping%2 != 0) {
	    //Given this is little-endian if i am on the low byte of the char i must move to the high byte to read correctly
	    f.seek(startPosMapping-1);
	}
	while (startPosMapping != 0 && Character.isLetterOrDigit(f.readChar())){
	    //moving to the previous char
	    f.seek(f.getFilePointer()-4);
	}
	startPosMapping=f.getFilePointer();	
	if (startPosMapping != 0) {            
	    if ((startPosMapping+chunkSize)%2 != 0) {
		//Given this is little-endian if i am on the low byte of the char i must move to the high byte to read correctly
		f.seek(startPosMapping+chunkSize-1);
	    }            
	    while (Character.isLetterOrDigit(f.readChar())){
		//moving to the previous char
		f.seek(f.getFilePointer()-4);
	    }
	    chunkSize=f.getFilePointer()-startPosMapping;
	    subChunkSize = Math.min(chunkSize, maxAllowedSubChunk);
	    //This second comparison is done because while memory-mapping the chunk we cannot exceed 
	    //Integer.MAX_VALUE bytes
	    subChunkSize = Math.min(subChunkSize, Integer.MAX_VALUE);
	}
	f.seek(0);
	mbb=fc.map(FileChannel.MapMode.READ_ONLY, startPosMapping, subChunkSize);                                
	perThreadChunkSize=(int)subChunkSize/Runtime.getRuntime().availableProcessors();                
	java.util.LinkedList<Thread> threads = new java.util.LinkedList<Thread>();
	l.info("About to start analyzing");
	while (mbb != null) {
	    l.info("Processing a sub-chunk");
	    try {
		startPosMapping=(MY_POSITION-1)*chunkSize+counter*subChunkSize;                               
                f.seek(startPosMapping);
		if (startPosMapping%2 != 0) {
		    //Given this is little-endian if i am on the low byte of the char i must move to the high byte to read correctly
		    f.seek(startPosMapping-1);
		}
                FileInputStream fis = new FileInputStream(filePath);                
                InputStreamReader isr = new InputStreamReader(fis, "UTF-16");
                BufferedReader br = new BufferedReader(isr);
                long charCount = 0;
                br.mark(Integer.MAX_VALUE);
                l.log(Level.INFO, "I have read {0} characters", br.read());
                while (br.read() != -1) {
                    charCount++;
                }
                l.log(Level.INFO, "I found {0} characters", charCount);
                br.reset();                
                br.skip(startPosMapping);
                char [] tmp = new char[1];
                br.read(tmp);                
		while (startPosMapping != 0 && Character.isLetterOrDigit(tmp[0])){
		    //moving to the previous char                    		    
                    short countTmp = 4;
                    while (countTmp > 0) {
                        br.reset();
                        try {                            
                            br.skip(startPosMapping-countTmp);
                            br.read(tmp);
                            startPosMapping -= countTmp;
                            break;
                        }
                        catch (IOException e) {
                            countTmp--;
                        }
                    }                    
		}                
                l.log(Level.INFO, "The starting position is {0}", startPosMapping);
		startPosMapping=f.getFilePointer();	
                f.seek(startPosMapping+chunkSize);
		if (startPosMapping != 0) {
		    if ((startPosMapping+chunkSize)%2 != 0) {
		//Given this is little-endian if i am on the low byte of the char i must move to the high byte to read correctly
			f.seek(startPosMapping+chunkSize-1);
		    }
		    while (Character.isLetterOrDigit(f.readChar())){
			//moving to the previous char
			f.seek(f.getFilePointer()-4);
		    }
		    chunkSize=f.getFilePointer()-startPosMapping;
		    subChunkSize = Math.min(chunkSize, maxAllowedSubChunk);
		    //This second comparison is done because while memory-mapping the chunk we cannot exceed 
		    //Integer.MAX_VALUE bytes
		    subChunkSize = Math.min(subChunkSize, Integer.MAX_VALUE);
		}
		l.log(Level.INFO, "I am starting to map at position {0}", startPosMapping);
		if (counter*subChunkSize < chunkSize) {                                         
		    mbb = fc.map(FileChannel.MapMode.READ_ONLY,startPosMapping, subChunkSize);
		}
		else{
		    l.info("I have exceeded my chunk size");
		    break; 
		}				 
		Charset c = Charset.forName("UTF-16");				 
		CharsetDecoder cd = c.newDecoder();
		cb = CharBuffer.allocate(mbb.limit()/2);				 
		cd.decode(mbb,cb,true);                                  
		mbb=null;
		System.gc();
		
	    } catch (IOException e) {
		// TODO Auto-generated catch block                      
		cb = CharBuffer.allocate(mbb.limit()/2);				 
		mbb=null;
		System.exit(-30);                
	    }                        
	    count = 0;
	    cb.rewind();
	    l.log(Level.INFO, "The character buffer has {0} characters in it and there are {1} characters left", new Object[]{cb.limit(), cb.remaining()});
	    try {															
		words=cb.toString().split("\\P{Alpha}");
	    }
	    catch (Exception e) {
		l.info(e.getMessage());
	    }
	    l.log(Level.INFO, "Begin : {0} End ", cb.toString());
	    wordsPos=0;                        
	    wordsPerThread=words.length/Runtime.getRuntime().availableProcessors();
	    w.lock();
	    l.log(Level.INFO, "I have {0} words to analyze", words.length);
	    while (wordsPos < words.length) {                                				
		//This is done to allow the thread to have time to read its own share of data
		//sem.acquireUninterruptibly();                               
		TextAnalyzer.WorkerThread wt = new TextAnalyzer.WorkerThread();
		Thread t = new Thread(wt);
		t.setName("Thread "+count);                                                                
		w.unlock();
		t.start();
		w.lock();
		threads.add(t);
		//Synchro issues that the lock doesn't seem able to solve                                                                
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
	    w.unlock();
	    cb=null;                        
	    System.gc();
	    for (int k=0; k < threads.size() ; k++) {                       
		threads.get(k).join();                       
	    }
	    //Waiting for all of the threads to complete their work before going on with mine		   
	    counter++;                   
	    //w.unlock();
	}
	l.info("Ended processing");
	java.util.Set<String> keys=wordCounts.keySet();
	String [] keys_array = keys.toArray(new String[0]);                
	f.close();
	fc.close();
	com.mongodb.MongoClientOptions.Builder b = new com.mongodb.MongoClientOptions.Builder();
	//b=b.readConcern(ReadConcern.LOCAL);
	b=b.writeConcern(new WriteConcern(1));                
	com.mongodb.MongoClient m = new com.mongodb.MongoClient(host+":"+port,b.build());				                
	MongoDatabase db = m.getDatabase("word_count");		
	com.mongodb.client.MongoCollection<org.bson.Document> coll = db.getCollection("counters");                		
	Set<String> words=wordCounts.keySet();
	String [] words_array=words.toArray(new String [0]);                
	BsonDocument filter; 
	com.mongodb.client.model.UpdateOptions opt = new com.mongodb.client.model.UpdateOptions();
	opt=opt.upsert(true);                
	for (int a=0; a < words_array.length; a++) {
	    l.log(Level.INFO, "Processing the {0}th word out of {1}", new Object[]{a, words_array.length});
	    FindIterable<org.bson.Document> fi = coll.find(Filters.eq("word", words_array[a]));
	    org.bson.Document doc = null;
	    try {
		doc = fi.first();                      
	    } catch (Exception e) {
		l.info(e.getMessage());
		l.log(Level.INFO, "The offending word was {0}", words_array[a]);
		continue;
	    }
	    if (doc == null) {                             
		doc = new org.bson.Document();
		doc.append("word",words_array[a]);
		doc.append("count",wordCounts.get(words_array[a]));
		coll.insertOne(doc);
	    }
	    else {           
		coll.updateOne(Filters.eq("word", words_array[a]), Updates.inc("count", wordCounts.get(words_array[a])));
	    }                                        
	}
	MongoDatabase status = m.getDatabase("ChallengeConfig");
	com.mongodb.client.MongoCollection<org.bson.Document> statusColl = status.getCollection("Config");                                                
	if (statusColl.count(Filters.exists("done"))  != NUM_WORKERS-1) {
	    org.bson.Document myDoc = statusColl.find(Filters.eq("worker_id", myId)).first();
	    myDoc.append("done", true);
	    statusColl.replaceOne(Filters.eq("worker_id",myId), myDoc);
	}
	else {                                                                                
	    l.info("Starting to show the results ...");
	    FindIterable<org.bson.Document> list=coll.find();
	    com.mongodb.client.MongoCursor<org.bson.Document> it = list.iterator();
	    while (it.hasNext()) {
		org.bson.Document d = (org.bson.Document)it.next();
		l.log(Level.INFO, "The word {0} has appeared {1} times", new Object[]{d.getString("word"), d.getInteger("count")});                        
	    }
	    statusColl.deleteMany(Filters.exists("worker_id"));
	}
	m.close();				
    }
    static class WorkerThread implements Runnable {              
	public void run() {                                
	    int sPos,fPos;
	    w.lock();
	    sPos=wordsPos;
	    fPos=sPos+Math.min(words.length-sPos,wordsPerThread);
	    wordsPos=fPos+1;                    
	    w.unlock();
	    //System.gc();                    
	    //StringTokenizer tok=new StringTokenizer(pvtStr, " ?.,:;");                                        
	    int j=sPos;
	    while (j < fPos){                        
		//l.info("Processing the "+j+"th string by the "+id+"th thread");                        
		w.lock();
		try {
		    //l.info("This is thread nr. "+id+" processing word "+j+" out of "+splitString.length);
		    if (words[j].trim().length() > 0) {
			if (wordCounts.containsKey(words[j].trim().toLowerCase())){
			    int count = wordCounts.get(words[j].trim().toLowerCase());
			    count++;
			    wordCounts.replace(words[j].trim().toLowerCase(), count);
			}
			else {
			    wordCounts.put(words[j].trim().toLowerCase(), 1);
			}                                
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

