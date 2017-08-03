package com.thefloow.challenge;

import java.util.Properties;

import com.mongodb.client.MongoDatabase;

public class TextAnalyzer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub		
		java.util.concurrent.Semaphore sem = new java.util.concurrent.Semaphore(10, true);
		sem.acquireUninterruptibly();
		com.mongodb.MongoClient m = new com.mongodb.MongoClient("localhost", 21321);
		MongoDatabase db =  m.getDatabase("fake");		
	}

}
