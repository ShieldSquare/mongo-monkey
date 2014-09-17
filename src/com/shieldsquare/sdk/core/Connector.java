/**
Contributors: Nachi
*/
package com.shieldsquare.sdk.core;

import java.net.UnknownHostException;
import org.apache.log4j.Logger;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.shieldsquare.sdk.utils.ReadPropertiesFile;

public class Connector {

	/* Logger details */
	private final static Logger logger = Logger.getLogger(Connector.class.getName());
	
	/* Mongo details */
	private static final String mongoHost = ReadPropertiesFile.getProperty("mongo.host");
	private static final Integer mongoPort = Integer.parseInt(ReadPropertiesFile.getProperty("mongo.port"));
	
	/* DB details */
	private static final String dbName = ReadPropertiesFile.getProperty("db.name");
	private static String dbCollectionName = ReadPropertiesFile.getProperty("collection.name");
	
	/* Client variables */
	private static MongoClient mongoClient;
	private static DB db;
	private static DBCollection collection;
	
	private static boolean initiated = false;
	
	public static void initiateSession(){

		try {
			mongoClient = new MongoClient(mongoHost,mongoPort);
			db = mongoClient.getDB(dbName);
			collection = db.getCollection(dbCollectionName);
			initiated = true;
		} catch (UnknownHostException uhe) {
			logger.error("Error connecting to mongo!", uhe);
		}
	}
	
	public static DBCollection getCollection(){
		if(!initiated)
			initiateSession();
		return collection;
	}
	
}
