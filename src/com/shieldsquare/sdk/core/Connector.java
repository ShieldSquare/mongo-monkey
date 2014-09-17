/**
Contributors: Nachi
*/
package com.shieldsquare.sdk.core;

import java.net.UnknownHostException;
import org.apache.log4j.Logger;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class Connector {

	/* Logger details */
	private final static Logger logger = Logger.getLogger(Connector.class.getName());
	
	/* Mongo details */
	private String mongoHost;
	private Integer mongoPort;
	
	/* DB details */
	private String dbName;
	private String dbCollectionName;
	
	/* Client variables */
	private MongoClient mongoClient;
	private DB db;
	private DBCollection collection = null;
	
	private static boolean initiated = false;
	
	/**
	 * 
	 * @param mongoHost
	 * @param mongoPort
	 * @param dbName
	 * @param dbCollectionName
	 */
	public Connector(String mongoHost, Integer mongoPort, String dbName, String dbCollectionName){
		this.mongoHost = mongoHost;
		this.mongoPort = mongoPort;
		this.setDbName(dbName);
		this.setDbCollectionName(dbCollectionName);
	}
	
	public void initiateSession(){

		try {
			mongoClient = new MongoClient(mongoHost,mongoPort);
			db = mongoClient.getDB(getDbName());
			collection = db.getCollection(getDbCollectionName());
			initiated = true;
		} catch (UnknownHostException uhe) {
			logger.error("Error connecting to mongo!", uhe);
		}
	}
	
	/**
	 * 
	 * @return mongo collection
	 */
	public DBCollection getCollection(){
		if(!initiated)
			initiateSession();
		return collection;
	}

	public String getDbCollectionName() {
		return dbCollectionName;
	}

	public void setDbCollectionName(String dbCollectionName) {
		this.dbCollectionName = dbCollectionName;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	
}
