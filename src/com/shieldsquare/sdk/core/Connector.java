/**
Contributors: Nachi
 */
package com.shieldsquare.sdk.core;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

public class Connector {

	/* Logger details */
	private final static Logger logger = Logger.getLogger(Connector.class.getName());

	/* Mongo details */

	private List<ServerAddress> serverSeeds = new ArrayList<ServerAddress>();

	/* DB details */
	private String dbName;
	private String dbCollectionName;

	/* Client variables */
	private MongoClient mongoClient;
	private DB db;
	private DBCollection collection = null;


	private boolean initiated = false;
	private boolean slaveOk = false;
	/**
	 * 
	 * @param mongoHost
	 * @param mongoPort
	 * @param dbName
	 * @param dbCollectionName
	 */
	public Connector(String mongoHost, Integer mongoPort, String dbName, String dbCollectionName){
		this.setDbName(dbName);
		this.setDbCollectionName(dbCollectionName);
		addHost(mongoHost, mongoPort);
	}

	public void initiateSession(){
		if(initiated)
			closeSession();
		mongoClient = new MongoClient(serverSeeds);
		if(isSlaveOk())
			mongoClient.setReadPreference(ReadPreference.secondaryPreferred());
		db = mongoClient.getDB(getDbName());
		collection = db.getCollection(getDbCollectionName());
		initiated = true;
	}
	
	public void closeSession(){
		mongoClient.close();
		mongoClient = null;
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

	public boolean isSlaveOk() {
		return slaveOk;
	}

	public Connector setSlaveOk(boolean slaveOk) {
		this.slaveOk = slaveOk;
		return this;
	}

	public Connector addHost(String mongoHost, int mongoPort){
		try {
			ServerAddress newSeed = new ServerAddress(mongoHost, mongoPort);
			serverSeeds.add(newSeed);
		} catch (UnknownHostException e) {
			logger.error("Error connecting to mongo!" + e);
		}
		return this;
	}

}
