/**
Contributors: Nachi
*/
package com.shieldsquare.sdk.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.shieldsquare.sdk.exceptions.MongoUtilException;

public class Aggregator {

	Logger logger = Logger.getLogger(Aggregator.class);
	
	/*
	 * Defaults
	 */
	private int resultLimit = 20;
	private int sortIndicator = -1;
	
	private AggregationOutput output = null;
	private String initMatchFieldName = null;
	private Object initMatchFieldValue = null;
	private String initGroupFieldName = null;
	private Object initGroupFieldValue = null;
	private DBCollection collection = null;
	private boolean queryEditLock = false;
	
	DBObject groupFields;
	DBObject matchFields;
	List<DBObject> pipeline = null;
	List<BasicDBObject> matchList = null;
	
	/*
	 * Constructors
	 */
	
	/**
	 * 
	 * @param matchFieldName
	 * @param matchFieldValue
	 * @param groupFieldName
	 * @param groupFieldValue
	 * @param sortIndicator
	 */
	public Aggregator(String matchFieldName, Object matchFieldValue, 
			String groupFieldName, Object groupFieldValue, int sortIndicator ) {
		
		this.initMatchFieldName=matchFieldName;
		this.initMatchFieldValue=matchFieldValue;
		
		this.initGroupFieldName = groupFieldName;
		this.initGroupFieldValue = groupFieldValue;
		
		this.sortIndicator = sortIndicator;
		
	}
	
	/**
	 * 
	 * @param matchFieldName
	 * @param matchFieldValue
	 * @param groupFieldName
	 * @param groupFieldValue
	 */
	public Aggregator(String matchFieldName, Object matchFieldValue, 
			String groupFieldName, Object groupFieldValue) {
		
		this.initMatchFieldName=matchFieldName;
		this.initMatchFieldValue=matchFieldValue;
		
		this.initGroupFieldName = groupFieldName;
		this.initGroupFieldValue = groupFieldValue;
		
	}
	
	/**
	 * Default result limit is 20
	 * @param aggregator object
	 */
	public Aggregator setResultLimit(int resultLimit){
		this.resultLimit = resultLimit;
		return this;
	}
	
	/**
	 * 
	 * @param collection
	 * @return aggregator object
	 */
	public Aggregator setCollection(DBCollection collection){
		this.collection=collection;
		return this;
	}
	
	/**
	 * 
	 * @param dbObject
	 * @param fieldName
	 * @param fieldValue
	 * @return aggregator object
	 * @throws MongoUtilException 
	 */
	public Aggregator addField(DBObject dbObject, String fieldName, Object fieldValue) throws MongoUtilException{
		if(queryEditLock)
			throw new MongoUtilException("Attempt to edit query when locked");
		dbObject.put(fieldName, fieldValue);
		return this;
	}
	
	/**
	 * 
	 * @param dbObject
	 * @return
	 */
	public Aggregator addAggregationOperation(DBObject dbObject){
		if(pipeline == null)
			pipeline = new ArrayList<DBObject>();
		pipeline.add(dbObject);
		return this;
	}
	
	/**
	 * 
	 * @param fieldName
	 * @param fieldValue
	 * @throws MongoUtilException 
	 */
	public Aggregator addMatchField(String fieldName, Object fieldValue) throws MongoUtilException{
		if(queryEditLock)
			throw new MongoUtilException("Attempt to edit query when locked");
		if(matchList == null)
			matchList = new ArrayList<BasicDBObject>();
		matchList.add(new BasicDBObject(fieldName, fieldValue));
		return this;
	}
	
	/**
	 * 
	 * @param fieldName
	 * @param fieldValue
	 * @throws MongoUtilException 
	 */
	public void addGroupField(String fieldName, Object fieldValue) throws MongoUtilException{
		if(queryEditLock)
			throw new MongoUtilException("Attempt to edit query when locked");
		if(groupFields == null)
			groupFields = new BasicDBObject(fieldName, fieldValue);
		else
			addField(groupFields, fieldName, fieldValue);
	}
	
	private void compileFields(){
		matchFields = new BasicDBObject("$and", matchList);
		queryEditLock = true;
	}
	
	private void buildQuery() throws MongoUtilException{
		
		addMatchField(initMatchFieldName, initMatchFieldValue);
		addGroupField(initGroupFieldName, initGroupFieldValue);
		addGroupField("count", new BasicDBObject("$sum",1));
		compileFields();
		
	}
	
	private void aggregate(){

		try {
			buildQuery();
		} catch (MongoUtilException e) {
			logger.error("Query build error!", e);
		}
		
		DBObject match = new BasicDBObject("$match", matchFields);
		
		DBObject group = new BasicDBObject("$group",groupFields);
		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", sortIndicator));
		DBObject limit = new BasicDBObject("$limit", resultLimit);
		addAggregationOperation(match);
		addAggregationOperation(group);
		if(sortIndicator != 0)
			addAggregationOperation(sort);
		addAggregationOperation(limit);
		
		output = collection.aggregate(pipeline);
		queryEditLock = false;
	}
	
	/**
	 * Returns the DBObjects of aggregation result as an iterator
	 * @return resultIterator
	 */
	public Iterator<DBObject> getIterator(DBCollection collection) throws MongoUtilException{
		this.collection = collection;
		if(collection == null)
			throw new MongoUtilException("Unable to get iterator. Invalid collection.");
		if(output == null)
			aggregate();
		return output.results().iterator();
	}

}
