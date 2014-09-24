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
	private int DEFAULT_RESULT_LIMIT = 20;
	private int DEFAULT_SORT_INDICATOR = -1;
	private String DEFAULT_MATCH_OPERATION = "and";

	private AggregationOutput output = null;
	private String initMatchFieldName = null;
	private Object initMatchFieldValue = null;
	private String initGroupFieldName = null;
	private Object initGroupFieldValue = null;
	private DBCollection collection = null;

	private int resultLimit;
	private String matchOperation;
	private int sortIndicator;

	private DBObject groupFields;
	private DBObject matchFields;
	private List<DBObject> pipeline = null;
	private List<BasicDBObject> matchList = null;

	private boolean queryCompiled = false;

	/*
	 * Constructors
	 */

	/**
	 * 
	 * @param collection
	 * @param matchFieldName
	 * @param matchFieldValue
	 * @param groupFieldName
	 * @param groupFieldValue
	 */
	public Aggregator(DBCollection collection, String matchFieldName, Object matchFieldValue, 
			String groupFieldName, Object groupFieldValue) {
		this.collection=collection;
		this.initMatchFieldName=matchFieldName;
		this.initMatchFieldValue=matchFieldValue;
		this.initGroupFieldName = groupFieldName;
		this.initGroupFieldValue = groupFieldValue;
		this.matchOperation = DEFAULT_MATCH_OPERATION;
		this.resultLimit = DEFAULT_RESULT_LIMIT;
		this.sortIndicator = DEFAULT_SORT_INDICATOR;
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
	 */
	public Aggregator addField(DBObject dbObject, String fieldName, Object fieldValue){
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
	 */
	public Aggregator addMatchField(String fieldName, Object fieldValue){
		if(matchList == null)
			matchList = new ArrayList<BasicDBObject>();
		matchList.add(new BasicDBObject(fieldName, fieldValue));
		return this;
	}

	/**
	 * 
	 * @param fieldName
	 * @param fieldValue
	 */
	public void addGroupField(String fieldName, Object fieldValue){
		if(fieldValue.getClass() != BasicDBObject.class && fieldValue.getClass() != DBObject.class)
			fieldValue = "$"+fieldValue.toString();
		if(groupFields == null)
			groupFields = new BasicDBObject(fieldName, fieldValue);
		else
			addField(groupFields, fieldName, fieldValue);
	}

	private void compileQuery(){
		if(!queryCompiled){
			matchFields = new BasicDBObject("$"+matchOperation, matchList);
			queryCompiled = true;
		}
	}

	private void buildQuery(){
		if(queryCompiled)
			flushQuery();
		addMatchField(initMatchFieldName, initMatchFieldValue);
		addGroupField(initGroupFieldName, initGroupFieldValue);
		addGroupField("count", new BasicDBObject("$sum",1));
		compileQuery();
		DBObject limit = new BasicDBObject("$limit", resultLimit);
		addAggregationOperation(new BasicDBObject("$match", matchFields));
		addAggregationOperation(new BasicDBObject("$group", groupFields));
		if(sortIndicator != 0)
			addAggregationOperation(new BasicDBObject("$sort", new BasicDBObject("count", sortIndicator)));
		addAggregationOperation(limit);
	}

	private void flushQuery(){
		matchFields = null;
		groupFields = null;
		pipeline = null;
		queryCompiled = false;
	}

	private void aggregate(){
		buildQuery();
		output = collection.aggregate(pipeline);
	}

	/**
	 * Returns the DBObjects of aggregation result as an iterator
	 * @return resultIterator
	 */
	public Iterator<DBObject> getIterator() throws MongoUtilException{
		if(collection == null)
			throw new MongoUtilException("Unable to get iterator. Invalid collection.");
		aggregate();
		return output.results().iterator();
	}

	/**
	 * @return the sortIndicator
	 */
	public int getSortIndicator() {
		return sortIndicator;
	}

	/**
	 * @param sortIndicator the sortIndicator to set
	 */
	public Aggregator setSortIndicator(int sortIndicator) {
		this.sortIndicator = sortIndicator;
		return this;
	}

	/**
	 * @param matchOperation the aggregationOperation to set
	 */
	public Aggregator setMatchOperation(String matchOperation){
		this.matchOperation = matchOperation;
		return this;
	}

}
