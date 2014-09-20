/**
Contributors: Nachi
 */
package com.shieldsquare.sdk.core;

import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class Distinct {

	private int count;
	private List <Object> distinctValuesList = null;
	private DBObject query = null;
	private DBCollection collection;
	private String initField;

	public Distinct(DBCollection collection, String key) {
		this.collection = collection;
		this.initField = key;
	}

	public Distinct addCondition(String fieldName, Object fieldValue){
		if(query==null)
			query = new BasicDBObject(fieldName,fieldValue);
		else
			query.put(fieldName, fieldValue);
		return this;
	}

	public List <Object> getKeys(){
		if(distinctValuesList==null)
			distinct();
		return distinctValuesList;
	}

	public int getCount() {
		if(distinctValuesList ==null)
			getKeys();
		count = distinctValuesList.size();
		return count;
	}
	
	@SuppressWarnings("unchecked")
	private void distinct(){
		distinctValuesList = (query==null)? collection.distinct(initField):collection.distinct(initField, query);
	}

}
