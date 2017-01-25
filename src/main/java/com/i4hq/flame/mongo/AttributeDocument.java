/**
 * 
 */
package com.i4hq.flame.mongo;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.Encoder;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;

/**
 * This class is a decorator for Documents. 
 * It adds an additional field to indicate the type of attribute document it is.
 * @author rmoten
 *
 */
public class AttributeDocument extends Document {

	/**
		 * @author rmoten
		 *
		 */
	public enum AttributeDocumentType {
		REFERENCE,
		GEO,
		DEFAULT;
	}


	/**
	 * 
	 */
	private static final long serialVersionUID = -8380732190012596022L;
	private final Document decoratedDoc;
	private final AttributeDocumentType docType;

	
	public AttributeDocument(Document doc, AttributeDocumentType docType) {
		super();
		this.decoratedDoc = doc;
		this.docType = docType;
	}
	
	


	/* (non-Javadoc)
	 * @see java.util.Map#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		// TODO Auto-generated method stub

	}


	public <C> BsonDocument toBsonDocument(Class<C> documentClass, CodecRegistry codecRegistry) {
		return decoratedDoc.toBsonDocument(documentClass, codecRegistry);
	}


	public Document append(String key, Object value) {
		return decoratedDoc.append(key, value);
	}


	public <T> T get(Object key, Class<T> clazz) {
		return decoratedDoc.get(key, clazz);
	}


	public Integer getInteger(Object key) {
		return decoratedDoc.getInteger(key);
	}


	public int getInteger(Object key, int defaultValue) {
		return decoratedDoc.getInteger(key, defaultValue);
	}


	public Long getLong(Object key) {
		return decoratedDoc.getLong(key);
	}


	public Double getDouble(Object key) {
		return decoratedDoc.getDouble(key);
	}


	public String getString(Object key) {
		return decoratedDoc.getString(key);
	}


	public Boolean getBoolean(Object key) {
		return decoratedDoc.getBoolean(key);
	}


	public boolean getBoolean(Object key, boolean defaultValue) {
		return decoratedDoc.getBoolean(key, defaultValue);
	}


	public ObjectId getObjectId(Object key) {
		return decoratedDoc.getObjectId(key);
	}


	public Date getDate(Object key) {
		return decoratedDoc.getDate(key);
	}


	public String toJson() {
		return decoratedDoc.toJson();
	}


	public String toJson(JsonWriterSettings writerSettings) {
		return decoratedDoc.toJson(writerSettings);
	}


	public String toJson(Encoder<Document> encoder) {
		return decoratedDoc.toJson(encoder);
	}


	public String toJson(JsonWriterSettings writerSettings, Encoder<Document> encoder) {
		return decoratedDoc.toJson(writerSettings, encoder);
	}


	public int size() {
		return decoratedDoc.size();
	}


	public boolean isEmpty() {
		return decoratedDoc.isEmpty();
	}


	public boolean containsValue(Object value) {
		return decoratedDoc.containsValue(value);
	}


	public boolean containsKey(Object key) {
		return decoratedDoc.containsKey(key);
	}


	public Object get(Object key) {
		return decoratedDoc.get(key);
	}


	public Object put(String key, Object value) {
		return decoratedDoc.put(key, value);
	}


	public Object remove(Object key) {
		return decoratedDoc.remove(key);
	}


	public void clear() {
		decoratedDoc.clear();
	}


	public Set<String> keySet() {
		return decoratedDoc.keySet();
	}


	public Collection<Object> values() {
		return decoratedDoc.values();
	}


	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		return decoratedDoc.entrySet();
	}


	public boolean equals(Object o) {
		return decoratedDoc.equals(o);
	}


	public int hashCode() {
		return decoratedDoc.hashCode();
	}


	public String toString() {
		return decoratedDoc.toString();
	}




	public static long getSerialversionuid() {
		return serialVersionUID;
	}




	public Document getDecoratedDoc() {
		return decoratedDoc;
	}




	public AttributeDocumentType getDocType() {
		return docType;
	}


	

}
