/**
 * 
 */
package com.i4hq.flame.mongo;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.i4hq.flame.core.AttributeExpression;
import com.i4hq.flame.core.AttributeIdFactory;
import com.i4hq.flame.core.AttributeType;
import com.i4hq.flame.core.AttributeValue;
import com.i4hq.flame.core.FlameEntity;
import com.i4hq.flame.core.FlameEntityDAO;
import com.i4hq.flame.core.GeospatialPosition;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.PolygonCoordinates;
import com.mongodb.client.model.geojson.Position;

/**
 * @author rmoten
 *
 */
public class MongoFlameDAO implements AttributeIdFactory, FlameEntityDAO {

	private static Logger logger = LoggerFactory.getLogger(MongoFlameDAO.class);

	private static final String TYPE_FIELD = "type";
	private static final String VALUE_FIELD = "value";
	private static final String TYPE_EXPR_FIELD = "type_expr";


	private final class AddAttributeToEntityAction implements Consumer<Document> {
		private final Map<String, FlameEntity> resultEntities;

		private AddAttributeToEntityAction(Map<String, FlameEntity> resultEntities) {
			this.resultEntities = resultEntities;
		}

		@Override
		public void accept(Document t) {
			String entityId = t.getString(ID_FIELD);
			FlameEntity entity = resultEntities.get(entityId);
			if (entity == null){
				entity = FlameEntity.createEntity(entityId);
				resultEntities.put(entityId, entity);
			}
			entity.addAttribute(t.getString(ID_FIELD), t.get(VALUE_FIELD), AttributeType.valueOf(t.getString(TYPE_FIELD)));
		}
	}


	/**
	 * This enumeration indicates the steps performed in a save transaction of an entity.
	 * This is needed because a save transaction has to write to multiple collections.
	 * @author rmoten
	 *
	 */
	private enum SaveTransactionStep {
		START, SAVED_TO_ENTITIES_COLLECTION,
		;

	}


	private static final String STATE_FIELD = "state";
	private static final String ATTRIBUTE_NAMES_COLLECTION = "attribute_names";
	private static final String TS_FIELD = "ts";
	private static final String NAME_FIELD = "name";
	private static final String ID_FIELD = "_id";
	private static MongoFlameDAO instance = new MongoFlameDAO();

	public static MongoFlameDAO getInstance() {
		return instance;
	}

	private Map<String, Long> attributeIds = new HashMap<>();
	private boolean isConnected = false;
	private MongoClient mongoClient;
	private MongoCollection<Document> attributesCollection;
	private MongoCollection<Document> locks;
	private MongoCollection<Document> typesCollection;
	private MongoCollection<Document> entitiesCollection;
	private MongoCollection<Document> entityAttributesCollection;

	private int maxCacheSize = 1024 * 10;
	private long lastCacheUpdate = 0;

	private MongoFlameDAO () {
		init();
	}	


	/* (non-Javadoc)
	 * @see java.lang.Object#finalize()
	 */
	@Override
	protected void finalize() throws Throwable {
		mongoClient.close();
		super.finalize();
	}


	private synchronized void init() {
		if (isConnected) {
			return;
		}
		mongoClient = new MongoClient(System.getProperty("mongo.host", "localhost"));
		final MongoDatabase database = mongoClient.getDatabase(System.getProperty("mongo.db", "flame"));
		attributesCollection = database.getCollection(ATTRIBUTE_NAMES_COLLECTION);
		entitiesCollection = database.getCollection("entities");
		typesCollection = database.getCollection("types");
		entityAttributesCollection = database.getCollection("entity_attributes");
		locks = database.getCollection("locks");
		isConnected = true;

		updateCache("");
	}

	protected boolean connect() {
		if (isConnected) {
			return isConnected;
		}
		init();
		return isConnected;
	}

	/* (non-Javadoc)
	 * @see com.i4hq.flame.AttributeIdFactory#getAttributeId(java.lang.String)
	 */
	@Override
	public long getAttributeId(String attributeName) {
		Long idOfAttribute = attributeIds.get(attributeName);
		if (idOfAttribute != null) {
			return idOfAttribute;
		}

		// Update the caches with the latest from centralized store including the one we are looking for.
		updateCache(attributeName);

		// Check if the ID is in the cache.
		idOfAttribute = attributeIds.get(attributeName);
		if (idOfAttribute != null) {
			return idOfAttribute;
		}

		// Obtain exclusive access to the attribute names collection. 
		lockAttributeStore();
		try {
			// Make the ID the total number of attributes plus 1.
			idOfAttribute = attributesCollection.count() +1;

			// Write the new ID to the store.
			attributesCollection.insertOne(new Document().append(ID_FIELD, idOfAttribute).append(NAME_FIELD, attributeName).append(TS_FIELD, System.currentTimeMillis()));

			// update the cache 
			attributeIds.put(attributeName, idOfAttribute);
		} finally {
			unlockAttributeStore();
		}
		return idOfAttribute;
	}

	/**
	 * Acquire the lock for the attribute names store.
	 */
	private void lockAttributeStore() {
		// Use find and update atomic

		while (null != locks.findOneAndUpdate(Filters.and(Filters.eq(ID_FIELD, ATTRIBUTE_NAMES_COLLECTION), Filters.eq(STATE_FIELD,false)), Updates.set(STATE_FIELD, true))) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				// Ignore the interruption.
			}
		}
	}

	/**
	 * Release the lock for the attribute names store.
	 */
	private void unlockAttributeStore() {
		locks.findOneAndUpdate(Filters.eq(ID_FIELD, ATTRIBUTE_NAMES_COLLECTION), Updates.set(STATE_FIELD, false));

	}

	/**
	 * This method updates the cache of attribute IDs with the ID of the given attribute, if it exists, and the last N added since the last cache update. 
	 * N is determined based on whether the cache is empty or not. If the cache is empty, then N is the maximum cache size. If the cache is not empty N is 10% of the maximum cache size.
	 * @param attributeName
	 */
	protected synchronized void updateCache(final String attributeName) {
		int numberToFetch = attributeIds.isEmpty() ? maxCacheSize : (int) (maxCacheSize * 0.10);
		connect();

		// We want to update the cache with a specific attribute name and all other new attributes added since we last updated the cache.
		// This requires two queries if we want to limit the number of results returned, one to get the newly added attribute names and one to get the specific attribute name.
		// It is possible the specific attribute name will be in the results for the newly added attributes.
		// Therefore, we only do the second query if the specific attribute name isn't included in the results.
		final Map<String, Long> cachedIds = this.attributeIds;

		// Use this to indicate whether the given attribute name was returned in the results.
		final ValueRef<Boolean> notInLastUpdateResults = new ValueRef<>(false);
		Consumer<? super Document> action = new Consumer<Document>(){

			@Override
			public void accept(Document t) {
				long id = t.getLong(ID_FIELD);
				String name = t.getString(NAME_FIELD);
				cachedIds.put(name, id);
				lastCacheUpdate = System.currentTimeMillis();
				if (attributeName.equals(name)) {
					notInLastUpdateResults.setValue(true);
				}
			}			
		};

		Bson attributeNameSinceLastCacheUpdate = Filters.gte(TS_FIELD, lastCacheUpdate);
		attributesCollection.find(attributeNameSinceLastCacheUpdate).limit(numberToFetch).forEach(action);

		if (notInLastUpdateResults.getValue()){
			attributesCollection.find(Filters.eq(NAME_FIELD, attributeName)).limit(numberToFetch).forEach(action);
		}

	}


	/* (non-Javadoc)
	 * @see com.i4hq.flame.EntityDAO#save(com.i4hq.flame.Entity)
	 */
	@Override
	public boolean save(FlameEntity entity) {		
		SaveTransactionStep step = SaveTransactionStep.START;
		boolean insertCompletedSuccessfully = false;
		try {
			String typeHash = createHash(entity.getType());
			Document entitiesDocument = new Document(ID_FIELD, entity.getId()).append(TYPE_FIELD, typeHash);
			Document typesDocument = new Document(ID_FIELD, typeHash).append(TYPE_EXPR_FIELD, entity.getType());
			List<Document> attributes = toEntityAttributesDocuments(entity);

			// Save type. No need to roll this back. May through an exception because of a duplicate type
			try {
				typesCollection.insertOne(typesDocument);
			} catch (MongoWriteException ex) {
				// Assume this only occurs when the type already exists. Therefore we ignore it since different entities may have the same type.
			}

			// Save to entities collection.
			try {
				entitiesCollection.insertOne(entitiesDocument);
				step = SaveTransactionStep.SAVED_TO_ENTITIES_COLLECTION;
			} catch (MongoWriteException ex) {
				// Assuming we only get this when we inserting an entity that already exists. 
			}

			// Save attributes.
			// Fail if any insert fails.
			try {
				entityAttributesCollection.insertMany(attributes,new InsertManyOptions().ordered(false));
			} catch (MongoBulkWriteException ex) {
				// Assuming we only get this when we inserting attributes that already exists. 
			}
			insertCompletedSuccessfully = true;
		} 
		catch (Throwable ex) {
			logger.error("Error saving. Rolling back", ex);
			rollbackSave(step, entity);
		} 
		return insertCompletedSuccessfully;
	}

	private String createHash(String type) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(type.getBytes());
			return new BigInteger(1,md.digest()).toString(16);
		} catch (NoSuchAlgorithmException e) {
			logger.error("MD5", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public int save(List<FlameEntity> entities) {
		int count = 0;
		for (FlameEntity entity : entities){
			if (save(entity)) {
				count++;
			}
		}

		return count;
	}

	/**
	 * @param entity
	 * @param idOfEntityType - the ID of the type of the entity.
	 * @return
	 */
	private List<Document> toEntityAttributesDocuments(FlameEntity entity){
		List<Document> docs = new ArrayList<>(entity.getAttributes().size());
		// Create a document for each attribute.
		for (Entry<String, AttributeValue> attribute : entity.getAttributes().entrySet()){
			Document doc = new Document(ID_FIELD, attribute.getKey());
			AttributeType attributeType = attribute.getValue().getType();
			doc.append(VALUE_FIELD, attributeType.convertToJava(attribute.getValue().getValue()));
			doc.append(TYPE_FIELD, attributeType.toString());
			docs.add(doc);
		}

		return docs;
	}
	private void rollbackSave(SaveTransactionStep step, FlameEntity entity) {
		switch (step) {
		case SAVED_TO_ENTITIES_COLLECTION:
			logger.warn("Rolling back writing of '{}' to the entities collection.", entity.getId());
			entitiesCollection.deleteOne(Filters.eq(ID_FIELD, entity.getId()));
		case START:

		}

	}


	@Override
	public FlameEntity getEntitiesById(final String id) {
		final FlameEntity entity = FlameEntity.createEntity(id);
		Consumer<Document> addAttribute = new Consumer<Document>(){

			@Override
			public void accept(Document t) {
				entity.addAttribute(t.getString(ID_FIELD), t.get(VALUE_FIELD), AttributeType.valueOf(t.getString(TYPE_FIELD)));
			}
		};

		entityAttributesCollection.find(Filters.regex(ID_FIELD, entity.getEntityIdPrefix() + "*")).forEach(addAttribute);

		return entity;
	}

	@Override
	public List<FlameEntity> getEntitiesByIds(List<String> ids) {

		final List<FlameEntity> results = new LinkedList<>();
		for (String id : ids){
			results.add(getEntitiesById(id));
		}
		return results;
	}

	@Override
	public Collection<FlameEntity> getEntitiesWithAttributeValue(String attributePath, String value) {
		final Map<String, FlameEntity> resultEntities = new HashMap<>();

		Consumer<Document> addAttribute = new AddAttributeToEntityAction(resultEntities);		

		Bson regex = Filters.regex(ID_FIELD, ".*" + FlameEntity.ENITY_ID_ATTIRBUTE_PATH_SEPARATOR);
		entityAttributesCollection.find(regex).forEach(addAttribute);;

		return resultEntities.values();
	}


	@Override
	public Collection<FlameEntity> getEntitiesByAttributeExpression(AttributeExpression expr) {
		switch(expr.getOperator()){
		case IN:
			return getAttributiesByGeospatialRegion(expr.getAttributeName(), expr.getCoordinates());
		default:
			return new LinkedList<>();
		}
	}


	private Collection<FlameEntity> getAttributiesByGeospatialRegion(String attributeName, GeospatialPosition[] geospatialPositions) {
		final Map<String, FlameEntity> resultEntities = new HashMap<>();

		Consumer<Document> addAttribute = new AddAttributeToEntityAction(resultEntities);		

		if (geospatialPositions.length < 8 && geospatialPositions.length % 2 != 0) {

		}
		List<Position> positions = new LinkedList<>();
		for (GeospatialPosition gp : geospatialPositions) {

			Position p = new Position(gp.getLongitude(), gp.getLatitude());
			positions.add(p);
		}
		@SuppressWarnings("unchecked")
		PolygonCoordinates polyGoncoordinates = new PolygonCoordinates(positions);
		Polygon polygon = new Polygon(polyGoncoordinates);

		entityAttributesCollection.find(Filters.geoWithin("location", polygon)).forEach(addAttribute);;

		return resultEntities.values();
	}

}
