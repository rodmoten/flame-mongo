/**
 * 
 */
package com.i4hq.flame.mongo;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonElement;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.i4hq.flame.core.AttributeExpression;
import com.i4hq.flame.core.AttributeType;
import com.i4hq.flame.core.AttributeValue;
import com.i4hq.flame.core.FlameEntity;
import com.i4hq.flame.core.FlameEntityDAO;
import com.i4hq.flame.core.FlameEntityFactory;
import com.i4hq.flame.core.GeospatialPosition;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;

/**
 * @author rmoten
 *
 */
public class MongoFlameDAO implements FlameEntityDAO {

	private static Logger logger = LoggerFactory.getLogger(MongoFlameDAO.class);

	private static final String TYPE_FIELD = "type";
	private static final String VALUE_FIELD = "value";
	private static final String TYPE_EXPR_FIELD = "type_expr";

	private static final String TS_FIELD = "ts";
	private static final String ID_FIELD = "_id";
	public static final String LOCATION_FIELD = "loc";
	private static final String CONCEPT_FIELD = "concepts";

	private static final String ENTITY_ID_FIELD = "entity_id";
	private static final String ATTRIBUTE_NAME_FIELD = "attribute_name";

	private static final int MAX_MONGO_KEY_SIZE = 1024;

	private static final String LONG_STRING_FIELD = "orig_value";

	private static final String REFERENCE_FIELD = "ref";

	private static final String TEXT_FIELD = "text";


	private final class AddAttributeToEntityAction implements Consumer<Document> {
		private final Map<String, FlameEntity> resultEntities;

		private AddAttributeToEntityAction(Map<String, FlameEntity> resultEntities) {
			this.resultEntities = resultEntities;
		}

		@Override
		public void accept(Document t) {
			logger.debug("Found doc: {}", t);
			String entityId = t.getString(ID_FIELD);
			FlameEntity entity = resultEntities.get(entityId);
			if (entity == null){
				entity = FlameEntityFactory.createEntity(entityId);
				resultEntities.put(entityId, entity);
			}
			addAttributeInJsonToEntity(entity, t);
		}

	}

	public class BulkWriter {

		private long lastWrite = System.currentTimeMillis();
		private long waitTimeBeforeFlush;
		/**
		 * The minimum number of documents to keep in memory before sending to the server.
		 */
		private int bufferWriteThreshold;

		private final InsertManyOptions insertManyOptions = new InsertManyOptions().ordered(false);
		private List<Document> buffer = new LinkedList<>();
		private final MongoCollection<Document> collection;

		public BulkWriter(MongoCollection<Document> collection) {
			this.collection = collection;
			setBufferWriteThreshold(Integer.parseInt(System.getProperty("MONGO_FLAME_BULK_WRITE_MIN_THRESHOLD", "1000")));
			logger.info("MONGO_FLAME_BULK_WRITE_WAITTIME = {}", waitTimeBeforeFlush);
			logger.info("MONGO_FLAME_BULK_WRITE_MIN_THRESHOLD = {}", bufferWriteThreshold);
		}

		/**
		 * @param collection
		 * @param docs
		 * @return Returns true if and only if it performed the write.
		 */
		public boolean write(List<Document> docs) {
			buffer.addAll(docs);
			return insertMany();

		}

		private boolean insertMany() {
			if (buffer.size() > bufferWriteThreshold || System.currentTimeMillis() - lastWrite > waitTimeBeforeFlush){
				flush(); 
				buffer = new LinkedList<>();
				lastWrite = System.currentTimeMillis();
				return true;
			}
			return false;
		}


		public void flush() {
			try {				
				collection.insertMany(buffer, insertManyOptions);
			} catch (MongoBulkWriteException ex) {
				logger.debug(ex.getMessage()); 
			}
		}

		@Override
		protected void finalize() throws Throwable {
			close();
		}

		public void setBufferWriteThreshold(int bufferWriteThreshold) {
			this.bufferWriteThreshold = bufferWriteThreshold;
			waitTimeBeforeFlush = this.bufferWriteThreshold + (long) (this.bufferWriteThreshold * 0.10);
		}

		public boolean write(Document doc) {
			buffer.add(doc);
			return insertMany();

		}

		public void close() {
			flush();

		}

	}



	/**
	 * This class handle results from queries that join the entities collectiona and the entity_attributes collection, such as a search for entities within a specific region.
	 * @author rmoten
	 *
	 */
	private final class AddAttributeToEntityActionFromJoin implements Consumer<Document> {
		private final Map<String, FlameEntity> resultEntities;
		private final String attributesFieldName;

		private AddAttributeToEntityActionFromJoin(Map<String, FlameEntity> resultEntities, String attributesFieldName) {
			this.resultEntities = resultEntities;
			this.attributesFieldName = attributesFieldName;
		}

		@Override
		public void accept(Document t) {
			logger.debug("Found doc: {}", t);
			String entityId = t.getString(ID_FIELD);
			FlameEntity entity = resultEntities.get(entityId);
			if (entity == null){
				entity = FlameEntityFactory.createEntity(entityId);
				resultEntities.put(entityId, entity);
			}
			// Get the attributes of the entity
			@SuppressWarnings("unchecked")
			List<Document> attributes = (List<Document>) t.get(attributesFieldName);
			for (Document attribute : attributes){
				addAttributeInJsonToEntity(entity, attribute);
			}
		}
	}

	/**
	 * @param entity
	 * @param t
	 */
	private void addAttributeInJsonToEntity(FlameEntity entity, Document t) {
		entity.addAttribute(t.getString(ATTRIBUTE_NAME_FIELD), t.get(VALUE_FIELD), AttributeType.valueOf(t.getString(TYPE_FIELD)));
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


	//private static MongoFlameDAO instance = new MongoFlameDAO();

	public static MongoFlameDAO getInstance() {
		return new MongoFlameDAO();
	}

	private boolean isConnected = false;
	private MongoClient mongoClient;
	private MongoCollection<Document> typesCollection;
	private MongoCollection<Document> entitiesCollection;
	private MongoCollection<Document> entityAttributesCollection;

	private BulkWriter[] bulkWriters = new BulkWriter[3];
	final private int entityAttributesBulkWriter = 0;
	final private int entityBulkWriter = 1;
	final private int typesBulkWriter = 2;

	private MongoFlameDAO () {
		init();
	}	


	/* (non-Javadoc)
	 * @see java.lang.Object#finalize()
	 */
	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}

	public void close() {
		for(BulkWriter writer : bulkWriters) {
			if (writer != null) {
				writer.close();
			}
		}
		mongoClient.close();
	}

	private synchronized void init() {
		if (isConnected) {
			return;
		}
		mongoClient = new MongoClient(System.getProperty("mongo.host", "localhost"));
		final MongoDatabase database = mongoClient.getDatabase(System.getProperty("mongo.db", "flame"));
		entitiesCollection = database.getCollection("entities");
		typesCollection = database.getCollection("types");
		entityAttributesCollection = database.getCollection("entity_attributes");
		isConnected = true;
		bulkWriters[entityAttributesBulkWriter] = new BulkWriter(entityAttributesCollection);
		bulkWriters[entityBulkWriter] = new BulkWriter(entitiesCollection);
		bulkWriters[typesBulkWriter] = new BulkWriter(typesCollection);
	}

	protected boolean connect() {
		if (isConnected) {
			return isConnected;
		}
		init();
		return isConnected;
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
			if (entity.getGeospatialPosition() != null) {
				entitiesDocument.append(LOCATION_FIELD, toGeoJsonPoint(entity.getGeospatialPosition()));
			}
			Document typesDocument = new Document(ID_FIELD, typeHash).append(TYPE_EXPR_FIELD, entity.getType());
			List<Document> attributes = toEntityAttributesDocuments(entity);

			// Save type. No need to roll this back. May through an exception because of a duplicate type
			try {
				bulkWriters[typesBulkWriter].write(typesDocument);
			} catch (MongoWriteException ex) {
				// Assume this only occurs when the type already exists. Therefore we ignore it since different entities may have the same type.
				logger.debug(ex.getMessage());
			}

			// Save to entities collection.
			try {
				bulkWriters[this.entityBulkWriter].write(entitiesDocument);
				step = SaveTransactionStep.SAVED_TO_ENTITIES_COLLECTION;
			} catch (MongoWriteException ex) {
				// Assuming we only get this when we inserting an entity that already exists.
				logger.debug(ex.getMessage());
			}

			// Save attributes.
			// Fail if any insert fails.
			try {
				bulkWriters[entityAttributesBulkWriter].write(attributes);
			} catch (MongoBulkWriteException ex) {
				logger.debug(ex.getMessage()); 
			}
			insertCompletedSuccessfully = true;
		} 
		catch (Throwable ex) {
			logger.error("Error saving. Rolling back", ex);
			rollbackSave(step, entity);
		} 
		return insertCompletedSuccessfully;
	}

	private Document toGeoJsonPoint(GeospatialPosition gp) {
		Document doc = Document.parse(String.format("{ type: 'Point', coordinates: [ %f, %f ] }", gp.getLongitude(), gp.getLatitude()));
		return doc;
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

	private String createAttributeId(Object attributeValue, String attributeName, byte[] entityId) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(attributeValue == null ? "".getBytes() : attributeValue.toString().getBytes());
			md.update(attributeName.getBytes());
			md.update(entityId);
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
		List<Document> docs = new ArrayList<>(entity.size());
		byte[] entityIdInBytes = entity.getId().getBytes();

		// Create a document for each attribute.
		for (Entry<String, List<AttributeValue>> attributes : entity.getAttributes()){
			final List<AttributeValue> values = attributes.getValue();
			if (values == null) {
				continue;
			}
			for (AttributeValue attribute : values){
				Document doc = new Document();
				addAttributeColumns(doc, entityIdInBytes, attributes.getKey(), attribute);
				doc.append(ENTITY_ID_FIELD, entity.getId());
				doc.append(TS_FIELD, System.currentTimeMillis());
				docs.add(doc);
			}
		}

		return docs;
	}
	private void addAttributeColumns(Document doc, byte[] entityIdInBytes, String attributePathName, AttributeValue attribute) {

		AttributeType attributeType = attribute.getType();
		String value = attribute.getValue();
		final String attributeId = createAttributeId(value, attributePathName, entityIdInBytes);

		if (attributeType == AttributeType.REFERENCE){
			addToIndexableField(doc, REFERENCE_FIELD, value);	
			value = "...";
		} else if (attributeType == AttributeType.STRING && containsSpace(value)){
			doc.append(TEXT_FIELD, value);
			// If the string is too long, then ellide it. Since is has been added as a text field.
			value = isLongString(value) ? "..." : value;
		}
		// We will ellide long STRING value  because MongoDB cannot index them. 
		// long STRING values to TEXT. We 
		doc.append(ID_FIELD, attributeId);
		doc.append(ATTRIBUTE_NAME_FIELD, attributePathName);
		doc.append(TYPE_FIELD, attributeType.toString());
		addToIndexableField(doc, VALUE_FIELD, attributeType.convertToJava(value));	
	}

	private boolean containsSpace(String value) {
		if (value == null){
			return false;
		}
		int len = value.length();
		for (int i = 0; i < len; i++){
			if (value.charAt(i) == ' '){
				return true;
			}
		}
		return false;
	}


	private void addToIndexableField(Document doc, String fieldName, Object v){
		if (!(v instanceof String)){
			doc.append(fieldName, v);
			return;
		}
		String s = (String) v;

		if (!isLongString(s)){
			doc.append(fieldName, s);
		} else {
			doc.append(fieldName, "...");
			doc.append(LONG_STRING_FIELD, s);
		}
	}

	private boolean isLongString(String s){
		return s != null && s.length() / 2 >= MAX_MONGO_KEY_SIZE;
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
		final FlameEntity entity = FlameEntityFactory.createEntity(id);
		Consumer<Document> addAttribute = new Consumer<Document>(){

			@Override
			public void accept(Document t) {
				addAttributeInJsonToEntity(entity, t);
			}
		};

		entityAttributesCollection.find(Filters.eq(ENTITY_ID_FIELD, entity.getId())).forEach(addAttribute);

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
		case WITHIN:
			return getAttributiesByGeospatialRegion(expr.getAttributeName(), expr.getCoordinates());
		default:
			return new LinkedList<>();
		}
	}


	private Collection<FlameEntity> getAttributiesByGeospatialRegion(String attributeName, GeospatialPosition[] geospatialPositions) {
		final Map<String, FlameEntity> resultEntities = new HashMap<>();

		String attributesFieldName = "attributes";
		Consumer<Document> addAttribute = new AddAttributeToEntityActionFromJoin(resultEntities, attributesFieldName);		

		if (geospatialPositions.length < 3) {
			logger.info("Not enough points to form a polygon.");
			return resultEntities.values();
		}

		// Here an example of the kind of query we are trying to generate.
		//		db.entities.aggregate([{$match: { loc: { $geoWithin: { $geometry: { type: "Polygon", coordinates: [ [ [ 20.73414993286133, 56.85886001586914 ], 
		//		[ 20.29469680786133, 33.51984024047852 ], [ 70.91969299316406, 33.66626358032227 ], [ 63.97633743286133, 57.14605331420898 ], [ 50.00172805786133, 57.288818359375 ], 
		//		[ 20.73414993286133, 56.85886001586914 ] ] ] } } } }}, 
		//		{$lookup: { from:"entity_attributes", localField:"_id", foreignField: "entity_id", as: "attributes"}}])

		List<BsonValue> coordinates = new LinkedList<>();
		// The first point must be the first and last in the list.
		BsonArray firstPosition = createBsonGeoCoordinate(geospatialPositions[0]);
		coordinates.add(firstPosition);

		for (int i = 1; i < geospatialPositions.length; i++) {
			GeospatialPosition gp = geospatialPositions[i];
			BsonArray p = createBsonGeoCoordinate(gp);
			coordinates.add(p);
		}
		coordinates.add(firstPosition);
		BsonDocument geoWithin =
				new BsonDocument("$geoWithin",
						new BsonDocument("$geometry", new BsonDocument(Arrays.asList(
								new BsonElement("type", new BsonString("Polygon")),
								new BsonElement ("coordinates", new BsonArray(Arrays.asList(new BsonArray(coordinates))))))));

		BsonDocument match = new BsonDocument("$match", new BsonDocument(LOCATION_FIELD, geoWithin));

		// This is used to join the entities in the polygon to their attributes.
		BsonDocument lookup = new BsonDocument("$lookup",new BsonDocument(Arrays.asList(
				new BsonElement("from", new BsonString("entity_attributes")),
				new BsonElement("localField", new BsonString("_id")),
				new BsonElement("foreignField", new BsonString(ENTITY_ID_FIELD)),
				new BsonElement("as", new BsonString(attributesFieldName))
				)));

		List<? extends Bson> pipelines = Arrays.asList(match, lookup);

		entitiesCollection.aggregate(pipelines).forEach(addAttribute);

		logger.debug("Number of results: {}", resultEntities.size());
		return resultEntities.values();
	}


	/**
	 * @param gp
	 * @return
	 */
	private BsonArray createBsonGeoCoordinate(GeospatialPosition gp) {
		return new BsonArray(Arrays.asList(new BsonDouble(gp.getLongitude()), new BsonDouble(gp.getLatitude())));
	}

	public void setBufferWriteThreshold(int i) {
		for (BulkWriter writer : bulkWriters) {
			writer.setBufferWriteThreshold(i);
		}


	}

	public void flush() {
		for (BulkWriter writer : bulkWriters) {
			writer.flush();
		}
	}

}
