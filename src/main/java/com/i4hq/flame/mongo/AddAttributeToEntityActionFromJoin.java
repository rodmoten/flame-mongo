package com.i4hq.flame.mongo;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.bson.Document;

import com.i4hq.flame.core.EntityType;
import com.i4hq.flame.core.FlameEntity;
import com.i4hq.flame.core.FlameEntityFactory;

/**
 * This class handle results from queries that join the entities collectiona and the entity_attributes collection, such as a search for entities within a specific region.
 * @author rmoten
 *
 */
final class AddAttributeToEntityActionFromJoin implements Consumer<Document> {
	private final Map<String, FlameEntity> resultEntities;
	private final String attributesFieldName;
	private final EntityType entityType;
	private final String entityIdFieldName;


	/**
	 * @param entityIdFieldName
	 * @param resultEntities
	 * @param attributesFieldName
	 * @param entityType
	 */
	public AddAttributeToEntityActionFromJoin(String entityIdFieldName, Map<String, FlameEntity> resultEntities, String attributesFieldName,
			EntityType entityType) {
		super();
		this.resultEntities = resultEntities;
		this.attributesFieldName = attributesFieldName;
		this.entityType = entityType;
		this.entityIdFieldName = entityIdFieldName;
	}
	
	
	/**
	 * @param entityIdFieldName
	 * @param resultEntities
	 * @param attributesFieldName
	 * @param entityType
	 */
	public AddAttributeToEntityActionFromJoin(String entityIdFieldName, Map<String, FlameEntity> resultEntities, String attributesFieldName) {
		this(entityIdFieldName, resultEntities, attributesFieldName, null);
	}

	AddAttributeToEntityActionFromJoin(Map<String, FlameEntity> resultEntities, String attributesFieldName) {
		this(MongoFlameDAO.ID_FIELD, resultEntities, attributesFieldName, null);
	}

	@Override
	public void accept(Document doc) {
		MongoFlameDAO.logger.debug("Found doc: {}", doc);
		String entityId = doc.getString(entityIdFieldName);
		Double longitude = doc.getDouble(MongoFlameDAO.LONGITUDE_FIELD);
		Double latitude = doc.getDouble(MongoFlameDAO.LATITUDE_FIELD);
		FlameEntity entity = resultEntities.get(entityId);
		if (entity == null){
			entity = FlameEntityFactory.createEntity(entityId);
			resultEntities.put(entityId, entity);
		}
		
		@SuppressWarnings("unchecked")
		List<Document> attributes = (List<Document>) doc.get(attributesFieldName);
		for (Document attribute : attributes){
			MongoFlameDAO.addAttributeInJsonToEntity(entity, attribute, entityType);
		}
		
		if (longitude != null && latitude != null){
			entity.setLocation(longitude, latitude);
		}
	}
}