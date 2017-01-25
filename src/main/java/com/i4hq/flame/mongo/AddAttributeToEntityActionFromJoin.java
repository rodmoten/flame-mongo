package com.i4hq.flame.mongo;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.bson.Document;

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

	AddAttributeToEntityActionFromJoin(Map<String, FlameEntity> resultEntities, String attributesFieldName) {
		this.resultEntities = resultEntities;
		this.attributesFieldName = attributesFieldName;
	}

	@Override
	public void accept(Document t) {
		MongoFlameDAO.logger.debug("Found doc: {}", t);
		String entityId = t.getString(MongoFlameDAO.ID_FIELD);
		Double longitude = t.getDouble(MongoFlameDAO.LONGITUDE_FIELD);
		Double latitude = t.getDouble(MongoFlameDAO.LATITUDE_FIELD);
		FlameEntity entity = resultEntities.get(entityId);
		if (entity == null){
			entity = FlameEntityFactory.createEntity(entityId);
			resultEntities.put(entityId, entity);
		}
		if (longitude != null && latitude != null){
			entity.setLocation(longitude, latitude);
		}
		// Get the attributes of the entity
		@SuppressWarnings("unchecked")
		List<Document> attributes = (List<Document>) t.get(attributesFieldName);
		for (Document attribute : attributes){
			MongoFlameDAO.addAttributeInJsonToEntity(entity, attribute);
		}
	}
}