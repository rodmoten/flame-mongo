package com.i4hq.flame.mongo;

import java.util.Map;
import java.util.function.Consumer;

import org.bson.Document;

import com.i4hq.flame.core.EntityType;
import com.i4hq.flame.core.FlameEntity;
import com.i4hq.flame.core.FlameEntityFactory;

final class AddAttributeToEntityAction implements Consumer<Document> {
	private final Map<String, FlameEntity> resultEntities;
	private final EntityType entityType;

	AddAttributeToEntityAction(Map<String, FlameEntity> resultEntities) {
		this(resultEntities, null);
	}

	
	AddAttributeToEntityAction(Map<String, FlameEntity> resultEntities, EntityType entityType) {
		super();
		this.resultEntities = resultEntities;
		this.entityType = entityType;
	}


	@Override
	public void accept(Document t) {
		MongoFlameDAO.logger.debug("Found doc: {}", t);
		String entityId = t.getString(MongoFlameDAO.ENTITY_ID_FIELD);
		FlameEntity entity = resultEntities.get(entityId);
		if (entity == null){
			entity = FlameEntityFactory.createEntity(entityId);
			resultEntities.put(entityId, entity);
		}
		
		MongoFlameDAO.addAttributeInJsonToEntity(entity, t, entityType);
	}

}