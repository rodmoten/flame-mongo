package com.i4hq.flame.mongo;

import java.util.Map;

import com.i4hq.flame.core.FlameEntity;

public class MongoFlameEntity extends FlameEntity {

	private final MongoFlameDAO dao;
	private Map<String, String> references = null;
	protected MongoFlameEntity(String id, MongoFlameDAO dao) {
		super(id);
		this.dao = dao;
	}

	@Override
	public String getReference(String referenceName) {
		if (references == null) {
			references = dao.retrieveReferences(this.getId());
		}
		return references.get(referenceName);	
	}


}
