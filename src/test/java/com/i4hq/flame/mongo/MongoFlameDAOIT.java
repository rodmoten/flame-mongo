package com.i4hq.flame.mongo;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map.Entry;

import org.junit.Test;

import com.i4hq.flame.core.AttributeValue;
import com.i4hq.flame.core.FlameEntity;
import com.i4hq.flame.core.FlameEntityFactory;
import com.i4hq.flame.core.GuidEntityIdFactory;

public class MongoFlameDAOIT {

	private MongoFlameDAO dao = MongoFlameDAO.getInstance();
	
	
	@Test
	public void testSaveEntity_unbufferedWrites() throws Throwable {
		dao.setBufferWriteThreshold(0);
		
		final String filePath = "src/test/resources/entity1.json";
		FlameEntity entity = readEntityFromFile(filePath);
		
		assertEquals("saved", true, dao.save(entity));
		
		String id = entity.getId();

		FlameEntity retrievedEntity = dao.getEntitiesById(id);
		assertEquals("num of attributes", 9, retrievedEntity.getAttributes().size());
		String expectedType = entity.getType();
		assertEquals("type",expectedType, retrievedEntity.getType());
		for (Entry<String, AttributeValue> expectedEntry : entity.getAttributes()) {
			String expectedAttributeName = expectedEntry.getKey();
			AttributeValue expectedAttributeValue = expectedEntry.getValue();
			assertEquals(expectedAttributeName, expectedAttributeValue, retrievedEntity.getAttribute(expectedAttributeName));
		}
		
		
	}

	private FlameEntity readEntityFromFile(final String filePath) throws FileNotFoundException, IOException {
		StringBuilder jsonText = readJsonFromFile(filePath);
		FlameEntity entity = FlameEntityFactory.createFromJson(GuidEntityIdFactory.getInstance(), dao, jsonText.toString());
		return entity;
	}
	
	@Test
	public void testSaveEntity_bufferedWrites() throws Throwable {
		dao.setBufferWriteThreshold(10000);
		
		final String filePath = "src/test/resources/entity2.json";
		FlameEntity entity2 = readEntityFromFile(filePath);
		
		assertEquals("saved", true, dao.save(entity2));
		
		String id2 = entity2.getId();

		FlameEntity retrievedEntity = dao.getEntitiesById(id2);
		assertEquals("num of attributes before flush", 0, retrievedEntity.getAttributes().size());
		dao.setBufferWriteThreshold(10);
		FlameEntity entity1 = readEntityFromFile("src/test/resources/entity1.json");
		
		assertEquals("saved", true, dao.save(entity1));
		
		retrievedEntity = dao.getEntitiesById(id2);
		assertEquals("num of attributes", 9, retrievedEntity.getAttributes().size());
		String expectedType = entity2.getType();
		assertEquals("type",expectedType, retrievedEntity.getType());
		for (Entry<String, AttributeValue> expectedEntry : entity2.getAttributes()) {
			String expectedAttributeName = expectedEntry.getKey();
			AttributeValue expectedAttributeValue = expectedEntry.getValue();
			assertEquals(expectedAttributeName, expectedAttributeValue, retrievedEntity.getAttribute(expectedAttributeName));
		}
		
	}

	private StringBuilder readJsonFromFile(String filePath) throws FileNotFoundException, IOException {
		BufferedReader reader = new BufferedReader (new FileReader (filePath));
		StringBuilder jsonText = new StringBuilder();
		while (reader.ready()) {
			jsonText.append(reader.readLine());
			jsonText.append('\n');
		}
		reader.close();
		return jsonText;
	}
}
