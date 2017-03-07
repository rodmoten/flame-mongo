package com.i4hq.flame.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import com.i4hq.flame.core.AttributeValue;
import com.i4hq.flame.core.FlameEntity;
import com.i4hq.flame.core.FlameEntityFactory;
import com.i4hq.flame.core.GuidEntityIdFactory;
import com.mongodb.MongoClient;

public class MongoFlameDAOIT {

	private MongoFlameDAO dao;
	
	@Before
	public void setUp() throws Exception {
		executeScript("/Users/rmoten/git/flame-mongo/src/test/bash/setup-test.sh");
		MongoClient mongoClient = new MongoClient(System.getProperty("mongo.host", "localhost"));
		mongoClient.dropDatabase("flame");
		mongoClient.close();
		dao = MongoFlameDAO.getInstance();
		
	}

	private void executeScript(String script) {
		try
        {    
            String target = new String(script);
            Runtime rt = Runtime.getRuntime();
            String[] envp = {"PATH=/Users/rmoten/dev/apps/mongodb-osx-x86_64-3.4.1/bin/mongod"};
			Process process = rt.exec(target, envp);
			process.waitFor();

        } catch (Throwable t)
        {
            t.printStackTrace();
        }
	}
	
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
		for (Entry<String, List<AttributeValue>> expectedEntry : entity.getAttributes()) {
			String expectedAttributeName = expectedEntry.getKey();
			AttributeValue expectedAttributeValue = expectedEntry.getValue().get(0);
			assertEquals(expectedAttributeName, expectedAttributeValue, retrievedEntity.getAttribute(expectedAttributeName));
			assertTrue("has ts", expectedAttributeValue.getTimestamp() < System.currentTimeMillis());
		}
		
		
	}

	private FlameEntity readEntityFromFile(final String filePath) throws FileNotFoundException, IOException {
		StringBuilder jsonText = readJsonFromFile(filePath);
		FlameEntity entity = FlameEntityFactory.createFromJson(GuidEntityIdFactory.getInstance(), jsonText.toString());
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
		for (Entry<String, List<AttributeValue>> expectedEntry : entity2.getAttributes()) {
			String expectedAttributeName = expectedEntry.getKey();
			AttributeValue expectedAttributeValue = expectedEntry.getValue().get(0);
			assertEquals(expectedAttributeName, expectedAttributeValue, retrievedEntity.getAttribute(expectedAttributeName));
		}
		
	}
	
	@Test
	public void testNullTimestamp() throws Throwable{
		FlameEntity entity = FlameEntityFactory.createEntity("blah");
		Document doc = Document.parse("{ \"_id\" : \"2e221ea12073022a28c69444aaf52736\", \"value\" : true, \"attribute_name\" : \"properties:report\", \"type\" : \"BOOLEAN\", "
				+ "\"entity_id\" : \"8ffb6805518c29e27df949dc6eb1f70a\" }");
		MongoFlameDAO.addAttributeInJsonToEntity(entity, doc);
		
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
