package com.i4hq.flame.mongo;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import com.i4hq.flame.core.AttributeDecl;
import com.i4hq.flame.core.AttributeExpression;
import com.i4hq.flame.core.AttributeType;
import com.i4hq.flame.core.AttributeValue;
import com.i4hq.flame.core.EntityType;
import com.i4hq.flame.core.FlameEntity;
import com.i4hq.flame.core.FlameEntityFactory;
import com.i4hq.flame.core.GuidEntityIdFactory;
import com.i4hq.flame.core.MetadataItem;
import com.mongodb.MongoClient;

public class MongoFlameDAOIT {

	private MongoFlameDAO dao;

	@Before
	public void setUp() throws Exception {
		String testDb = "test&flame";
		System.setProperty("mongo.db", testDb);
		System.setProperty("mongo.host", "localhost");
		MongoClient mongoClient = new MongoClient("localhost");
		mongoClient.dropDatabase(testDb);
		mongoClient.close();

		dao = MongoFlameDAO.getInstance();

	}
	
	/**
	 * Test we can use any additional column as metadata.
	 * @throws Exception
	 */
	@Test 
	public void testAnyMetadata() throws Exception{
		dao.setBufferWriteThreshold(0);
		
		FlameEntity entity = new MongoFlameEntity("test1", dao);
		entity.addAttribute("noMetadata", "abcde", AttributeType.STRING);
		MetadataItem i1 = new MetadataItem("i1", "1.0");
		MetadataItem i2 = new MetadataItem("i2", "B");
		MetadataItem i3 = new MetadataItem("i3", "C");
		final String metadataAttribute = "hasMetadata";
		entity.addAttribute(metadataAttribute, "xyz", AttributeType.STRING, i1, i2, i3);
		
		assertEquals("saved", true, dao.save(entity));


		Collection<FlameEntity> results = dao.getEntitiesWithAttributeValue(metadataAttribute, "xyz");
		assertEquals("num of results", 1, results.size());
		FlameEntity retrievedEntity = (FlameEntity) results.toArray()[0];
		assertEquals("num of attributes", 2, retrievedEntity.getAttributes().size());
		AttributeValue actualAttributeValue = retrievedEntity.getAttribute(metadataAttribute);
		assertEquals("value of metadata attribute", actualAttributeValue.getValue(), "xyz");
		assertEquals("metadata i1", "1.0", actualAttributeValue.getMetadataValue("i1"));
		assertEquals("metadata i2", "B", actualAttributeValue.getMetadataValue("i2"));
		assertEquals("metadata i3", "C", actualAttributeValue.getMetadataValue("i3"));
		assertEquals("metadata i3", "C", actualAttributeValue.getMetadataValue("i3"));
		assertNull("metadata value", actualAttributeValue.getMetadataValue("value"));
		assertNull("metadata _id", actualAttributeValue.getMetadataValue("_id"));
		assertNull("metadata attribute_name", actualAttributeValue.getMetadataValue("attribute_name"));
		assertNull("metadata entity_id", actualAttributeValue.getMetadataValue("entity_id"));
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
		dao.setBufferWriteThreshold(1);
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
	public void addAttributeInJsonToEntity_nullTimeStamp() throws Throwable{
		FlameEntity entity = FlameEntityFactory.createEntity("blah");
		Document doc = Document.parse("{ \"_id\" : \"2e221ea12073022a28c69444aaf52736\", \"value\" : true, \"attribute_name\" : \"properties:report\", \"type\" : \"BOOLEAN\", "
				+ "\"entity_id\" : \"8ffb6805518c29e27df949dc6eb1f70a\" }");
		MongoFlameDAO.addAttributeInJsonToEntity(entity, doc, null);

	}

	@Test
	public void addAttributeInJsonToEntity_withEntityType() throws Throwable{
		FlameEntity entity = FlameEntityFactory.createEntity("blah");
		EntityType et = new EntityType(100, new AttributeDecl("properties:report", AttributeType.BOOLEAN), new AttributeDecl("sloppy", AttributeType.NUMBER));

		Document doc = Document.parse("{ \"_id\" : \"2e221ea12073022a28c69444aaf52731\", \"value\" : true, \"attribute_name\" : \"properties:report\", \"type\" : \"BOOLEAN\", "
				+ "\"entity_id\" : \"8ffb6805518c29e27df949dc6eb1f70a\" 'ts':NumberLong(100)}");
		assertEquals("properties:report boolean", true , MongoFlameDAO.addAttributeInJsonToEntity(entity, doc, et));

		doc = Document.parse("{ \"_id\" : \"2e221ea12073022a28c69444aaf52732\", \"value\" : 'silly', \"attribute_name\" : \"properties:report\", \"type\" : \"STRING\", "
				+ "\"entity_id\" : \"8ffb6805518c29e27df949dc6eb1f70a\" 'ts':NumberLong(200)}");
		assertEquals("properties:report string", false , MongoFlameDAO.addAttributeInJsonToEntity(entity, doc, et));

		doc = Document.parse("{ \"_id\" : \"2e221ea12073022a28c69444aaf52733\", \"value\" : 13, \"attribute_name\" : \"sloppy\", \"type\" : \"NUMBER\", "
				+ "\"entity_id\" : \"8ffb6805518c29e27df949dc6eb1f70a\" 'ts':NumberLong(300)}");
		assertEquals("properties:report string", true , MongoFlameDAO.addAttributeInJsonToEntity(entity, doc, et));

		// Not added because of the wrong attribute type
		doc = Document.parse("{ \"_id\" : \"2e221ea12073022a28c69444aaf52734\", \"value\" : 13, \"attribute_name\" : \"sloppy\", \"type\" : \"STRING\", "
				+ "\"entity_id\" : \"8ffb6805518c29e27df949dc6eb1f70a\" 'ts':NumberLong(400)}");
		assertEquals("properties:report string", false , MongoFlameDAO.addAttributeInJsonToEntity(entity, doc, et));
		

		assertEquals("properties:report value", "true", entity.getAttribute("properties:report").getValue());
		assertEquals("properties:report Count", 1, entity.getAttributes("properties:report").size());
		assertEquals("SLOPPY", null, entity.getAttribute("SLOPPY"));
		assertEquals("sloppy value", "13", entity.getAttribute("sloppy").getValue());
	}

	
	@Test
	public void retrieveEntityByTypeWithAge() throws  Exception{
		final String filePath = "src/test/resources/entity2.json";
		FlameEntity entity2 = readEntityFromFile(filePath);

		assertEquals("saved", true, dao.save(entity2));
		dao.flush();
		Thread.sleep(1000);
		long newerThanEntity2 = System.currentTimeMillis();

		Thread.sleep(1000);
		FlameEntity entity1 = readEntityFromFile("src/test/resources/entity1.json");

		assertEquals("saved", true, dao.save(entity1));
		Collection<FlameEntity> result = dao.getEntitiesByAttributeExpression(AttributeExpression.fromType(new EntityType(newerThanEntity2)));
		assertEquals(1, result.size());
		assertEquals(entity1.getId(), result.toArray(new FlameEntity[0])[0].getId());		
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
