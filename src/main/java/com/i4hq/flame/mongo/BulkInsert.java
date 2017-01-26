package com.i4hq.flame.mongo;

import java.util.LinkedList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;

public class BulkInsert implements BulkOperation{

	private long lastWrite = System.currentTimeMillis();
	private long waitTimeBeforeFlush;
	/**
	 * The minimum number of documents to keep in memory before sending to the server.
	 */
	private int bufferWriteThreshold;

	private final InsertManyOptions insertManyOptions = new InsertManyOptions().ordered(false);
	private List<Document> buffer = new LinkedList<>();
	private final MongoCollection<Document> collection;

	public BulkInsert(MongoCollection<Document> collection) {
		this.collection = collection;
		setBufferWriteThreshold(Integer.parseInt(System.getProperty("MONGO_FLAME_BULK_WRITE_MIN_THRESHOLD", "1000")));
		MongoFlameDAO.logger.info("MONGO_FLAME_BULK_WRITE_WAITTIME = {}", waitTimeBeforeFlush);
		MongoFlameDAO.logger.info("MONGO_FLAME_BULK_WRITE_MIN_THRESHOLD = {}", bufferWriteThreshold);
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
			if (buffer.size() > 0) {
				collection.insertMany(buffer, insertManyOptions);
			}
		} catch (MongoBulkWriteException ex) {
			MongoFlameDAO.logger.debug(ex.getMessage()); 
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