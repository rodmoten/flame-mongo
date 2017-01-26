package com.i4hq.flame.mongo;

import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;

public class BulkUpdate implements BulkOperation {

	private long lastWrite = System.currentTimeMillis();
	private long waitTimeBeforeFlush;
	/**
	 * The minimum number of documents to keep in memory before sending to the server.
	 */
	private int bufferWriteThreshold;

	private List<UpdateOneModel<Document>> buffer = new LinkedList<>();
	private final MongoCollection<Document> collection;

	public BulkUpdate(MongoCollection<Document> collection) {
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
	public boolean update(final Bson filter, final Bson update) {
		buffer.add(new UpdateOneModel<Document>(filter, update, new UpdateOptions().upsert(true)));
		return updateMany();
	}

	private boolean updateMany() {
		if (buffer.size() > bufferWriteThreshold || System.currentTimeMillis() - lastWrite > waitTimeBeforeFlush){
			flush(); 
			buffer = new LinkedList<>();
			lastWrite = System.currentTimeMillis();
			return true;
		}
		return false;
	}


	/* (non-Javadoc)
	 * @see com.i4hq.flame.mongo.BulkOperation#flush()
	 */
	@Override
	public void flush() {
		try {		
			if (buffer.size() > 0) {
				collection.bulkWrite(buffer);
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
		waitTimeBeforeFlush = this.bufferWriteThreshold + (long) (this.bufferWriteThreshold * 1);
	}

	/* (non-Javadoc)
	 * @see com.i4hq.flame.mongo.BulkOperation#close()
	 */
	@Override
	public void close() {
		flush();

	}

}