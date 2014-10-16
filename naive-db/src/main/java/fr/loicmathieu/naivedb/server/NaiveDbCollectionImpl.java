package fr.loicmathieu.naivedb.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fr.loicmathieu.naivedb.api.NaiveDbCollection;
import fr.loicmathieu.naivedb.api.NaiveDbIndex;
import fr.loicmathieu.naivedb.server.persist.AsynchronousCollectionPersister;
import fr.loicmathieu.naivedb.server.persist.CollectionPersister;
import fr.loicmathieu.naivedb.server.persist.SychronousCollectionPersister;
import gnu.trove.map.hash.THashMap;

public class NaiveDbCollectionImpl implements NaiveDbCollection {
	private static final Logger LOG = LogManager.getLogger(NaiveDbCollectionImpl.class);

	private static final byte SAVED_FLAG = 0;
	private static final byte UPDATED_FLAG = 1;
	private static final byte DELETED_FLAG = 2;

	private final String name;
	private final boolean persist;

	private Map<String, String> documents = new THashMap<>(100000);
	private Map<String, NaiveDbIndexImpl> indexes = new HashMap<>();
	private AtomicLong idGenerator = new AtomicLong();

	private CollectionPersister persister;

	private boolean closeInProgress = false;


	public NaiveDbCollectionImpl(String name) {
		this(name, false, false);
	}

	public NaiveDbCollectionImpl(String name, boolean persist) {
		this(name, persist, false);
	}


	public NaiveDbCollectionImpl(String name, boolean persist, boolean enableAsynch) {
		LOG.info("[" + name + "] - Initializing the collection with persistence " + (persist ? "enabled" : "disabled"));
		this.name = name;
		this.persist = persist;

		if (persist) {
			if(enableAsynch){
				persister = new AsynchronousCollectionPersister(name);
			}
			else {
				persister = new SychronousCollectionPersister(name);
			}
			persister.init();

			LOG.info("[" + name + "] - Loading previous data from persistent storage");
			long start = System.currentTimeMillis();
			List<String> rawDocument = persister.loadRawDocuments();
			addPersistedDocuments(rawDocument);
			LOG.info("[" + name + "] - Loading previous data from persistent storage done in " + (System.currentTimeMillis() - start) + " ms");
		}
		LOG.info("[" + name + "] - Collection successfully initialized");
	}


	public String save(String document) {
		//TODO add synchronization
		// generate ID
		String id = String.valueOf(idGenerator.getAndIncrement());

		// save the item
		internalSave(id, document);

		// presist if needed
		if (persist) {
			persister.persistDocument(id, document, SAVED_FLAG);
		}

		// return generated id
		return id;
	}


	public String save(String id, String document) {
		//TODO add synchronization
		boolean update = documents.containsKey(id);

		// save the item
		internalSave(id, document);

		// presist if needed
		if (persist) {
			byte flag = update ? UPDATED_FLAG : SAVED_FLAG;
			persister.persistDocument(id, document, flag);
		}

		// return generated id
		return id;
	}

	public void remove(String id){
		//TODO add synchronization
		String document = documents.remove(id);

		// de-index it
		if(document != null){
			for (NaiveDbIndexImpl index : indexes.values()) {
				index.deIndexDocument(id, document);
			}
		}

		// presist if needed the removal state
		if (persist) {
			persister.persistDocument(id, document, DELETED_FLAG);
		}

	}


	private void addPersistedDocuments(List<String> rawDocuments){
		LOG.info("[" + name + "] - Adding persisted documents to the collection");
		for(String line : rawDocuments){
			String[] item = line.split("\\|");
			if(item[2].equals(String.valueOf(DELETED_FLAG))){
				//remove the item, event needs to occurs in order
				remove(item[0]);
			}
			else {
				// save the item or update it!
				internalSave(item[0], item[1]);
			}
		}
	}


	private void internalSave(String id, String document) {
		// save the item
		documents.put(id, document);

		// index it
		for (NaiveDbIndexImpl index : indexes.values()) {
			index.indexDocument(id, document);
		}
	}


	public String get(String id) {
		return documents.get(id);
	}


	public List<String> find(String attribut, String value) {
		// find the right index
		NaiveDbIndexImpl index = indexes.get(attribut);
		if (index == null) {
			// TODO proper expection handling
			throw new RuntimeException("Index on " + attribut + " should exist in order to search on it!");
		}

		List<String> ids = index.get(value);
		if (ids != null) {
			List<String> docs = new ArrayList<>(ids.size());
			for (String id : ids) {
				docs.add(documents.get(id));
			}
			return docs;
		}

		return null;
	}


	public String getName() {
		return name;
	}

	public int getSize() {
		return documents.size();
	}


	public NaiveDbIndex ensureIndex(String attribute) {
		// synchronize this properly
		NaiveDbIndex index = indexes.get(attribute);
		if (index == null) {
			index = new NaiveDbIndexImpl(attribute);
			indexes.put(attribute, (NaiveDbIndexImpl) index);
		}

		//TODO, if previous data exist, need to index them

		return index;
	}


	public void close() {
		if(!closeInProgress){
			closeInProgress = true;

			LOG.info("[" + name + "] - Closing the collection");
			if(persist){
				//close the persistence if any
				persister.shutdown();
			}
			LOG.info("[" + name + "] - Collection closed");
		}

	}

}
