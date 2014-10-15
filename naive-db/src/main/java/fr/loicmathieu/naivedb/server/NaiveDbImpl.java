package fr.loicmathieu.naivedb.server;

import java.util.HashMap;
import java.util.Map;

import fr.loicmathieu.naivedb.api.NaiveDb;
import fr.loicmathieu.naivedb.api.NaiveDbCollection;


public class NaiveDbImpl implements NaiveDb {
	private Map<String, NaiveDbCollection> collections = new HashMap<>();

	public NaiveDbCollection ensureCollection(String name) {
		return ensureCollection(name, false, false);
	}

	public NaiveDbCollection ensureCollection(String name, boolean persist) {
		return ensureCollection(name, persist, false);
	}

	public NaiveDbCollection ensureCollection(String name, boolean persist, boolean asynch) {
		if(! collections.containsKey(name)){
			createCollection(name, persist, asynch);
		}

		//TODO check that persist state is OK
		return collections.get(name);
	}

	private void createCollection(String name, boolean persist, boolean asynch) {
		//TODO add some synchronization somewhere to avoid double-creation ...
		NaiveDbCollection collection = new NaiveDbCollectionImpl(name, persist, asynch);
		collections.put(name, collection);
	}

	public void shutdown() {
		for(NaiveDbCollection collection : collections.values()){
			collection.close();
		}
	}

}
