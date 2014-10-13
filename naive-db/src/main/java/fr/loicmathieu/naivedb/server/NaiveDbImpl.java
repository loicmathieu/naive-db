package fr.loicmathieu.naivedb.server;

import java.util.HashMap;
import java.util.Map;

import fr.loicmathieu.naivedb.api.NaiveDb;
import fr.loicmathieu.naivedb.api.NaiveDbCollection;


public class NaiveDbImpl implements NaiveDb {
	private Map<String, NaiveDbCollection> collections = new HashMap<>();

	public NaiveDbCollection ensureCollection(String name) {
		if(! collections.containsKey(name)){
			createCollection(name, false);
		}

		//TODO check that persist state is OK
		return collections.get(name);
	}

	public NaiveDbCollection ensureCollection(String name, boolean persist) {
		if(! collections.containsKey(name)){
			createCollection(name, persist);
		}

		//TODO check that persist state is OK
		return collections.get(name);
	}

	private void createCollection(String name, boolean persist) {
		//TODO add some synchronization somewhere to avoid double-creation ...
		NaiveDbCollection collection = new NaiveDbCollectionImpl(name, persist);
		collections.put(name, collection);
	}

}
