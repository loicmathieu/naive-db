package fr.loicmathieu.naivedb.server.persist;

import java.util.List;


public interface CollectionPersister {
	public void init();
	public void shutdown();
	public List<String> loadRawDocuments();
	public void persistDocument(String id, String document, byte flag);
}
