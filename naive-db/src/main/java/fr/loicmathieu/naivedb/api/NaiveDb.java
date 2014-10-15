package fr.loicmathieu.naivedb.api;


public interface NaiveDb {
	public NaiveDbCollection ensureCollection(String name);
	public NaiveDbCollection ensureCollection(String name, boolean persist);
	public NaiveDbCollection ensureCollection(String name, boolean persist, boolean asynch);

	public void shutdown();
}
