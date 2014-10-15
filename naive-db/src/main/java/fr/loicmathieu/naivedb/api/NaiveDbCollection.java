package fr.loicmathieu.naivedb.api;

import java.util.List;


public interface NaiveDbCollection {
	//data method
	public String save(String document);
	public String save(String id, String document);
	public String get(String id);
	public void remove(String id);
	public List<String> find(String attribut, String value);

	//collection specific method
	public String getName();
	public NaiveDbIndex ensureIndex(String attribute);
	public int getSize();
	public void close();
}
