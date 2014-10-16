package fr.loicmathieu.naivedb.server;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import fr.loicmathieu.naivedb.api.NaiveDbIndex;
import gnu.trove.map.hash.THashMap;

public class NaiveDbIndexImpl implements NaiveDbIndex {
	private Map<String, List<String>> indexMap = new THashMap<>(100000);
	private final String attribute;


	public NaiveDbIndexImpl(String attribute){
		this.attribute = attribute;
	}


	public void indexDocument(String id, String document){
		String key = extractKey(document);

		if(key != null){
			//if a key is found, we keep the id for indexing
			List<String> ids = indexMap.get(key);
			if(ids == null){
				//no existing ids for this key, create new List
				ids = new LinkedList<>();
			}
			ids.add(id);
			indexMap.put(key, ids);
		}
	}


	public void deIndexDocument(String id, String document){
		String key = extractKey(document);

		if(key != null){
			//if a key is found, we remove the id from the index
			List<String> ids = indexMap.get(key);
			ids.remove(id);
			if(ids.isEmpty()){
				indexMap.remove(key);
			}
			else {
				indexMap.put(key, ids);
			}
		}
	}


	public List<String> get(String key){
		List<String> ids = indexMap.get(key);
		return ids;
	}


	public int getSize(){
		return indexMap.size();
	}


	private String extractKey(String document) {
		//TODO find a way to do this without any copying and creation of object!
		//remove { and }
		String partial = document.substring(1, document.length() - 1);
		//split on ","
		String [] items = partial.split(",");

		//iterate over attribute and find the match!
		String key = null;
		for (String item : items) {
			//split on : to split attribute and value
			String[] subItem = item.split(":");
			String itemAttribute = subItem[0].trim();
			itemAttribute = itemAttribute.substring(1, itemAttribute.length() - 1);
			if(attribute.equals(itemAttribute)){
				key = subItem[1].trim();
				key = key.substring(1, key.length() - 1);
				break;
			}
		}

		return key;
	}

}
