package fr.loicmathieu.naivedb.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fr.loicmathieu.naivedb.api.NaiveDbIndex;

//TODO define if we need or not to extract the interface
public class NaiveDbIndexImpl implements NaiveDbIndex {
	private Map<String, List<String>> indexMap = new HashMap<>();
	private String attribute;

	public NaiveDbIndexImpl(String attribute){
		this.attribute = attribute;
	}

	public void indexDocument(String id, String document){
		//TODO find a way to do this without any copying and creation of object!
		//remove { and }
		String partial = document.substring(1, document.length() - 1);
		//split on ","
		String [] items = partial.split(",");

		//iterate over attribute and find the match!
		String key = null;
		for(int i = 0; i<items.length; i = i++){
			String[] subItem = items[i].split(":");
			String itemAttribute = subItem[0].substring(1, subItem[0].length() - 1);
			if(attribute.equals(itemAttribute)){
				key = subItem[1].substring(1, subItem[1].length() - 1);
				break;
			}
		}

		if(key != null){
			//if a key is found, we keep the id for indexing
			List<String> ids = indexMap.get(key);
			if(ids == null){
				//no existing ids for this key, create new List
				ids = new ArrayList<>();
			}
			ids.add(id);
			indexMap.put(key, ids);
		}
	}

	public List<String> get(String key){
		List<String> ids = indexMap.get(key);
		return ids;
	}

	public int getSize(){
		return indexMap.size();
	}
}
