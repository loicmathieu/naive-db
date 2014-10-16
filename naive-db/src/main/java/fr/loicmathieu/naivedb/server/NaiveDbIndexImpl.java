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
		//init index with first + 1 and last - 1 because of the {}!
		int firstIdx = 1;
		int cursorIdx = 1;
		int lastIdx = document.length() - 1;

		String key = null;
		boolean notFound = true;
		while(notFound && cursorIdx < lastIdx){
			cursorIdx++;
			if(document.charAt(cursorIdx) == ','){//TODO needs to allow ',' in the document!
				String item = document.substring(firstIdx, cursorIdx);
				key = checkItem(item);
				if(key != null){
					notFound = false;
				}
				else {
					//didn't found, move the startIdx
					firstIdx = cursorIdx + 1;
				}
			}

		}

		return key;
	}


	private String checkItem(String item) {
		//init index with first + 1 and last - 1 because of the {}!
		int attributeStartIdx = 0;
		while(item.charAt(attributeStartIdx) != '"'){//search for the '"' to avoid space
			attributeStartIdx++;
		}
		attributeStartIdx++; //move away the '"'

		int indexOf = item.indexOf(':');
		int attributeLastIdx = indexOf;
		while(item.charAt(attributeLastIdx) != '"'){//search for the '"' to avoid space
			attributeLastIdx--;
		}

		String itemAttribute = item.substring(attributeStartIdx, attributeLastIdx);
		if(attribute.equals(itemAttribute)){
			int keyStartIdx = indexOf;
			while(item.charAt(keyStartIdx) != '"'){
				keyStartIdx++;
			}
			keyStartIdx++;//move away the '"'

			int keyLastIdx = item.length() - 1;
			while(item.charAt(keyLastIdx) != '"'){
				keyLastIdx--;
			}
			return item.substring(keyStartIdx, keyLastIdx);
		}

		return null;
	}

}
