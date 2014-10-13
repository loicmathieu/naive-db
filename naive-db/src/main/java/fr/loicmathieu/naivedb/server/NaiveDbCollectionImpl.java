package fr.loicmathieu.naivedb.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import fr.loicmathieu.naivedb.api.NaiveDbCollection;
import fr.loicmathieu.naivedb.api.NaiveDbIndex;

public class NaiveDbCollectionImpl implements NaiveDbCollection {

	private static final int BUFFER_LENGTH = 1024;

	private String name;
	private boolean persist;
	private FileOutputStream persistantStorage;
	private FileChannel storageChannel;
	private ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_LENGTH);

	private Map<String, String> documents = new HashMap<>();
	private Map<String, NaiveDbIndexImpl> indexes = new HashMap<>();
	private AtomicLong idGenerator = new AtomicLong();


	public NaiveDbCollectionImpl(String name) {
		this.name = name;
		this.persist = false;
	}


	public NaiveDbCollectionImpl(String name, boolean persist) {
		this.name = name;
		this.persist = persist;

		if (persist) {
			try {
				// open a file descriptor to persist the document
				File storageFile = new File("D:/Temp/naivedb.dat");
				if (!storageFile.exists()) {
					storageFile.createNewFile();
				}
				else {
					loadCollectionFromDisk(storageFile);
				}
				persistantStorage = new FileOutputStream(storageFile);
				storageChannel = persistantStorage.getChannel();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}


	private void loadCollectionFromDisk(File storageFile) throws IOException {
		try(BufferedReader  reader = new BufferedReader(new FileReader(storageFile))){
			String line = reader.readLine();
			while(line != null){
				String[] item = line.split("\\|");
				//TODO if already exist, value will be overridden, maybe compaction needs to occurs
				// save the item
				internalSave(item[0], item[1]);

				line = reader.readLine();
			}
		}
	}


	public String save(String document) {
		// generate ID
		String id = String.valueOf(idGenerator.getAndIncrement());

		// save the item
		internalSave(id, document);

		// presist if needed
		if (persist) {
			persistDocumentOnDiskViaChannel(id, document);
		}

		// return generated id
		return id;
	}

	private void internalSave(String id, String document) {
		// save the item
		documents.put(id, document);

		// index it
		for (NaiveDbIndexImpl index : indexes.values()) {
			index.indexDocument(id, document);
		}
	}


	private void persistDocumentOnDiskViaChannel(String id, String document) {
		//TODO synchronize this or the data will be mixed in!
		//TODO byteBuffer is 1Ko, make multiple write to data more than 1Ko
		try {
			byteBuffer.put(id.getBytes());
			byteBuffer.put("|".getBytes());
			byteBuffer.put(document.getBytes());
			byteBuffer.put("\n".getBytes());
			byteBuffer.flip();
			while(byteBuffer.hasRemaining()) {
				storageChannel.write(byteBuffer);
			}
			byteBuffer.clear();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
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


	public NaiveDbIndex ensureIndex(String attribute) {
		// synchronize this properly
		NaiveDbIndex index = indexes.get(attribute);
		if (index == null) {
			index = new NaiveDbIndexImpl(attribute);
			indexes.put(attribute, (NaiveDbIndexImpl) index);
		}

		return index;
	}

}
