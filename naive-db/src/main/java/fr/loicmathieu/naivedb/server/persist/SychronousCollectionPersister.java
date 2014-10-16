package fr.loicmathieu.naivedb.server.persist;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SychronousCollectionPersister implements CollectionPersister{
	private static final Logger LOG = LogManager.getLogger(SychronousCollectionPersister.class);
	private static final int BUFFER_LENGTH = 1024;
	private static final byte[] SEPARATOR = "|".getBytes();
	private static final byte[] EOL = "\n".getBytes();

	private final String collectionName;

	private File storageFile;
	private FileOutputStream persistantStorage;
	private FileChannel storageChannel;
	private ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_LENGTH);

	public SychronousCollectionPersister(String collectionName){
		this.collectionName = collectionName;
	}

	public void init() {
		LOG.info("[" + collectionName + "] - Initialize synchronous persistent storage");
		try {
			// open a file descriptor to persist the document
			storageFile = new File("D:/Temp/" + collectionName + ".dat");
			if (!storageFile.exists()) {
				storageFile.createNewFile();
			}
			persistantStorage = new FileOutputStream(storageFile);
			storageChannel = persistantStorage.getChannel();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	public void shutdown() {
		try {
			storageChannel.close();
			persistantStorage.close();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	public List<String> loadRawDocuments() {
		LOG.info("[" + collectionName + "] - Loading raw documents from synchronous persistent storage");
		//TODO set size in order to avoid too many re-sizing
		List<String> rawDocuments = new LinkedList<>();
		try(BufferedReader  reader = new BufferedReader(new FileReader(storageFile))){
			String line = reader.readLine();
			while(line != null){
				rawDocuments.add(line);
				line = reader.readLine();
			}
		}
		catch(IOException e){
			throw new RuntimeException(e);
		}

		return rawDocuments;
	}


	public void persistDocument(String id, String document, byte flag) {
		//TODO synchronize this or the data will be mixed in!
		//TODO byteBuffer is 1Ko, make multiple write to data more than 1Ko
		try {
			byteBuffer.put(id.getBytes());
			byteBuffer.put(SEPARATOR);
			byteBuffer.put(document.getBytes());
			byteBuffer.put(SEPARATOR);
			byteBuffer.put(flag);
			byteBuffer.put(EOL);
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

}
