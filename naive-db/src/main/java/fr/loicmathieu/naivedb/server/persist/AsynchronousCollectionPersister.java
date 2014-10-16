package fr.loicmathieu.naivedb.server.persist;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;



public class AsynchronousCollectionPersister extends SychronousCollectionPersister {
	private final ExecutorService execService;

	public AsynchronousCollectionPersister(String collectionName) {
		super(collectionName);

		this.execService = Executors.newSingleThreadExecutor();
	}

	@Override
	public void persistDocument(String id, String document, byte flag) {
		execService.submit(() -> super.persistDocument(id, document, flag));
	}

	@Override
	public void shutdown() {
		execService.shutdown();
		try {
			execService.awaitTermination(5, TimeUnit.MINUTES);//TODO herr
		}
		catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		super.shutdown();
	}
}
