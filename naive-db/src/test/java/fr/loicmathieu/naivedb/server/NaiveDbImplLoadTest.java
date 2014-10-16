package fr.loicmathieu.naivedb.server;

import java.io.File;
import java.util.List;

import org.junit.Test;

import fr.loicmathieu.naivedb.api.NaiveDb;
import fr.loicmathieu.naivedb.api.NaiveDbCollection;
import fr.loicmathieu.naivedb.api.NaiveDbIndex;


public class NaiveDbImplLoadTest {
	private static final int NB_PERSON = 1000000;

	/**
	 * RUN 1 :
	 * Save all elements in : 3758
	 *     	=> 333333.3333333333 tx/s
	 * Read all elements in : 1194
	 * 		=> 1000000.0 tx/s
	 *
	 * RUN 2 : Trove!
	 * Save all elements in : 2744
	 * 	=> 500000.0 tx/s
	 * Read all elements in : 702
	 * 	=> Infinity tx/s
	 */
	@Test
	public void test() {
		NaiveDb db = new NaiveDbImpl();
		NaiveDbCollection col = db.ensureCollection("person");

		//inserting all elems to db
		long start = System.currentTimeMillis();
		for(int i=0; i<NB_PERSON; i++){
			col.save("{\"firstname\":\"john" + i + "\",\"lastname\":\"doe\",\"gender\":\"male\",\"age\":\"22\"}");
		}
		long elapsed = System.currentTimeMillis() - start;
		System.out.println("Save all elements in : " + elapsed);
		double tx = NB_PERSON / (double) (elapsed / 1000);
		System.out.println("\t=> " + tx + " tx/s");

		//reading them all!
		String person = "";
		start = System.currentTimeMillis();
		for(int i=0; i<NB_PERSON; i++){
			person = col.get(String.valueOf(i));
		}
		System.out.println("Person blackhole : " + person);
		elapsed = System.currentTimeMillis() - start;
		System.out.println("Read all elements in : " + elapsed);
		tx = NB_PERSON / (double) (elapsed / 1000);
		System.out.println("\t=> " + tx + " tx/s");

		//shutdown the db
		db.shutdown();
	}


	/**
	 * RUN 1
	 * Save all elements in : 7665
	 * 	=> 142857.14285714287 tx/s
	 * Read all elements in : 159
	 *	=> Infinity tx/s
	 * 	Read all elements from index in : 1495
	 * 	=> 1000000.0 tx/s
	 *
	 * RUN 2 : Trove
	 * Save all elements in : 6431
	 * 	=> 166666.66666666666 tx/s
	 * Read all elements in : 135
	 * 	=> Infinity tx/s
	 * Read all elements from index in : 409
	 *	=> Infinity tx/s
	 *
	 * RUN 3 : better indexing techincs
	 * Save all elements in : 5522
	 * 	=> 200000.0 tx/s
	 * Read all elements in : 1834
	 * 	=> 1000000.0 tx/s
	 *	=> Infinity tx/s
	 */
	@Test
	public void testWithIndex() {
		NaiveDb db = new NaiveDbImpl();
		NaiveDbCollection col = db.ensureCollection("person");
		NaiveDbIndex index = col.ensureIndex("firstname");

		//inserting all elems to db
		long start = System.currentTimeMillis();
		for(int i=0; i<NB_PERSON; i++){
			col.save("{\"firstname\":\"john" + i + "\",\"lastname\":\"doe\",\"gender\":\"male\",\"age\":\"22\"}");
		}
		long elapsed = System.currentTimeMillis() - start;
		System.out.println("Save all elements in : " + elapsed);
		double tx = NB_PERSON / (double) (elapsed / 1000);
		System.out.println("\t=> " + tx + " tx/s");

		//checking the index
		System.out.println("Indexed : " + index.getSize() + " elements");

		//reading them all!
		String person = "";
		start = System.currentTimeMillis();
		for(int i=0; i<NB_PERSON; i++){
			person = col.get(String.valueOf(i));
		}
		System.out.println("Person blackhole : " + person);
		elapsed = System.currentTimeMillis() - start;
		System.out.println("Read all elements in : " + elapsed);
		tx = NB_PERSON / (double) (elapsed / 1000);
		System.out.println("\t=> " + tx + " tx/s");

		//reading them all from the index!
		List<String> persons = null;
		start = System.currentTimeMillis();
		for(int i=0; i<NB_PERSON; i++){
			persons = col.find("firstname", "john" + String.valueOf(i));
		}
		System.out.println("Person blackhole : " + persons);
		elapsed = System.currentTimeMillis() - start;
		System.out.println("Read all elements from index in : " + elapsed);
		tx = NB_PERSON / (double) (elapsed / 1000);
		System.out.println("\t=> " + tx + " tx/s");

		//shutdown the db
		db.shutdown();
	}

	/**
	 * RUN 1:
	 * Save all elements in : 9579
	 * 	=> 111111.11111111111 tx/s
	 * Read all elements in : 207
	 * 	=> Infinity tx/s
	 *
	 * RUN 2 - NIO:
	 * Save all elements in : 7432
	 * 	=> 142857.14285714287 tx/s
	 * Read all elements in : 1257
	 *	=> 1000000.0 tx/s
	 */
	@Test
	public void testWithStorage() {
		File storageFile = new File("D:/Temp/person.dat");
		if(storageFile.exists()){
			storageFile.delete();
		}

		NaiveDb db = new NaiveDbImpl();
		NaiveDbCollection col = db.ensureCollection("person", true);

		//inserting all elems to db
		long start = System.currentTimeMillis();
		for(int i=0; i<NB_PERSON; i++){
			col.save("{\"firstname\":\"john" + i + "\",\"lastname\":\"doe\"}");
		}
		long elapsed = System.currentTimeMillis() - start;
		System.out.println("Save all elements in : " + elapsed);
		double tx = NB_PERSON / (double) (elapsed / 1000);
		System.out.println("\t=> " + tx + " tx/s");

		//reading them all!
		String person = "";
		start = System.currentTimeMillis();
		for(int i=0; i<NB_PERSON; i++){
			person = col.get(String.valueOf(i));
		}
		System.out.println("Person blackhole : " + person);
		elapsed = System.currentTimeMillis() - start;
		System.out.println("Read all elements in : " + elapsed);
		tx = NB_PERSON / (double) (elapsed / 1000);
		System.out.println("\t=> " + tx + " tx/s");

		//shutdown the db
		db.shutdown();
	}


	/**
	 * RUN 1:
	 * Save all elements in : 9579
	 * 	=> 111111.11111111111 tx/s
	 * Read all elements in : 207
	 * 	=> Infinity tx/s
	 */
	@Test
	public void testWithAsynchStorage() {
		File storageFile = new File("D:/Temp/person.dat");
		if(storageFile.exists()){
			storageFile.delete();
		}

		NaiveDb db = new NaiveDbImpl();
		NaiveDbCollection col = db.ensureCollection("person", true, true);

		//inserting all elems to db
		long start = System.currentTimeMillis();
		for(int i=0; i<NB_PERSON; i++){
			col.save("{\"firstname\":\"john" + i + "\",\"lastname\":\"doe\"}");
		}
		long elapsed = System.currentTimeMillis() - start;
		System.out.println("Save all elements in : " + elapsed);
		double tx = NB_PERSON / (double) (elapsed / 1000);
		System.out.println("\t=> " + tx + " tx/s");

		//reading them all!
		String person = "";
		start = System.currentTimeMillis();
		for(int i=0; i<NB_PERSON; i++){
			person = col.get(String.valueOf(i));
		}
		System.out.println("Person blackhole : " + person);
		elapsed = System.currentTimeMillis() - start;
		System.out.println("Read all elements in : " + elapsed);
		tx = NB_PERSON / (double) (elapsed / 1000);
		System.out.println("\t=> " + tx + " tx/s");

		//shutdown the db
		db.shutdown();
	}


	/**
	 * Loading previous data from disk done in 4190 ms
	 */
	@Test
	public void testReloadFromDisk() {
		NaiveDb db = new NaiveDbImpl();
		NaiveDbCollection col = db.ensureCollection("person", true);

		//reading them all!
		String person = "";
		long start = System.currentTimeMillis();
		for(int i=0; i<NB_PERSON; i++){
			person = col.get(String.valueOf(i));
		}
		System.out.println("Person blackhole : " + person);
		long elapsed = System.currentTimeMillis() - start;
		System.out.println("Read all elements in : " + elapsed);
		double tx = NB_PERSON / (double) (elapsed / 1000);
		System.out.println("\t=> " + tx + " tx/s");

		//shutdown the db
		db.shutdown();
	}

}
