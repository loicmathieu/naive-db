package fr.loicmathieu.naivedb.server;

import org.junit.Assert;
import org.junit.Test;

import fr.loicmathieu.naivedb.api.NaiveDb;
import fr.loicmathieu.naivedb.api.NaiveDbCollection;
import fr.loicmathieu.naivedb.api.NaiveDbIndex;


public class NaiveDbImplBasicTest {
	@Test
	public void test(){
		NaiveDb db = new NaiveDbImpl();
		NaiveDbCollection col = db.ensureCollection("person");

		//save some stuff
		String id = col.save("{\"firstname\" : \"Jon\", \"lastname\" : \"Doe\", \"gender\" : \"male\" }");
		col.save("{\"firstname\" : \"Jane\", \"lastname\" : \"Doe\", \"gender\" : \"female\" }");
		col.save("{\"firstname\" : \"Elvis\", \"lastname\" : \"Presley\", \"gender\" : \"male\" }");
		col.save("{\"firstname\" : \"Elvira\", \"lastname\" : \"Demonia\", \"gender\" : \"female\" }");
		Assert.assertEquals(4, col.getSize());
		Assert.assertEquals(col.get(id), "{\"firstname\" : \"Jon\", \"lastname\" : \"Doe\", \"gender\" : \"male\" }");

		//update the first document
		String modifiedId = col.save(id, "{\"firstname\" : \"Jonny\", \"lastname\" : \"Doe\", \"gender\" : \"male\" }");
		Assert.assertEquals(modifiedId, id);
		Assert.assertEquals(col.get(id), "{\"firstname\" : \"Jonny\", \"lastname\" : \"Doe\", \"gender\" : \"male\" }");
		Assert.assertEquals(4, col.getSize());

		//finally delete it
		col.remove(id);
		Assert.assertEquals(3, col.getSize());

		//shutdown the db
		db.shutdown();
	}

	@Test
	public void testWithIndex(){
		NaiveDb db = new NaiveDbImpl();
		NaiveDbCollection col = db.ensureCollection("person");
		NaiveDbIndex index = col.ensureIndex("lastname");

		//save some stuff
		String id = col.save("{\"firstname\" : \"Jon\", \"lastname\" : \"Doe\", \"gender\" : \"male\" }");
		col.save("{\"firstname\" : \"Jane\", \"lastname\" : \"Doe\", \"gender\" : \"female\" }");
		String otherId = col.save("{\"firstname\" : \"Elvis\", \"lastname\" : \"Presley\", \"gender\" : \"male\" }");
		col.save("{\"firstname\" : \"Elvira\", \"lastname\" : \"Demonia\", \"gender\" : \"female\" }");
		Assert.assertEquals(4, col.getSize());
		Assert.assertEquals(3, index.getSize());
		Assert.assertEquals(col.get(id), "{\"firstname\" : \"Jon\", \"lastname\" : \"Doe\", \"gender\" : \"male\" }");

		//update the first document
		String modifiedId = col.save(id, "{\"firstname\" : \"Jonny\", \"lastname\" : \"Doe\", \"gender\" : \"male\" }");
		Assert.assertEquals(modifiedId, id);
		Assert.assertEquals(col.get(id), "{\"firstname\" : \"Jonny\", \"lastname\" : \"Doe\", \"gender\" : \"male\" }");
		Assert.assertEquals(3, index.getSize());
		Assert.assertEquals(4, col.getSize());

		//finally delete it
		col.remove(id);
		Assert.assertEquals(3, index.getSize());
		Assert.assertEquals(3, col.getSize());

		//and delete a record that have an index
		col.remove(otherId);
		Assert.assertEquals(2, index.getSize());
		Assert.assertEquals(2, col.getSize());

		//shutdown the db
		db.shutdown();
	}
}
