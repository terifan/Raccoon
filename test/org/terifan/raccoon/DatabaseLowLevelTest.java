package org.terifan.raccoon;

import java.util.Random;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;
import static org.testng.Assert.*;


public class DatabaseLowLevelTest
{
	public DatabaseLowLevelTest()
	{
		Log.LEVEL = 10;
	}


	@Test
	public void testSingleTableInsertTiny() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			database.save(new _Fruit("red", "apple1", 123, rnd(1000)));
			database.save(new _Fruit("red", "apple2", 123, rnd(1000)));
			database.save(new _Fruit("red", "apple3", 123, rnd(1000)));
			database.save(new _Fruit("red", "apple4", 123, rnd(1000)));
			database.save(new _Fruit("red", "apple5", 123, rnd(1000)));
			database.save(new _Fruit("red", "apple6", 123, rnd(1000)));
			database.save(new _Fruit("red", "apple7", 123, rnd(1000)));
			database.save(new _Fruit("red", "apple8", 123, rnd(1000)));
			database.save(new _Fruit("red", "apple9", 123, rnd(1000)));
			database.commit();

//			assertNull(database.integrityCheck());
		}

//		for (byte[] buf : device.getStorage().values())
//		{
//			Log.hexDump(buf);
//		}

		assertTrue(true);
	}

	private String rnd(int len)
	{
		char[] c = new char[len];
		Random rnd = new Random();
		for (int i = 0; i < len; i++)
		{
			c[i] = (char)('a' + rnd.nextInt(26));
		}
		return new String(c);
	}
}
