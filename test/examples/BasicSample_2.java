package examples;

import resources.entities._Object1K;
import resources.entities._Person1K;
import resources.entities._Fruit2K;
import resources.entities._Fruit1K;
import java.io.IOException;
import java.util.GregorianCalendar;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.secure.AccessCredentials;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.testng.annotations.Test;


public class BasicSample_2
{
	@Test
	public void test() throws IOException
	{
		AccessCredentials accessCredentials = new AccessCredentials("my password").setIterationCount(100);

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		try (Database db = Database.open(blockDevice, OpenOption.CREATE_NEW, accessCredentials))
		{
			db.save(new _Fruit1K("apple", 52.12));
			db.save(new _Fruit1K("orange", 47.78));
			db.save(new _Fruit1K("banana", 89.45));
			db.save(new _Fruit2K("yellow", "lemmon", 89, "bitter"));
			db.save(new _Person1K("Stig", "Helmer", "Stiggatan 10", "41478", "Göteborg", "Sverige", "+46311694797", "+46701649947", "Global Company", 182, 87));
			db.save(new _Person1K("Jane", "Doe", "Some Street 7", "649412", "Somecity", "US", null, null, "Other Company", 167, 59));
			db.save(new _Object1K("a gregorian calendar", new GregorianCalendar()));
			db.commit();
		}

		try (Database db = Database.open(blockDevice, OpenOption.OPEN, accessCredentials))
		{
			System.out.println("fruits:");
			db.list(_Fruit1K.class).forEach(System.out::println);

			System.out.println("people:");
			db.list(_Person1K.class).forEach(System.out::println);
		}

		blockDevice.dump();
	}
}