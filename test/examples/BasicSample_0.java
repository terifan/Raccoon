package examples;

import java.io.IOException;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;
import org.testng.annotations.Test;
import org.terifan.raccoon.annotations.Id;


public class BasicSample_0
{
	@Test
	public void test() throws IOException
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW, new AccessCredentials("password")))
		{
			db.save(new MyEntity(1, "apple"));
			db.commit();
		}

		try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN, new AccessCredentials("password")))
		{
			db.list(MyEntity.class).forEach(System.out::println);
		}
	}

	static class MyEntity
	{
		@Id int id;
		String name;

		public MyEntity()
		{
		}

		public MyEntity(int aId, String aName)
		{
			this.id = aId;
			this.name = aName;
		}

		@Override
		public String toString()
		{
			return "X{" + "id=" + id + ", name=" + name + '}';
		}
	}
}