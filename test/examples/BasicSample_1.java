package examples;

import java.io.IOException;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.testng.annotations.Test;


public class BasicSample_1
{
	@Test
	public void test() throws IOException
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(4096);

		try (Database db = Database.open(blockDevice, OpenOption.CREATE_NEW))
		{
			db.save(new MyEntity(1, "apple"));
			db.save(new MyEntity(2, "banana"));
			db.commit();
		}

		try (Database db = Database.open(blockDevice, OpenOption.OPEN))
		{
			db.list(MyEntity.class).forEach(System.out::println);
		}
	}

	static class MyEntity
	{
		@Key int id;
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