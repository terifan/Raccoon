package test;

import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;


public class Test
{
	public static void main(String ... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

			try (Database db = Database.open(blockDevice, OpenOption.CREATE_NEW, new AccessCredentials("password")))
			{
				db.save(new MyEntity(1, "apple"));
				db.commit();
			}

			try (Database db = Database.open(blockDevice, OpenOption.OPEN, new AccessCredentials("password")))
			{
				db.list(MyEntity.class).forEach(System.out::println);
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
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
