package test;

import java.io.File;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseBuilder;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.FileBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;


public class Test
{
	public static void main(String ... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

			try (Database db = new Database(blockDevice, OpenOption.CREATE_NEW))
			{
				db.save(new MyEntity(1, "apple"));
				db.commit();
			}

			try (Database db = new Database(blockDevice, OpenOption.OPEN))
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
