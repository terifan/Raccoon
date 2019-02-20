package test;

import java.awt.Dimension;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.TableParam;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;


public class Test
{
	public static void main(String ... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

			try (Database db = new Database(blockDevice, OpenOption.CREATE_NEW, CompressionParam.NO_COMPRESSION, new TableParam(1, 1, 0)))
			{
				for (int i = 0; i < 1000; i++)
				{
					System.out.println("-------save--------");
					db.save(new MyEntity(i, "01234567890123456789"));
				}

				System.out.println("------commit-------");
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
		Dimension dim;

		public MyEntity()
		{
		}

		public MyEntity(int aId, String aName)
		{
			this.id = aId;
			this.name = aName;
			dim = new Dimension(aId,-aId);
		}

		@Override
		public String toString()
		{
			return "X{" + "id=" + id + ", name=" + name + '}'+dim;
		}
	}
}
