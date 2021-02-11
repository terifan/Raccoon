package test;

import java.util.HashSet;
import java.util.Random;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.TableParam;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;


public class Test2
{
	public static void main(String ... args)
	{
		try
		{
			long t = System.currentTimeMillis();

			Random rnd = new Random();

			HashSet<Integer> existing = new HashSet<>();

			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

			for (int test = 0; test < 10; test++)
			{
				try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE, CompressionParam.NO_COMPRESSION, new TableParam(1, 1), null))
				{
					int insert = 0;
					int update = 0;
					int expectedInsert = 0;
					int expectedUpdate = 0;
					for (int i = 0; i < 100000; i++)
					{
						int k = rnd.nextInt(1000);
						if (existing.add(k)) expectedInsert++; else expectedUpdate++;
						if (db.save(new MyEntity(k, "01234567890123456789"))) insert++; else update++;
					}

					int delete = 0;
					int expectedDelete = 0;
					for (int i = 0; i < 10000; i++)
					{
						int k = rnd.nextInt(10000);
						if (existing.remove(k)) expectedDelete++;
						if (db.remove(new MyEntity(k))) delete++;
					}

					System.out.println(insert + " " + update + " " + delete + " --- " + expectedInsert + " "  + expectedUpdate + " " + expectedDelete);

					db.commit();
				}
			}

			System.out.println(System.currentTimeMillis()-t);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	static class MyEntity
	{
		@Key Integer id;
		String name;

		public MyEntity()
		{
		}

		public MyEntity(Integer aId)
		{
			this.id = aId;
		}

		public MyEntity(Integer aId, String aName)
		{
			this.id = aId;
			this.name = aName;
		}


		@Override
		public String toString()
		{
			return "MyEntity{" + "id=" + id + ", name=" + name + '}';
		}
	}
}
