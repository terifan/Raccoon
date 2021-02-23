package test;

import java.awt.Dimension;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.TableParam;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;


public class Test1
{
	public static void main(String ... args)
	{
		try
		{
			long t = System.currentTimeMillis();
//			for (int test = 0; test < 10; test++)
			{
				MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

				try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW, CompressionParam.NO_COMPRESSION, new TableParam(1, 1), null))
				{
					for (int i = 0; i < 1000000; i++)
					{
						db.save(new MyEntity(i, Integer.toString(-i), "01234567890123456789"));
					}

					db.commit();
				}

				System.out.println(blockDevice.length()*blockDevice.getBlockSize()/1024.0/1024);

				try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN))
				{
					System.out.println(db.remove(new MyEntity(4, "-4")));

//					db.list(MyEntity.class).forEach(System.out::println);

					System.out.println("--------");
					System.out.println(db.get(new MyEntity(4, "-4")));
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
		@Key Integer id1;
		@Key String id2;
		String name;
		Dimension dim;

		public MyEntity()
		{
		}

		public MyEntity(Integer aId1, String aId2)
		{
			this.id1 = aId1;
			this.id2 = aId2;
		}

		public MyEntity(Integer aId1, String aId2, String aName)
		{
			this.id1 = aId1;
			this.id2 = aId2;
			this.name = aName;
			dim = new Dimension(aId1,aId1);
		}


		@Override
		public String toString()
		{
			return "MyEntity{" + "id1=" + id1 + ", id2=" + id2 + ", name=" + name + ", dim=" + dim + '}';
		}
	}
}