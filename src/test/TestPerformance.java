package test;

import java.io.File;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseBuilder;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.io.physical.FileBlockDevice;


public class TestPerformance
{
	public static void main(String... args)
	{
		try
		{
			for (int perLeaf = 1; perLeaf <= 8; perLeaf *= 2)
			{
				for (int perNode = 1; perNode <= 8; perNode *= 2)
				{
					for (int blockSize = 4096; blockSize <= 8192; blockSize *= 2)
					{
						long t = System.currentTimeMillis();

						try (Database db = new DatabaseBuilder(new FileBlockDevice(new File("d:\\test_" + blockSize + "_" + perNode + "_" + perLeaf + ".db"), blockSize, false)).setPagesPerNode(perNode).setPagesPerLeaf(perLeaf).setCompression(CompressionParam.NO_COMPRESSION).create())
						{
							for (int i = 0; i < 1000_000; i++)
							{
								db.save(new MyEntity(i, "item-" + i));
							}
							db.commit();
						}

						System.out.print(blockSize + " " + perNode + " " + perLeaf + " " + (System.currentTimeMillis() - t) + "\t");
					}

					System.out.println();
				}
			}

//			try (Database db = Database.open(new File("d:\\test.db"), OpenOption.OPEN, new AccessCredentials("password")))
//			{
//				db.list(MyEntity.class).forEach(System.out::println);
//			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	static class MyEntity
	{
		@Key int intkey1;
		long long1;
		double double1;
		String string1;
		String string2;
		boolean boolean1;
		int int1;
		int int2;
		int int3;


		public MyEntity()
		{
		}


		public MyEntity(int aId, String aName)
		{
			intkey1 = aId;
			long1 = System.nanoTime();
			double1 = Math.random();
			boolean1 = true;
			int1 = (int)long1;
			int2 = (int)(long1 >>> 32);
			int3 = 0;
			string1 = aName;
			string2 = double1 + string1 + long1;
		}
	}
}
