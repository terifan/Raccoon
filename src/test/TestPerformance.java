package test;

import java.io.File;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseBuilder;
import org.terifan.raccoon.io.physical.FileBlockDevice;
import org.terifan.raccoon.annotations.Id;


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

//			try (Database db = new Database(new File("d:\\test.db"), OpenOption.OPEN, new AccessCredentials("password")))
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
		@Id int intkey1;
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


		public int getIntkey1()
		{
			return intkey1;
		}


		public void setIntkey1(int aIntkey1)
		{
			this.intkey1 = aIntkey1;
		}


		public long getLong1()
		{
			return long1;
		}


		public void setLong1(long aLong1)
		{
			this.long1 = aLong1;
		}


		public double getDouble1()
		{
			return double1;
		}


		public void setDouble1(double aDouble1)
		{
			this.double1 = aDouble1;
		}


		public String getString1()
		{
			return string1;
		}


		public void setString1(String aString1)
		{
			this.string1 = aString1;
		}


		public String getString2()
		{
			return string2;
		}


		public void setString2(String aString2)
		{
			this.string2 = aString2;
		}


		public boolean isBoolean1()
		{
			return boolean1;
		}


		public void setBoolean1(boolean aBoolean1)
		{
			this.boolean1 = aBoolean1;
		}


		public int getInt1()
		{
			return int1;
		}


		public void setInt1(int aInt1)
		{
			this.int1 = aInt1;
		}


		public int getInt2()
		{
			return int2;
		}


		public void setInt2(int aInt2)
		{
			this.int2 = aInt2;
		}


		public int getInt3()
		{
			return int3;
		}


		public void setInt3(int aInt3)
		{
			this.int3 = aInt3;
		}
	}
}
