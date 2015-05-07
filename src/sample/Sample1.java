package sample;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Random;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.OpenOption;


public class Sample1
{
	public static void main(String... args)
	{
		try
		{
			Random r = new Random(0);

			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.CREATE_NEW))
			{
				for (int j = 0; j < 100; j++)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < 10000; i++)
					{
						byte[] buf = new byte[10000 + r.nextInt(5000)];
						r.nextBytes(buf);
						db.save(new Item("item" + j + "_" + i), new ByteArrayInputStream(buf));
					}
					System.out.println(j+" "+(System.currentTimeMillis()-t));
					db.commit();
				}
			}

//			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.READ_ONLY))
//			{
//				for (int j = 0; j < 100; j++)
//				{
//					long t = System.currentTimeMillis();
//					for (int i = 0; i < 1000; i++)
//					{
//						byte[] buf = new byte[100000 + r.nextInt(50000)];
//						r.nextBytes(buf);
//
//						byte[] fetch = Streams.fetch(db.read(new Item("item" + j + "_" + i)));
//
//						if (!Arrays.equals(buf, fetch)) throw new IllegalStateException();
//					}
//					System.out.println(j+" "+(System.currentTimeMillis()-t));
//					db.commit();
//				}
//			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	static class Item
	{
		@Key String name;


		public Item()
		{
		}


		public Item(String aName)
		{
			this.name = aName;
		}


		@Override
		public String toString()
		{
			return name;
		}
	}
}
