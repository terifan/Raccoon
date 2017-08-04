package test;

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
			System.setErr(System.out);

			for (int test = 1; test <= 1000; test++)
			{
				System.out.println("--------- "+test+" ----------");

				MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

				try (Database db = new Database(blockDevice, OpenOption.CREATE_NEW, CompressionParam.NO_COMPRESSION, new TableParam(1, 1, 0)))
				{
					for (int i = 0; i < test; i++)
					{
						System.out.println("------- save "+i+" --------");
						db.save(new MyEntity(""+i, "01234567890123456789012345678901234567890123456789"));
					}

					System.out.println("------ commit -------");
					db.commit();
				}

				try (Database db = new Database(blockDevice, OpenOption.OPEN))
				{
					if (test == 1000)
					{
						db.getTable(MyEntity.class).scan();
					}
					
					for (int i = 0; i < test; i++)
					{
						System.out.println("-------- get "+i+" --------");
						db.get(new MyEntity(""+i));
					}

//					db.list(MyEntity.class).forEach(System.out::println);
				}
				
				System.out.println();
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	static class MyEntity
	{
		@Key String id;
		String name;

		public MyEntity()
		{
		}

		public MyEntity(String aId)
		{
			this.id = aId;
		}

		public MyEntity(String aId, String aName)
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
