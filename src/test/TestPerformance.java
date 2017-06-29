package test;

import java.io.File;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.secure.AccessCredentials;


public class TestPerformance
{
	public static void main(String ... args)
	{
		try
		{
			try (Database db = Database.open(new File("d:\\test-"+System.currentTimeMillis()+".db"), OpenOption.CREATE_NEW))
			{
				for (int i = 0; i < 1000_000; i++)
				{
					db.save(new MyEntity(i, "item-" + i));
				}
				db.commit();
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
