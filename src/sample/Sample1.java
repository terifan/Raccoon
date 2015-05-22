package sample;

import java.io.File;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.util.Log;


public class Sample1
{
	public static void main(String... args)
	{
		try
		{
			Log.LEVEL = 4;

			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.CREATE_NEW))
			{
				db.save(new Item("test1", new String(new byte[7000])));
				db.save(new Item("test2", new String(new byte[7000])));
				db.save(new Item("test3", new String(new byte[7000])));
				db.save(new Item("test4", new String(new byte[7000])));
				db.commit();
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	static class Item
	{
		@Key String name;
		String value;


		public Item()
		{
		}


		public Item(String aName, String aValue)
		{
			name = aName;
			value = aValue;
		}
	}
}
