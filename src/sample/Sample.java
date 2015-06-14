package sample;

import java.io.File;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.util.Log;


public class Sample
{
	public static void main(String... args)
	{
		try
		{
			Log.LEVEL = 10;
			
			AccessCredentials accessCredentials = new AccessCredentials("password");

			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.CREATE_NEW, accessCredentials))
			{
				db.save(new Fruit("apple", 52.12));
				db.save(new Fruit("orange", 47.78));
				db.save(new Fruit("banana", 89.45));
				db.commit();
			}

			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.OPEN, accessCredentials))
			{
				db.list(Fruit.class).stream().forEach(System.out::println);
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	static class Fruit
	{
		@Key String name;
		double calories;


		public Fruit()
		{
		}


		public Fruit(String aName, double aCalories)
		{
			name = aName;
			calories = aCalories;
		}


		@Override
		public String toString()
		{
			return name + ", " + calories;
		}
	}
}