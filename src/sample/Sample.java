package sample;

import java.io.File;
import org.terifan.v1.raccoon.Database;
import org.terifan.v1.raccoon.Key;
import org.terifan.v1.raccoon.OpenOption;
import org.terifan.v1.util.log.Log;

public class Sample 
{
	public static void main(String... args)
	{
		try
		{
			try (Database db = Database.open(new File("d:/log.db"), OpenOption.CREATE_NEW))
			{
				db.save(new Fruit("apple", 52));
				db.commit();
			}

			try (Database db = Database.open(new File("d:/log.db"), OpenOption.OPEN))
			{
				db.list(Fruit.class).stream().forEach(Log.out::println);
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
			this.name = aName;
			this.calories = aCalories;
		}


		@Override
		public String toString()
		{
			return name + ", " + calories;
		}
	}
}
