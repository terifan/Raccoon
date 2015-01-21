package sample;

import java.io.File;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.OpenOption;

public class Sample
{
	public static void main(String... args)
	{
		try
		{
			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.CREATE_NEW))
			{
				db.save(new Fruit("apple", 52.1));
				db.commit();
			}

			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.OPEN))
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
