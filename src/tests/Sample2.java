package tests;

import org.terifan.raccoon.Key;
import org.terifan.raccoon.serialization.FieldCategory;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.util.Log;


public class Sample2
{
	public static void main(String... args)
	{
		try
		{
			Fruit f = new Fruit("apple", 52.8);

			byte[] v1 = new Marshaller(Fruit.class).marshal(f, FieldCategory.KEY);
			byte[] v2 = new Marshaller(Fruit.class).marshal(f, FieldCategory.VALUE);

			Log.hexDump(v1);
			Log.hexDump(v2);
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
