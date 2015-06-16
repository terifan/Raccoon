package tests;

import org.terifan.raccoon.Key;


public class _Fruit1K
{
	@Key public String _name;
	public double calories;


	public _Fruit1K()
	{
	}


	public _Fruit1K(String aName, double aCalories)
	{
		_name = aName;
		calories = aCalories;
	}


	@Override
	public String toString()
	{
		return _name + ", " + calories;
	}
}