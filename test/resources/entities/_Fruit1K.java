package resources.entities;


public class _Fruit1K
{
	public String _name;
	public double calories;


	public _Fruit1K()
	{
	}


	public _Fruit1K(String aName)
	{
		_name = aName;
	}


	public _Fruit1K(String aName, double aCalories)
	{
		_name = aName;
		calories = aCalories;
	}


	@Override
	public String toString()
	{
		return "_Fruit1K{" + "_name=" + _name + ", calories=" + calories + '}';
	}
}