package resources.entities;

import org.terifan.raccoon.Initializable;
import org.terifan.raccoon.annotations.Id;


public class _Fruit1KInit implements Initializable
{
	@Id public String _name;
	public double calories;
	public boolean initialized;


	public _Fruit1KInit()
	{
	}


	public _Fruit1KInit(String aName)
	{
		_name = aName;
	}


	public _Fruit1KInit(String aName, double aCalories)
	{
		_name = aName;
		calories = aCalories;
	}


	@Override
	public void initialize()
	{
		initialized = true;
	}


	@Override
	public String toString()
	{
		return "_Fruit1K{" + "_name=" + _name + ", calories=" + calories + '}';
	}
}