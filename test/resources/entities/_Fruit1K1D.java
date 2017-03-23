package resources.entities;

import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;


public class _Fruit1K1D
{
	@Key public String _name;
	@Discriminator public String _color;
	public double calories;


	public _Fruit1K1D()
	{
	}


	public _Fruit1K1D(String aColor, String aName)
	{
		_name = aName;
		_color = aColor;
	}


	public _Fruit1K1D(String aColor, String aName, double aCalories)
	{
		_name = aName;
		_color = aColor;
		calories = aCalories;
	}


	@Override
	public String toString()
	{
		return "_Fruit1K1D{" + "_name=" + _name + ", _color=" + _color + ", calories=" + calories + '}';
	}
}