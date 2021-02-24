package resources.entities;

import org.terifan.raccoon.annotations.Discriminator;
import org.terifan.raccoon.annotations.Id;


public class _Fruit1K1D
{
	@Id public String _name;
	@Discriminator public String _color;
	public double calories;


	public _Fruit1K1D()
	{
	}


	public _Fruit1K1D(String aColor)
	{
		_color = aColor;
	}


	public _Fruit1K1D(String aColor, String aName, double aCalories)
	{
		_color = aColor;
		_name = aName;
		calories = aCalories;
	}


	@Override
	public String toString()
	{
		return "_Fruit1K1D{" + "_name=" + _name + ", _color=" + _color + ", calories=" + calories + '}';
	}
}