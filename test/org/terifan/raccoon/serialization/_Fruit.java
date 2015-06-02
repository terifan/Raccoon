package org.terifan.raccoon.serialization;

import org.terifan.raccoon.Key;


class _Fruit
{
	@Key String name;
	double calories;


	public _Fruit()
	{
	}


	public _Fruit(String aName, double aCalories)
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