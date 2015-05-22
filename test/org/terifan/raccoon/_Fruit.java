package org.terifan.raccoon;


class _Fruit
{
	@Key String name;
	@Key String color;
	String shape;
	String taste;
	int value;


	public _Fruit()
	{
	}


	public _Fruit(String aColor, String aName)
	{
		color = aColor;
		name = aName;
	}


	public _Fruit(String aColor, String aName, int aNumber)
	{
		color = aColor;
		name = aName;
		value = aNumber;
	}


	public _Fruit(String aColor, String aName, int aNumber, String aTaste)
	{
		color = aColor;
		name = aName;
		value = aNumber;
		taste = aTaste;
	}
}