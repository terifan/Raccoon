package org.terifan.raccoon;


class _Animal
{
	@Key String name;
	int number;
	double weight;
	String color;
	String family;
	boolean dangerous;


	public _Animal()
	{
	}


	public _Animal(String aName)
	{
		name = aName;
	}


	public _Animal(String aName, int aNumber)
	{
		name = aName;
		number = aNumber;
	}
}