package org.terifan.raccoon;


class _DiscriminatedNumber
{
	@Discriminator boolean odd;
	@Key int number;
	String name;


	public _DiscriminatedNumber()
	{
	}


	public _DiscriminatedNumber(int aNumber)
	{
		number = aNumber;
		odd = (aNumber & 1) == 1;
	}


	public _DiscriminatedNumber(String aName, int aNumber)
	{
		name = aName;
		number = aNumber;
		odd = (aNumber & 1) == 1;
	}


	@Override
	public String toString()
	{
		return "number="+number+", name="+name+", odd="+odd;
	}
}
