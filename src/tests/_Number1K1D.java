package tests;

import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;


public class _Number1K1D
{
	@Discriminator public boolean _odd;
	@Key public int _number;
	public String name;


	public _Number1K1D()
	{
	}


	public _Number1K1D(int aNumber)
	{
		_number = aNumber;
		_odd = (aNumber & 1) == 1;
	}


	public _Number1K1D(String aName, int aNumber)
	{
		name = aName;
		_number = aNumber;
		_odd = (aNumber & 1) == 1;
	}


	@Override
	public String toString()
	{
		return "number="+_number+", name="+name+", odd="+_odd;
	}
}
