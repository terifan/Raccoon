package resources.entities;

import org.terifan.raccoon.annotations.Discriminator;
import org.terifan.raccoon.annotations.Id;


public class _Number1K1D
{
	@Discriminator public boolean _odd;
	@Id public int _number;
	public String name;


	public _Number1K1D()
	{
	}


	public _Number1K1D(int aNumber)
	{
		_number = aNumber;
		_odd = (aNumber & 1) == 1;
	}


	public _Number1K1D(boolean aOdd)
	{
		_odd = aOdd;
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
		return "_Number1K1D{" + "_odd=" + _odd + ", _number=" + _number + ", name=" + name + '}';
	}
}
