package resources.entities;

import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;


public class _Number1K1DS
{
	public @Discriminator Integer _disc;
	public @Key int _number;
	public String name;


	public _Number1K1DS()
	{
	}


	public _Number1K1DS(int aNumber, Integer aDisc)
	{
		_number = aNumber;
		_disc = aDisc;
	}


	public _Number1K1DS(Integer aDisc)
	{
		_disc = aDisc;
	}


	public _Number1K1DS(String aName, int aNumber, Integer aDisc)
	{
		name = aName;
		_number = aNumber;
		_disc = aDisc;
	}


	@Override
	public String toString()
	{
		return "_Number1K1D{" + "_disc=" + _disc + ", _number=" + _number + ", name=" + name + '}';
	}
}
