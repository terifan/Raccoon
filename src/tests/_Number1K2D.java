package tests;

import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;


public class _Number1K2D
{
	@Discriminator public int _number;
	@Discriminator public String _color;
	@Key public int _id;
	public String name;


	public _Number1K2D()
	{
	}


	public _Number1K2D(int aNumber, String aColor, int aId, String aName)
	{
		_number = aNumber;
		_color = aColor;
		_id = aId;
		name = aName;
	}


	@Override
	public String toString()
	{
		return "number="+_number+", name="+name+", id=" + _id + ", color=" + _color;
	}
}
