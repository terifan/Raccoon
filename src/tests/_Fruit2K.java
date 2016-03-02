package tests;

import org.terifan.raccoon.Key;


public class _Fruit2K
{
	@Key public String _color;
	@Key public String _name;
	public String shape;
	public String taste;
	public int value;


	public _Fruit2K()
	{
	}


	public _Fruit2K(String aColor, String aName)
	{
		_color = aColor;
		_name = aName;
	}


	public _Fruit2K(String aColor, String aName, int aNumber)
	{
		_color = aColor;
		_name = aName;
		value = aNumber;
	}


	public _Fruit2K(String aColor, String aName, int aNumber, String aTaste)
	{
		_color = aColor;
		_name = aName;
		value = aNumber;
		taste = aTaste;
	}


	@Override
	public String toString()
	{
		return "_Fruit2K{" + "_color=" + _color + ", _name=" + _name + ", shape=" + shape + ", taste=" + taste + ", value=" + value + '}';
	}
}