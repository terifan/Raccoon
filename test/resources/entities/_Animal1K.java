package resources.entities;

import org.terifan.raccoon.annotations.Id;


public class _Animal1K
{
	@Id public String _name;
	public int number;
	public double weight;
	public String color;
	public String family;
	public boolean dangerous;


	public _Animal1K()
	{
	}


	public _Animal1K(String aName)
	{
		_name = aName;
	}


	public _Animal1K(String aName, int aNumber)
	{
		_name = aName;
		number = aNumber;
	}
}