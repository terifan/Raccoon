package org.terifan.raccoon;

import org.terifan.raccoon.document.Document;


public class _Fruit extends Document
{
	public _Fruit()
	{
	}


	public _Fruit(String aName)
	{
		put("_id", aName);
	}


	public _Fruit(String aName, double aCalories)
	{
		put("_id", aName);
		put("calories", aCalories);
	}
}