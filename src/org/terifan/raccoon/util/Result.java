package org.terifan.raccoon.util;


public class Result<E>
{
	private E mValue;


	public Result()
	{
	}


	public Result(E value)
	{
		mValue = value;
	}


	public E get()
	{
		return mValue;
	}


	public void set(E value)
	{
		mValue = value;
	}


	@Override
	public String toString()
	{
		return mValue == null ? "<null>" : mValue.toString();
	}
}
