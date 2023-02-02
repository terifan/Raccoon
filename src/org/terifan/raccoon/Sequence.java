package org.terifan.raccoon;

import java.util.Iterator;


public abstract class Sequence<T> implements Iterator<T>
{
	private T mValue;


	protected abstract T advance() throws Exception;


	@Override
	public boolean hasNext()
	{
		if (mValue == null)
		{
			try
			{
				mValue = advance();
			}
			catch (Exception e)
			{
				throw new IllegalStateException(e);
			}
		}

		return mValue != null;
	}


	@Override
	public T next()
	{
		T tmp = mValue;
		mValue = null;
		return tmp;
	}
}
