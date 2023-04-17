package org.terifan.raccoon.util;

import java.util.Arrays;
import org.terifan.raccoon.io.util.Console.Color;


public class FormattedOutput
{
	private StringBuilder mBuffer;


	public FormattedOutput()
	{
		mBuffer = new StringBuilder();
	}


	@Override
	public String toString()
	{
		return mBuffer.toString();
	}


	public <T> void array(Consumer<T> aConsumer, Object... aValues)
	{
		array(aConsumer, (Iterable)Arrays.asList(aValues));
	}


	public <T> void array(Consumer<T> aConsumer, Iterable<T> aIterable)
	{
		mBuffer.append("[");
		aIterable.forEach(new java.util.function.Consumer<T>()
		{
			int len = mBuffer.length();
			@Override
			public void accept(T element)
			{
				if (mBuffer.length() > len)
				{
					mBuffer.append(",");
				}
				try
				{
					aConsumer.handle(element);
				}
				catch (Exception e)
				{
				}
			}
		});
		mBuffer.append("]");
	}


	public void append(Object aText)
	{
		mBuffer.append(aText);
	}


	public void append(String aFormat, Object aArgument)
	{
		switch (aFormat)
		{
			case "%s":
			case "\"%s\"":
			case "'%s'":
				aFormat = wrap(Color.BLUE, aFormat);
				break;
			case "%d":
			case "%f":
			case "#%d":
			case "0x%d":
				aFormat = wrap(Color.MAGENTA, aFormat);
				break;
			default:
				break;
		}

		mBuffer.append(String.format(aFormat, aArgument));
	}


	private static String wrap(Color aColor, String aFormat)
	{
		return aColor + aFormat + Color.RESET;
	}


	@FunctionalInterface
	public interface Consumer<E>
	{
		void handle(E aValue) throws Exception;
	}
}
