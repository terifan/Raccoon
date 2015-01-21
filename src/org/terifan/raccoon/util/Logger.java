package org.terifan.raccoon.util;


public class Logger
{
	public static int LEVEL = 10;

	private static int mIndent;
	private String mName;


	public Logger(String aName)
	{
		mName = aName;
	}


	public Logger inc(Object... aMessage)
	{
		logImpl(2, aMessage);
		mIndent++;
		return this;
	}


	public Logger dec(Object... aMessage)
	{
		mIndent--;
		if (mIndent < 0)
		{
			mIndent = 0;
		}
		logImpl(2, aMessage);
		return this;
	}


	public Logger w(Object... aMessage)
	{
		logImpl(3, aMessage);
		return this;
	}


	public Logger e(Object... aMessage)
	{
		logImpl(4, aMessage);
		return this;
	}


	public Logger i(Object... aMessage)
	{
		logImpl(2, aMessage);
		return this;
	}


	public Logger d(Object... aMessage)
	{
		logImpl(1, aMessage);
		return this;
	}


	private void logImpl(int aLevel, Object... aMessage)
	{
		if (aLevel > LEVEL)
		{
			if (aMessage != null && aMessage.length > 0)
			{
				StringBuilder message = new StringBuilder();

				for (int i = 0; i < mIndent; i++)
				{
					message.append(".. ");
				}

				for (Object o : aMessage)
				{
					message.append(o);
				}

				StackTraceElement[] trace = Thread.currentThread().getStackTrace();
				String className = trace[3].getClassName();
				className = className.substring(className.lastIndexOf(".") + 1);
				String methodName = trace[3].getMethodName();
				String loggerName = mName == null ? "" : mName;

				System.out.printf("%-40s%-40s%s\n", className+"."+methodName, loggerName, message.toString());
			}
		}
	}
}
