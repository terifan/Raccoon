package org.terifan.raccoon.util;

import java.io.PrintStream;
import java.nio.charset.Charset;
import org.terifan.raccoon.LogLevel;


public class Log
{
	private final static String [] DIGITS = {"0","1","2","3","4","5","6","7","8","9","A","B","C","D","E","F"};

	public final static PrintStream out = System.out;

	private static LogLevel mLevel = LogLevel.ERROR;
	private static int mIndent;


	public static void setLevel(LogLevel aLevel)
	{
		mLevel = aLevel;
	}


	public static void inc()
	{
		mIndent++;
	}


	public static void dec()
	{
		mIndent--;
	}


	public static void s(String aMessage, Object... aParams)
	{
		logImpl(LogLevel.FATAL, aMessage, aParams);
	}


	public static void e(String aMessage, Object... aParams)
	{
		logImpl(LogLevel.ERROR, aMessage, aParams);
	}


	public static void w(String aMessage, Object... aParams)
	{
		logImpl(LogLevel.WARN, aMessage, aParams);
	}


	public static void i(String aMessage, Object... aParams)
	{
		logImpl(LogLevel.INFO, aMessage, aParams);
	}


	public static void d(String aMessage, Object... aParams)
	{
		logImpl(LogLevel.DEBUG, aMessage, aParams);
	}


	private static void logImpl(LogLevel aLevel, String aMessage, Object... aParams)
	{
		if (aLevel.ordinal() >= mLevel.ordinal() && aMessage != null)
		{
			StringBuilder message = new StringBuilder();
			for (int i = 0; i < mIndent; i++)
			{
				message.append("... ");
			}
			message.append(String.format(aMessage, aParams));

			StackTraceElement[] trace = Thread.currentThread().getStackTrace();
			String className = trace[3].getClassName();
			className = className.substring(className.lastIndexOf('.') + 1);
			String methodName = trace[3].getMethodName();
			String loggerName = trace[3].getFileName() + ":" + trace[3].getLineNumber();

			System.out.printf("%-30s%-30s%-30s%-7s %s%n", loggerName, className, methodName, aLevel, message.toString());
		}
	}


	public static void hexDump(byte[] aBuffer)
	{
		if (aBuffer == null)
		{
			Log.out.println("hexdump: null");
			return;
		}
		if (aBuffer.length == 0)
		{
			Log.out.println("hexdump: empty");
			return;
		}

		int lw = 56;
		int mr = 10000;

		StringBuilder binText = new StringBuilder("");
		StringBuilder hexText = new StringBuilder("");

		for (int row = 0, offset = 0; row < mr && offset < aBuffer.length; row++)
		{
			hexText.append(String.format("%04d: ", row * lw));

			int padding = 3 * lw + lw / 8;

			for (int i = 0; offset < aBuffer.length && i < lw; i++)
			{
				int c = 0xff & aBuffer[offset++];

				hexText.append(String.format("%02x ", c));
				binText.append(Character.isISOControl(c) ? '.' : (char)c);
				padding -= 3;

				if ((i & 7) == 7)
				{
					hexText.append(" ");
					padding--;
				}
			}

			for (int i = 0; i < padding; i++)
			{
				hexText.append(" ");
			}

			Log.out.println(hexText.append(binText).toString());

			binText.setLength(0);
			hexText.setLength(0);
		}
	}


	public static String toString(byte[] aValue)
	{
		return aValue == null ? null : new String(aValue, Charset.defaultCharset());
	}
}
