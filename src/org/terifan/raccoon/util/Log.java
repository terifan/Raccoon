package org.terifan.raccoon.util;

import java.io.PrintStream;


public class Log
{
	public final static PrintStream out = System.out;

	public static int LEVEL = 2;

	private static int mIndent;


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
		logImpl(0, aMessage, aParams);
	}


	public static void e(String aMessage, Object... aParams)
	{
		logImpl(1, aMessage, aParams);
	}


	public static void w(String aMessage, Object... aParams)
	{
		logImpl(2, aMessage, aParams);
	}


	public static void i(String aMessage, Object... aParams)
	{
		logImpl(3, aMessage, aParams);
	}


	public static void v(String aMessage, Object... aParams)
	{
		logImpl(4, aMessage, aParams);
	}


	public static void d(String aMessage, Object... aParams)
	{
		logImpl(5, aMessage, aParams);
	}


	private static void logImpl(int aLevel, String aMessage, Object... aParams)
	{
		if (aLevel <= LEVEL && aMessage != null)
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

			String type;
			switch (aLevel)
			{
				case 0: type = "SEVERE"; break;
				case 1: type = "ERROR"; break;
				case 2: type = "WARN"; break;
				case 3: type = "INFO"; break;
				case 4: type = "VERBOSE"; break;
				case 5: type = "DEBUG"; break;
				default: type = ""; break;
			}

			System.out.printf("%-30s%-30s%-30s%-7s %s\n", loggerName, className, methodName, type, message.toString());
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

		int LW = 56;
		int MR = 100;

		StringBuilder binText = new StringBuilder("");
		StringBuilder hexText = new StringBuilder("");

		for (int row = 0, offset = 0; offset < aBuffer.length && row < MR; row++)
		{
			hexText.append(String.format("%04d: ", row * LW));

			int padding = 3 * LW + LW / 8;

			for (int i = 0; offset < aBuffer.length && i < LW; i++)
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
		return aValue == null ? null : new String(aValue);
	}
}
