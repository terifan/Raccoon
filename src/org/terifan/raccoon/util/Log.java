package org.terifan.raccoon.util;

import java.io.PrintStream;
import java.nio.charset.Charset;
import org.terifan.raccoon.LogLevel;


public class Log
{
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
			message.append(Console.format(aMessage, aParams));

			StackTraceElement[] trace = Thread.currentThread().getStackTrace();
			String className = trace[3].getClassName();
			className = className.substring(className.lastIndexOf('.') + 1);
			String methodName = trace[3].getMethodName();
			String loggerName = trace[3].getFileName() + ":" + trace[3].getLineNumber();

			System.out.printf("%-30s  %-30s  %-30s  %-7s  %s%n", shrink(loggerName,30), shrink(className,30), shrink(methodName,30), aLevel, message.toString());
		}
	}


	private static String shrink(String aText, int aLength)
	{
		return aText.length() <= aLength ? aText : aText.substring(0, aLength / 2 - 2) + "[..]" + aText.substring(aText.length() - aLength / 2 + 2);
	}


	public static void hexDump(byte[] aBuffer)
	{
		hexDump(aBuffer, 48);
	}


//	public static void hexDump(byte[] aBuffer, int aWidth)
//	{
//		if (aBuffer == null)
//		{
//			Log.out.println("hexdump: null");
//			return;
//		}
//		if (aBuffer.length == 0)
//		{
//			Log.out.println("hexdump: empty");
//			return;
//		}
//
//		int lw = aWidth;
//		int mr = 10000;
//
//		StringBuilder binText = new StringBuilder("");
//		StringBuilder hexText = new StringBuilder("");
//
//		for (int row = 0, offset = 0; row < mr && offset < aBuffer.length; row++)
//		{
//			hexText.append(String.format("%04d: ", row * lw));
//
//			int padding = 3 * lw + lw / 8;
//
//			for (int i = 0; offset < aBuffer.length && i < lw; i++)
//			{
//				int c = 0xff & aBuffer[offset++];
//
//				hexText.append(String.format("%02x ", c));
//				binText.append(Character.isISOControl(c) ? '.' : (char)c);
//				padding -= 3;
//
//				if ((i & 7) == 7)
//				{
//					hexText.append(" ");
//					padding--;
//				}
//			}
//
//			for (int i = 0; i < padding; i++)
//			{
//				hexText.append(" ");
//			}
//
//			Log.out.println(hexText.append(binText).toString());
//
//			binText.setLength(0);
//			hexText.setLength(0);
//		}
//	}


	public static void hexDump(byte[] aBuffer, int LW)
	{
		int MR = 1000;

		StringBuilder binText = new StringBuilder("");
		StringBuilder hexText = new StringBuilder("");

		for (int row = 0, offset = 0; offset < aBuffer.length && row < MR; row++)
		{
			hexText.append(String.format("%04d: ", row * LW));

			int padding = 3 * LW + LW / 8;

			for (int i = 0; offset < aBuffer.length && i < LW; i++)
			{
				int c = 0xff & aBuffer[offset++];

				if (!(c < ' ' || c >= 128))
				{
					hexText.append(Console.Color.BLUE);
					binText.append(Console.Color.BLUE);
				}
				if (c >= '0' && c <= '9')
				{
					hexText.append(Console.Color.MAGENTA);
					binText.append(Console.Color.MAGENTA);
				}

				hexText.append(String.format("%02x ", c));
				binText.append(Character.isISOControl(c) ? '.' : (char)c);

				if (c < ' ' || c >= 128 || c >= '0' && c <= '9')
				{
					hexText.append(Console.Color.RESET);
					binText.append(Console.Color.RESET);
				}


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

			System.out.println(hexText.append(binText).toString());

			binText.setLength(0);
			hexText.setLength(0);
		}
	}


	public static String toHex(byte[] aValue)
	{
		StringBuilder sb = new StringBuilder();
		for (byte b : aValue)
		{
			sb.append(String.format("%02X", b));
		}
		return sb.toString();
	}


	public static String toString(byte[] aValue)
	{
		return aValue == null ? null : new String(aValue, Charset.defaultCharset());
	}
}
