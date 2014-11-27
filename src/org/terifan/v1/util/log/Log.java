package org.terifan.v1.util.log;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

// discriminator

public class Log
{
	private final static ConsoleTarget CONSOLE = new ConsoleTarget();

	/**
	 * This is a safe replacement for System.out to make FindBugs happy.
	 */
	private final static ProxyStream outProxy = new ProxyStream(System.out);
	public final static PrintStream out = new PrintStream(outProxy);

	/**
	 * This is a safe replacement for System.err to make FindBugs happy.
	 */
	private final static ProxyStream errProxy = new ProxyStream(System.err);
	public final static PrintStream err = new PrintStream(errProxy);

	private static final ArrayList<Entry> mEntries = new ArrayList<>();


	public static String getStackTraceString(Throwable aThrowable)
	{
		if (aThrowable == null)
		{
			return "null";
		}

		StringWriter w = new StringWriter();
		aThrowable.printStackTrace(new PrintWriter(w, true));

		return w.toString();
	}


	public static String getStackTraceStringFlatten(Exception aException)
	{
		return getStackTraceString(aException).replace('\r',' ').replace('\n',' ').replace('\t',' ').replace("    "," ").replace("   "," ").replace("  "," ");
	}


	public static void i(String aTag, String aMessage)
	{
		log(LogLevel.INFO, aTag, aMessage, null);
	}


	public static void i(String aTag, String aMessage, Object... aParams)
	{
		log(LogLevel.INFO, aTag, aMessage, null, aParams);
	}


	public static void i(String aTag, String aMessage, Throwable aThrowable)
	{
		log(LogLevel.INFO, aTag, aMessage, aThrowable);
	}


	public static void i(String aTag, String aMessage, Throwable aThrowable, Object... aParams)
	{
		log(LogLevel.INFO, aTag, aMessage, aThrowable, aParams);
	}


	public static void d(String aTag, String aMessage)
	{
		log(LogLevel.DEBUG, aTag, aMessage, null);
	}


	public static void d(String aTag, String aMessage, Object... aParams)
	{
		log(LogLevel.DEBUG, aTag, aMessage, null, aParams);
	}


	public static void d(String aTag, String aMessage, Throwable aThrowable)
	{
		log(LogLevel.DEBUG, aTag, aMessage, aThrowable);
	}


	public static void d(String aTag, String aMessage, Throwable aThrowable, Object... aParams)
	{
		log(LogLevel.DEBUG, aTag, aMessage, aThrowable, aParams);
	}


	public static void e(String aTag, String aMessage)
	{
		log(LogLevel.ERROR, aTag, aMessage, null);
	}


	public static void e(String aTag, String aMessage, Object... aParams)
	{
		log(LogLevel.ERROR, aTag, aMessage, null, aParams);
	}


	public static void e(String aTag, String aMessage, Throwable aThrowable)
	{
		log(LogLevel.ERROR, aTag, aMessage, aThrowable);
	}


	public static void e(String aTag, String aMessage, Throwable aThrowable, Object... aParams)
	{
		log(LogLevel.ERROR, aTag, aMessage, aThrowable, aParams);
	}


	public static void w(String aTag, String aMessage)
	{
		log(LogLevel.WARN, aTag, aMessage, null);
	}


	public static void w(String aTag, String aMessage, Object... aParams)
	{
		log(LogLevel.WARN, aTag, aMessage, null, aParams);
	}


	public static void w(String aTag, String aMessage, Throwable aThrowable)
	{
		log(LogLevel.WARN, aTag, aMessage, aThrowable);
	}


	public static void w(String aTag, String aMessage, Throwable aThrowable, Object... aParams)
	{
		log(LogLevel.WARN, aTag, aMessage, aThrowable, aParams);
	}


	public static void log(LogLevel aLevel, String aTag, String aMessage, Throwable aThrowable, Object... aParams)
	{
		log(new Date(), aLevel, aTag, aMessage, aThrowable, aParams);
	}


	public static void inc(String TAG, String aString, Object... aParams)
	{
	}


	public static void dec(String TAG)
	{
	}


	public static void dec(String TAG, String aString, Object... aParams)
	{
	}


	public static synchronized void log(Date aDateTime, LogLevel aLevel, String aTag, String aMessage, Throwable aThrowable, Object... aParams)
	{
		String message;

		if (aParams == null || aParams.length == 0)
		{
			message = aMessage;
		}
		else
		{
			message = String.format(aMessage, aParams);
		}

		boolean found = false;
		for (Entry entry : mEntries)
		{
			boolean b = entry.tags.isEmpty() || entry.tags.contains(aTag);

			found |= b;

			if (b && aLevel.ordinal() >= entry.level.ordinal())
			{
				entry.target.writeLogEntry(aDateTime, aLevel, aTag, message, aThrowable);
			}
		}

		if (!found)
		{
			CONSOLE.writeLogEntry(aDateTime, aLevel, aTag, message, aThrowable);
		}
	}


	public static void addTarget(LogLevel aLevel, LogTarget aTarget, String... aTags)
	{
		if (aTags == null)
		{
			addTarget(aLevel, aTarget, (Collection)null);
		}
		else
		{
			addTarget(aLevel, aTarget, Arrays.asList(aTags));
		}
	}


	public static synchronized void addTarget(LogLevel aLevel, LogTarget aTarget, Collection<String> aTags)
	{
		if (aLevel == null)
		{
			throw new IllegalArgumentException();
		}
		if (aTarget == null)
		{
			throw new IllegalArgumentException();
		}

		// if target already exists then ignore it but add any new tags
		for (Entry entry : mEntries)
		{
			if (entry.level == aLevel && entry.target.equals(aTarget))
			{
				if (aTags != null)
				{
					entry.tags.addAll(aTags);
				}
				return;
			}
		}

		mEntries.add(new Entry(aTarget, aLevel, aTags));
	}


	public static synchronized void removeTarget(LogTarget aTarget)
	{
		for (int i = mEntries.size(); --i >= 0;)
		{
			if (mEntries.get(i).target.equals(aTarget))
			{
				mEntries.remove(i);
			}
		}
	}


	public static synchronized void removeTag(String aTag)
	{
		for (int i = mEntries.size(); --i >= 0;)
		{
			if (mEntries.get(i).tags.contains(aTag))
			{
				mEntries.get(i).tags.remove(aTag);
			}
		}
	}


	public static void setOut(OutputStream aOutputStream)
	{
		outProxy.mOutputStream = aOutputStream;
	}


	public static OutputStream getOut()
	{
		return outProxy.mOutputStream;
	}


	public static void setErr(OutputStream aOutputStream)
	{
		errProxy.mOutputStream = aOutputStream;
	}


	public static OutputStream getErr()
	{
		return errProxy.mOutputStream;
	}


	public static long freeMemory()
	{
		return Runtime.getRuntime().maxMemory() + Runtime.getRuntime().freeMemory() - Runtime.getRuntime().totalMemory();
	}


	private static class ProxyStream extends OutputStream
	{
		private OutputStream mOutputStream;

		public ProxyStream(OutputStream aOutputStream)
		{
			mOutputStream = aOutputStream;
		}


		@Override
		public void write(int c) throws IOException
		{
			if (mOutputStream != null)
			{
				mOutputStream.write(c);
			}
		}


		@Override
		public void write(byte[] aBuffer) throws IOException
		{
			if (mOutputStream != null)
			{
				mOutputStream.write(aBuffer);
			}
		}


		@Override
		public void write(byte[] aBuffer, int aOffset, int aLength) throws IOException
		{
			if (mOutputStream != null)
			{
				mOutputStream.write(aBuffer, aOffset, aLength);
			}
		}
	}


	private static class Entry
	{
		LogTarget target;
		LogLevel level;
		Collection<String> tags;


		public Entry(LogTarget aTarget, LogLevel aLevel, Collection<String> aTags)
		{
			this.target = aTarget;
			this.level = aLevel;
			this.tags = aTags;
		}
	}
}
