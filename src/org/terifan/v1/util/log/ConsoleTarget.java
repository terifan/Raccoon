package org.terifan.v1.util.log;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;


public class ConsoleTarget implements LogTarget
{
	private SimpleDateFormat mDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	@Override
	public void writeLogEntry(Date aDateTime, LogLevel aLogLevel, String aTag, String aMessage, Throwable aThrowable)
	{
		try
		{
			PrintStream o = aLogLevel == LogLevel.ERROR ? Log.err : Log.out;

			String dateTime = mDateFormat.format(new Date(aDateTime.getTime()));

			if (aThrowable != null)
			{
				o.printf("%s %-5s %-20s %s %s", dateTime, aLogLevel.name(), aTag, aMessage, Log.getStackTraceString(aThrowable).replace("\t","   "));
			}
			else
			{
				o.printf("%s %-5s %-20s %s\n", dateTime, aLogLevel.name(), aTag, aMessage);
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(Log.err);
		}
	}
}
