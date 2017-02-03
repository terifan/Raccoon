package org.terifan.raccoon;


@FunctionalInterface
public interface ErrorReportListener
{
	void receiveErrorReport(String aMessage, Throwable aThrowable);
}
