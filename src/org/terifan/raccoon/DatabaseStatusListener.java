package org.terifan.raccoon;


@FunctionalInterface
public interface DatabaseStatusListener
{
	void statusChanged(LogLevel aLevel, String aMessage, Throwable aThrowable);
}
