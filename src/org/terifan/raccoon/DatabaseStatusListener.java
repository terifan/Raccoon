package org.terifan.raccoon;

import org.terifan.raccoon.io.util.LogLevel;



@FunctionalInterface
public interface DatabaseStatusListener
{
	void statusChanged(LogLevel aLevel, String aMessage, Throwable aThrowable);
}
