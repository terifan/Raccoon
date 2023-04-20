package org.terifan.raccoon;

import org.terifan.raccoon.blockdevice.util.LogLevel;



@FunctionalInterface
public interface DatabaseStatusListener
{
	void statusChanged(LogLevel aLevel, String aMessage, Throwable aThrowable);
}
