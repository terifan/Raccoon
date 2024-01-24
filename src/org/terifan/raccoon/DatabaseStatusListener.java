package org.terifan.raccoon;

import org.terifan.logging.Level;


@FunctionalInterface
public interface DatabaseStatusListener
{
	void statusChanged(Level aLevel, String aMessage, Throwable aThrowable);
}
