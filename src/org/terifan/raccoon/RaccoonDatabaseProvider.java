package org.terifan.raccoon;

import org.terifan.logging.Level;
import org.terifan.raccoon.blockdevice.RaccoonStorageInstance;


public class RaccoonDatabaseProvider
{
	private RaccoonStorageInstance mDevice;
	private int mFlushInterval;
	private Level mLoggingLevel;


	public RaccoonDatabaseProvider(RaccoonStorageInstance aDevice)
	{
		mDevice = aDevice;

		mFlushInterval = 1000;
		mLoggingLevel = Level.FATAL;
	}


	/**
	 * Open or create a RaccoonDatabase with the properties in the builder.
	 */
	public RaccoonDatabase get()
	{
		return get(DatabaseOpenOption.CREATE);
	}


	/**
	 * Open or create a RaccoonDatabase with the properties in the builder.
	 */
	public RaccoonDatabase get(DatabaseOpenOption aOption)
	{
		return new RaccoonDatabase(mDevice, aOption);
	}


	public RaccoonDatabaseProvider withLogging(Level aLevel)
	{
		mLoggingLevel = aLevel;
		return this;
	}


	public RaccoonDatabaseProvider withFlushInterval(int aInterval)
	{
		mFlushInterval = aInterval;
		return this;
	}
}
