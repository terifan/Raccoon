package org.terifan.raccoon;

import org.terifan.logging.Level;
import org.terifan.raccoon.blockdevice.DeviceAccessOptions;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;


public class RaccoonDatabaseProvider
{
	private ManagedBlockDevice mDevice;
	private int mFlushInterval;
	private Level mLoggingLevel;


	public RaccoonDatabaseProvider(ManagedBlockDevice aDevice)
	{
		mDevice = aDevice;

		mFlushInterval = 1000;
		mLoggingLevel = Level.FATAL;
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file in the system temporary folder with a random name. File will be deleted on exit.
	 */
//	public static RaccoonDatabaseProvider device(RaccoonDevice aDevice)
//	{
//		return new RaccoonDatabaseProvider(aDevice);
//	}


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
		DeviceAccessOptions o = aOption == DatabaseOpenOption.REPLACE ? DeviceAccessOptions.CREATE : aOption == DatabaseOpenOption.READ_ONLY ? DeviceAccessOptions.READ_ONLY : DeviceAccessOptions.APPEND;
		return new RaccoonDatabase(mDevice.open(o), aOption);
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
