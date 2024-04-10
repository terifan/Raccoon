package org.terifan.raccoon;

import org.terifan.raccoon.blockdevice.BlockDeviceOpenOption;


public enum DatabaseOpenOption
{
	/**
	 * Open an already existing device for both read and write mode causing an exception if not found.
	 */
	OPEN,
	/**
	 * Open or create a new device for both read and write mode.
	 */
	CREATE,
	/**
	 * Replace any existing device and create a new for both read and write mode.
	 */
	REPLACE,
	/**
	 * Open an existing device for only read mode.
	 */
	READ_ONLY;


	public BlockDeviceOpenOption toBlockDeviceOpenOption()
	{
		switch (this)
		{
			case OPEN:
				return BlockDeviceOpenOption.OPEN;
			case CREATE:
				return BlockDeviceOpenOption.CREATE;
			case REPLACE:
				return BlockDeviceOpenOption.REPLACE;
			case READ_ONLY:
				return BlockDeviceOpenOption.READ_ONLY;
		}
		throw new Error();
	}
}
