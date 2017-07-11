package org.terifan.raccoon;

import java.util.UUID;
import org.terifan.raccoon.io.managed.DeviceHeader;

public class Constants
{
	final static DeviceHeader DEVICE_HEADER = new DeviceHeader("raccoon-database", 1, 0, UUID.fromString(""));
	final static int DEFAULT_BLOCK_SIZE = 4096;
}
