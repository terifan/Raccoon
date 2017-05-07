package org.terifan.raccoon;


public class Constants
{
	public final static int RACCOON_FILE_FORMAT_VERSION = 4;
	public final static int EXTRA_DATA_CHECKSUM_SEED = 0xf49209b1;
	public final static long RACCOON_DB_IDENTITY = 0x726163636f6f6e00L; // 'raccoon\0'
	public final static int DEFAULT_BLOCK_SIZE = 4096;
	public final static boolean REORDER_LAZY_CACHE_ON_READ = true;
	public final static boolean REORDER_LAZY_CACHE_ON_WRITE = true;
	public final static int DEFAULT_LAZY_WRITE_CACHE_SIZE = 512;
}
