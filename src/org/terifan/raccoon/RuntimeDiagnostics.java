package org.terifan.raccoon;


public class RuntimeDiagnostics
{
	public enum Operation
	{
		READ_BLOCK, WRITE_BLOCK, FREE_BLOCK, READ_NODE, WRITE_NODE, FREE_NODE
	}

	private static int mReadNodes;
	private static int mWriteNodes;
	private static int mFreeNodes;
	private static int mReadBlocks;
	private static int mWriteBlocks;
	private static int mFreeBlocks;
	private static int mReadBytes;
	private static int mWriteBytes;
	private static int mFreeBytes;


	public static boolean collectStatistics(Operation aOperation, int aSize)
	{
		switch (aOperation)
		{
			case READ_BLOCK:
				mReadBlocks++;
				mReadBytes += aSize;
				break;
			case WRITE_BLOCK:
				mWriteBlocks++;
				mWriteBytes += aSize;
				break;
			case FREE_BLOCK:
				mFreeBlocks++;
				mFreeBytes += aSize;
				break;
			case READ_NODE:
				mReadNodes++;
				break;
			case WRITE_NODE:
				mWriteNodes++;
				break;
			case FREE_NODE:
				mFreeNodes++;
				break;
		}
		return true;
	}


	public static void reset()
	{
		mReadBlocks = 0;
		mWriteBlocks = 0;
		mFreeBlocks = 0;
		mReadBytes = 0;
		mWriteBytes = 0;
		mFreeBytes = 0;
		mReadNodes = 0;
		mWriteNodes = 0;
		mFreeNodes = 0;
	}


	public static void print()
	{
		System.out.println(string());
	}


	public static String string()
	{
		return "RuntimeDiagnostics{" +
			"mReadBlocks=" + mReadBlocks +
			", mWriteBlocks=" + mWriteBlocks +
			", mFreeBlocks=" + mFreeBlocks +
			", mReadBytes=" + mReadBytes +
			", mWriteBytes=" + mWriteBytes +
			", mFreeBytes=" + mFreeBytes +
			", mReadNodes=" + mReadNodes +
			", mWriteNodes=" + mWriteNodes +
			", mFreeNodes=" + mFreeNodes
			+ '}';
	}
}
