package org.terifan.raccoon;

import java.util.function.Consumer;


public class RuntimeDiagnostics
{
	private static int mReadNodes;
	private static int mWriteNodes;
	private static int mFreeNodes;
	private static int mReadLeafs;
	private static int mWriteLeafs;
	private static int mFreeLeafs;
//	private static int mReadBlocks;
//	private static int mWriteBlocks;
//	private static int mFreeBlocks;
//	private static int mReadBytes;
//	private static int mWriteBytes;
//	private static int mFreeBytes;

	public enum Operation
	{
//		READ_BLOCK(n -> {mReadBlocks++; mReadBytes += n;}),
//		WRITE_BLOCK(n -> {mWriteBlocks++; mWriteBytes += n;}),
//		FREE_BLOCK(n -> {mFreeBlocks++; mFreeBytes += n;}),
		READ_NODE(n -> mReadNodes++),
		WRITE_NODE(n -> mWriteNodes++),
		FREE_NODE(n -> mFreeNodes++),
		READ_LEAF(n -> mReadLeafs++),
		WRITE_LEAF(n -> mWriteLeafs++),
		FREE_LEAF(n -> mFreeLeafs++);

		Consumer<Integer> mConsumer;

		Operation(Consumer<Integer> aConsumer)
		{
			mConsumer = aConsumer;
		}
	}


	public static boolean collectStatistics(Operation aOperation, int aSize)
	{
		aOperation.mConsumer.accept(aSize);
		return true;
	}


	public static boolean collectStatistics(Operation aOperation, Object aObject)
	{
		aOperation.mConsumer.accept(aObject != null ? 1 : 0);
		return true;
	}


	public static void reset()
	{
//		mReadBlocks = 0;
//		mWriteBlocks = 0;
//		mFreeBlocks = 0;
//		mReadBytes = 0;
//		mWriteBytes = 0;
//		mFreeBytes = 0;
		mReadLeafs = 0;
		mWriteLeafs = 0;
		mFreeLeafs = 0;
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
		return
//			"readBlocks=" + mReadBlocks +
//			", writeBlocks=" + mWriteBlocks +
//			", freeBlocks=" + mFreeBlocks +
//			", readBytes=" + mReadBytes +
//			", writeBytes=" + mWriteBytes +
//			", freeBytes=" + mFreeBytes +
			"rNode=" + mReadNodes +
			", wNode=" + mWriteNodes +
			", fNode=" + mFreeNodes +
			", rLeaf=" + mReadLeafs +
			", wLeaf=" + mWriteLeafs +
			", fLeaf=" + mFreeLeafs;
	}
}
