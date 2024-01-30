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
	private static int mReadExternal;
	private static int mWriteExternal;
	private static int mFreeExternal;


	public enum Operation
	{
		READ_NODE(n -> mReadNodes++),
		WRITE_NODE(n -> mWriteNodes++),
		FREE_NODE(n -> mFreeNodes++),
		READ_LEAF(n -> mReadLeafs++),
		WRITE_LEAF(n -> mWriteLeafs++),
		FREE_LEAF(n -> mFreeLeafs++),
		READ_EXT(n -> mReadExternal++),
		WRITE_EXT(n -> mWriteExternal++),
		FREE_EXT(n -> mFreeExternal++);

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
		mReadLeafs = 0;
		mWriteLeafs = 0;
		mFreeLeafs = 0;
		mReadNodes = 0;
		mWriteNodes = 0;
		mFreeNodes = 0;
		mReadExternal = 0;
		mWriteExternal = 0;
		mFreeExternal = 0;
	}


	public static void print()
	{
		System.out.println(string());
	}


	public static String string()
	{
		return String.format("node [%5d,%5d,%5d] leaf [%5d,%5d,%5d] ext [%5d,%5d,%5d]", mReadNodes, mWriteNodes, mFreeNodes, mReadLeafs, mWriteLeafs, mFreeLeafs, mReadExternal, mWriteExternal, mFreeExternal);
	}
}
