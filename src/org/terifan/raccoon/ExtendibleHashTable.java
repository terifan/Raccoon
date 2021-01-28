package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.terifan.raccoon.HashTreeTable.FakeInnerNode;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;
import org.terifan.security.messagedigest.MurmurHash3;


final class ExtendibleHashTable implements AutoCloseable, ITableImplementation
{
	private Cost mCost;
	private String mTableName;
	private TransactionGroup mTransactionId;
	private BlockAccessor mBlockAccessor;
	private PerformanceTool mPerformanceTool;
	private BlockPointer mRootNodeBlockPointer;
	private byte[] mDirectory;
	private long mHashSeed;
	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private boolean mChanged;
	private boolean mCommitChangesToBlockDevice;
	private int mModCount;

	private HashMap<Integer, ArrayMapEntry> mNodes;


	public ExtendibleHashTable()
	{
		mNodes = new HashMap<>();
	}


	@Override
	public void create(IManagedBlockDevice aBlockDevice, TransactionGroup aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName, Cost aCost, PerformanceTool aPerformanceTool)
	{
		mTableName = aTableName;
		mTransactionId = aTransactionId;
		mBlockAccessor = new BlockAccessor(aBlockDevice, aCompressionParam);
		mCommitChangesToBlockDevice = aCommitChangesToBlockDevice;
		mPerformanceTool = aPerformanceTool;
		mCost = aCost;

		Log.i("create table %s", mTableName);
		Log.inc();

		mHashSeed = new SecureRandom().nextLong();

		mDirectory = new byte[mBlockAccessor.getBlockDevice().getBlockSize()];

		mWasEmptyInstance = true;
		mChanged = true;

		Log.dec();
	}


	@Override
	public void open(IManagedBlockDevice aBlockDevice, TransactionGroup aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName, Cost aCost, PerformanceTool aPerformanceTool, byte[] aTableHeader)
	{
		mTableName = aTableName;
		mTransactionId = aTransactionId;
		mBlockAccessor = new BlockAccessor(aBlockDevice, aCompressionParam);
		mCommitChangesToBlockDevice = aCommitChangesToBlockDevice;
		mCost = aCost;
		mPerformanceTool = aPerformanceTool;

		Log.i("open table %s", mTableName);
		Log.inc();

		unmarshalHeader(aTableHeader);

		Log.dec();
	}


	private byte[] marshalHeader()
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(BlockPointer.SIZE + 4 + 4 + 4 + 3);
		mRootNodeBlockPointer.marshal(buffer);
		buffer.writeInt64(mHashSeed);
		buffer.writeVar32(mNodeSize);
		buffer.writeVar32(mLeafSize);
		mBlockAccessor.getCompressionParam().marshal(buffer);

		return buffer.trim().array();
	}


	private void unmarshalHeader(byte[] aTableHeader)
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.wrap(aTableHeader);

		mRootNodeBlockPointer = new BlockPointer();
		mRootNodeBlockPointer.unmarshal(buffer);

		mHashSeed = buffer.readInt64();
		mNodeSize = buffer.readVar32();
		mLeafSize = buffer.readVar32();
		mPointersPerNode = mNodeSize / BlockPointer.SIZE;

		mBitsPerNode = (int)(Math.log(mPointersPerNode) / Math.log(2));

		CompressionParam compressionParam = new CompressionParam();
		compressionParam.unmarshal(buffer);

		mBlockAccessor.setCompressionParam(compressionParam);
	}


	@Override
	public boolean get(ArrayMapEntry aEntry)
	{
		return mRootNode.get(aEntry, computeHash(aEntry.getKey()), 0);
	}


	@Override
	public ArrayMapEntry put(ArrayMapEntry aEntry)
	{
		checkOpen();

		if (aEntry.getKey().length + aEntry.getValue().length > getEntryMaximumLength())
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.getKey().length + ", value: " + aEntry.getValue().length + ", maximum: " + getEntryMaximumLength());
		}

		assert mPerformanceTool.tick("put");

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		mChanged = true;

		Result<ArrayMapEntry> oldEntry = new Result<>();

		long hash = computeHash(aEntry.getKey());

		if (mRootNode instanceof HashTreeTableLeafNode)
		{
			Log.d("put root value");

			if (mRootNode.put(aEntry, oldEntry, hash, 0))
			{
				Log.dec();
				assert mModCount == modCount : "concurrent modification";

				return oldEntry.get();
			}

			Log.d("upgrade root from leaf to node");

			mRootNode = ((HashTreeTableLeafNode)mRootNode).splitRootLeaf();
		}

		mRootNode.put(aEntry, oldEntry, hash, 0);

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return oldEntry.get();
	}


	@Override
	public ArrayMapEntry remove(ArrayMapEntry aEntry)
	{
		checkOpen();

		Result<ArrayMapEntry> oldEntry = new Result<>();

		boolean modified = mRootNode.remove(aEntry, oldEntry, computeHash(aEntry.getKey()), 0);

		mChanged |= modified;

		return oldEntry.get();
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		checkOpen();

		return new HashTreeTableNodeIterator(this, mRootNode);
	}


	@Override
	public ArrayList<ArrayMapEntry> list()
	{
		checkOpen();

		ArrayList<ArrayMapEntry> list = new ArrayList<>();

		for (Iterator<ArrayMapEntry> it = iterator(); it.hasNext();)
		{
			list.add(it.next());
		}

		return list;
	}


	@Override
	public boolean isChanged()
	{
		return mChanged;
	}


	@Override
	public byte[] commit(AtomicBoolean oChanged)
	{
		assert mPerformanceTool.tick("commit");

		checkOpen();

		try
		{
			if (mChanged)
			{
				int modCount = mModCount; // no increment
				Log.i("commit hash table");
				Log.inc();

				mRootNodeBlockPointer = mRootNode.flush();

				if (mCommitChangesToBlockDevice)
				{
					mBlockAccessor.getBlockDevice().commit();
				}

				mChanged = false;

				Log.i("table commit finished; root block is %s", mRootNodeBlockPointer);

				Log.dec();
				assert mModCount == modCount : "concurrent modification";

				if (oChanged != null)
				{
					oChanged.set(true);
				}
				return marshalHeader();
			}

			if (mWasEmptyInstance && mCommitChangesToBlockDevice)
			{
				mBlockAccessor.getBlockDevice().commit();
			}

			if (oChanged != null)
			{
				oChanged.set(false);
			}
			return marshalHeader();
		}
		finally
		{
			mWasEmptyInstance = false;
		}
	}


	@Override
	public void rollback()
	{
		checkOpen();

		Log.i("rollback");

		if (mCommitChangesToBlockDevice)
		{
			mBlockAccessor.getBlockDevice().rollback();
		}

		if (mWasEmptyInstance)
		{
			Log.d("rollback empty");

			// occurs when the hashtable is created and never been commited thus rollback is to an empty hashtable
			mRootNode = new HashTreeTableLeafNode(this, new FakeInnerNode());
		}
		else
		{
			Log.d("rollback %s", mRootNodeBlockPointer.getBlockType() == BlockType.LEAF ? "root map" : "root node");

			if (mRootNodeBlockPointer.getBlockType() == BlockType.LEAF)
			{
				mRootNode = new HashTreeTableLeafNode(this, new FakeInnerNode(), mRootNodeBlockPointer);
			}
			else
			{
				mRootNode = new HashTreeTableInnerNode(this, null, mRootNodeBlockPointer);
			}
		}

		mChanged = false;
	}


	@Override
	public void removeAll()
	{
		checkOpen();

		int modCount = ++mModCount;

		mRootNode.clear();
		mChanged = true;

		mRootNode = new HashTreeTableLeafNode(this, new FakeInnerNode());
		mRootNodeBlockPointer = null;

		assert mModCount == modCount : "concurrent modification";
	}


	/**
	 * Clean-up resources only
	 */
	@Override
	public void close()
	{
		mClosed = true;

		mBlockAccessor = null;
		mRootNode = null;
	}


	@Override
	public int size()
	{
		checkOpen();

		Result<Integer> result = new Result<>(0);

		visit(node ->
		{
			mCost.mTreeTraversal++;

			if (node instanceof HashTreeTableLeafNode)
			{
				HashTreeTableLeafNode leaf = (HashTreeTableLeafNode)node;
				result.set(result.get() + leaf.size());
			}
		});

		return result.get();
	}


	@Override
	public String integrityCheck()
	{
		Log.i("integrity check");

		return mRootNode.integrityCheck();
	}


	@Override
	public int getEntryMaximumLength()
	{
		return mLeafSize - HashTreeTableLeafNode.OVERHEAD;
	}


	void visit(HashTreeTableVisitor aVisitor)
	{
		mRootNode.visit(aVisitor);
	}


	void checkOpen()
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}
	}


	@Override
	public void scan(ScanResult aScanResult)
	{
//		aScanResult.tables++;
//
//		mRootNode.scan(aScanResult);
	}


	long computeHash(byte[] aKey)
	{
		return MurmurHash3.hash64(aKey, mHashSeed);
	}


	int computeIndex(long aHash, int aLevel)
	{
		return (int)(Long.rotateRight(aHash, aLevel * mBitsPerNode) & (mPointersPerNode - 1));
	}


//	void freeBlock(BlockPointer aBlockPointer)
//	{
//		mCost.mFreeBlock++;
//		mCost.mFreeBlockBytes += aBlockPointer.getAllocatedBlocks();
//
//		mBlockAccessor.freeBlock(aBlockPointer);
//	}
//
//
//	byte[] readBlock(BlockPointer aBlockPointer)
//	{
//		assert mPerformanceTool.tick("readBlock");
//
//		mCost.mReadBlock++;
//		mCost.mReadBlockBytes += aBlockPointer.getAllocatedBlocks();
//
//		return mBlockAccessor.readBlock(aBlockPointer);
//	}
//
//
//	BlockPointer writeBlock(HashTreeTableNode aNode, int aRange)
//	{
//		assert mPerformanceTool.tick("writeBlock");
//
//		BlockPointer blockPointer = mBlockAccessor.writeBlock(aNode.array(), 0, aNode.array().length, mTransactionId.get(), aNode.getType(), aRange);
//
//		mCost.mWriteBlock++;
//		mCost.mWriteBlockBytes += blockPointer.getAllocatedBlocks();
//
//		return blockPointer;
//	}
}
