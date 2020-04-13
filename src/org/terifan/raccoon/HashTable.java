package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;
import org.terifan.security.messagedigest.MurmurHash3;


final class HashTable implements AutoCloseable, Iterable<ArrayMapEntry>
{
	/*private*/ final Cost mCost;
	/*private*/ final String mTableName;
	final TransactionGroup mTransactionId;
	/*private*/ BlockAccessor mBlockAccessor;
	int mNodeSize;
	int mLeafSize;
	int mPointersPerNode;
	int mHashSeed;
	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private boolean mChanged;
	private boolean mCommitChangesToBlockDevice;
	/*private*/ final PerformanceTool mPerformanceTool;
	/*private*/ int mModCount;
	private int mBitsPerNode;
	private HashTableNode mRootNode;
	private BlockPointer mRootNodeBlockPointer;


	/**
	 * Open an existing HashTable or create a new HashTable with default settings.
	 */
	public HashTable(IManagedBlockDevice aBlockDevice, byte[] aTableHeader, TransactionGroup aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName, Cost aCost, PerformanceTool aPerformanceTool)
	{
		mPerformanceTool = aPerformanceTool;
		mTableName = aTableName;
		mTransactionId = aTransactionId;
		mCost = aCost;
		mCommitChangesToBlockDevice = aCommitChangesToBlockDevice;
		mBlockAccessor = new BlockAccessor(aBlockDevice, aCompressionParam, aTableParam.getBlockReadCacheSize());

		boolean create = aTableHeader == null;

		if (create)
		{
			Log.i("create table %s", mTableName);
			Log.inc();

			mNodeSize = aTableParam.getPagesPerNode() * aBlockDevice.getBlockSize();
			mLeafSize = aTableParam.getPagesPerLeaf() * aBlockDevice.getBlockSize();
			mHashSeed = new SecureRandom().nextInt();
			mPointersPerNode = mNodeSize / BlockPointer.SIZE;
			mBitsPerNode = (int)(Math.log(mPointersPerNode) / Math.log(2));

			mRootNode = new HashTableLeafNode(this, null);
			mRootNodeBlockPointer = writeBlock(mRootNode, mPointersPerNode);

			mWasEmptyInstance = true;
			mChanged = true;
		}
		else
		{
			Log.i("open table %s", mTableName);
			Log.inc();

			unmarshalHeader(aTableHeader);

			if (mRootNodeBlockPointer.getBlockType() == BlockType.LEAF)
			{
				mRootNode = new HashTableLeafNode(this, null, mRootNodeBlockPointer);
			}
			else
			{
				mRootNode = new HashTableInnerNode(this, null, mRootNodeBlockPointer);
			}
		}

		Log.dec();
	}


	public byte[] marshalHeader()
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(BlockPointer.SIZE + 4 + 4 + 4 + 3);
		mRootNodeBlockPointer.marshal(buffer);
		buffer.writeInt32(mHashSeed);
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

		mHashSeed = buffer.readInt32();
		mNodeSize = buffer.readVar32();
		mLeafSize = buffer.readVar32();
		mPointersPerNode = mNodeSize / BlockPointer.SIZE;

		mBitsPerNode = (int)(Math.log(mPointersPerNode) / Math.log(2));

		CompressionParam compressionParam = new CompressionParam();
		compressionParam.unmarshal(buffer);

		mBlockAccessor.setCompressionParam(compressionParam);
	}


	public boolean get(ArrayMapEntry aEntry)
	{
		return mRootNode.getValue(aEntry, computeHash(aEntry.getKey()), 0);
	}


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

		if (mRootNode instanceof HashTableLeafNode)
		{
			Log.d("put root value");

			if (mRootNode.putValue(aEntry, oldEntry, hash, 0))
			{
				Log.dec();
				assert mModCount == modCount : "concurrent modification";

				return oldEntry.get();
			}

			Log.d("upgrade root from leaf to node");

			mRootNode = ((HashTableLeafNode)mRootNode).growTree(0);

			mRootNodeBlockPointer = writeBlock(mRootNode, mPointersPerNode);
		}

		mRootNode.putValue(aEntry, oldEntry, hash, 0);

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return oldEntry.get();
	}


	public ArrayMapEntry remove(ArrayMapEntry aEntry)
	{
		checkOpen();

		Result<ArrayMapEntry> oldEntry = new Result<>();

		boolean modified = mRootNode.removeValue(aEntry, oldEntry, computeHash(aEntry.getKey()), 0);

		mChanged |= modified;

		return oldEntry.get();
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		checkOpen();

		return new HashTableNodeIterator(this, mRootNode);
	}


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


	public boolean isChanged()
	{
		return mChanged;
	}


	public boolean commit()
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

				freeBlock(mRootNodeBlockPointer);

				mRootNodeBlockPointer = writeBlock(mRootNode, mPointersPerNode);

				if (mCommitChangesToBlockDevice)
				{
					mBlockAccessor.getBlockDevice().commit();
				}

				mChanged = false;

				Log.i("table commit finished; root block is %s", mRootNodeBlockPointer);

				Log.dec();
				assert mModCount == modCount : "concurrent modification";

				return true;
			}

			if (mWasEmptyInstance && mCommitChangesToBlockDevice)
			{
				mBlockAccessor.getBlockDevice().commit();
			}

			return false;
		}
		finally
		{
			mWasEmptyInstance = false;
		}
	}


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
			mRootNode = new HashTableLeafNode(this, null);
		}
		else
		{
			Log.d("rollback %s", mRootNodeBlockPointer.getBlockType() == BlockType.LEAF ? "root map" : "root node");

			if (mRootNodeBlockPointer.getBlockType() == BlockType.LEAF)
			{
				mRootNode = new HashTableLeafNode(this, null, mRootNodeBlockPointer);
			}
			else
			{
				mRootNode = new HashTableInnerNode(this, null, mRootNodeBlockPointer);
			}
		}

		mChanged = false;
	}


	public void clear()
	{
		checkOpen();

		Log.i("clear");

		int modCount = ++mModCount;
		mChanged = true;

		visit(node ->
		{
			mCost.mTreeTraversal++;

			if (node instanceof HashTableLeafNode)
			{
				for (ArrayMapEntry entry : (HashTableLeafNode)node)
				{
					if ((entry.getFlags() & TableInstance.FLAG_BLOB) != 0)
					{
						Blob.deleteBlob(mBlockAccessor, entry.getValue());
					}
				}
			}

			freeBlock(node.getBlockPointer());
		});

		mRootNode = new HashTableLeafNode(this, null);
		mRootNodeBlockPointer = writeBlock(mRootNode, mPointersPerNode);

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


	public int size()
	{
		checkOpen();

		Result<Integer> result = new Result<>(0);

		visit(node ->
		{
			mCost.mTreeTraversal++;

			if (node instanceof HashTableLeafNode)
			{
				HashTableLeafNode leaf = (HashTableLeafNode)node;
				result.set(result.get() + leaf.size());
			}
		});

		return result.get();
	}


	public String integrityCheck()
	{
		Log.i("integrity check");

		return mRootNode.integrityCheck();
	}


	public int getEntryMaximumLength()
	{
		return mLeafSize - HashTableLeafNode.OVERHEAD;
	}


	void visit(HashTableVisitor aVisitor)
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


	public void scan(ScanResult aScanResult)
	{
		aScanResult.tables++;

		mRootNode.scan(aScanResult);
	}


	long computeHash(byte[] aKey)
	{
		return MurmurHash3.hash64(aKey, mHashSeed);
	}


	int computeIndex(long aHash, int aLevel)
	{
		return (int)(Long.rotateRight(aHash, aLevel * mBitsPerNode) & (mPointersPerNode - 1));
	}


	void freeBlock(BlockPointer aBlockPointer)
	{
		mCost.mFreeBlock++;
		mCost.mFreeBlockBytes += aBlockPointer.getAllocatedSize();

		mBlockAccessor.freeBlock(aBlockPointer);
	}


	byte[] readBlock(BlockPointer aBlockPointer)
	{
		assert mPerformanceTool.tick("readBlock");

		mCost.mReadBlock++;
		mCost.mReadBlockBytes += aBlockPointer.getAllocatedSize();

		return mBlockAccessor.readBlock(aBlockPointer);
	}


	BlockPointer writeBlock(HashTableNode aNode, int aRange)
	{
		assert mPerformanceTool.tick("writeBlock");

		BlockPointer blockPointer = mBlockAccessor.writeBlock(aNode.array(), 0, aNode.array().length, mTransactionId.get(), aNode.getType(), aRange);

		mCost.mWriteBlock++;
		mCost.mWriteBlockBytes += blockPointer.getAllocatedSize();

		if (aNode instanceof HashTableLeafNode)
		{
			((HashTableLeafNode)aNode).mBlockPointer = blockPointer;
		}

		return blockPointer;
	}
}
