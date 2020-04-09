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
	private HashTableRoot mRoot;
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

		if (aTableHeader == null)
		{
			Log.i("create table %s", mTableName);
			Log.inc();

			mNodeSize = aTableParam.getPagesPerNode() * aBlockDevice.getBlockSize();
			mLeafSize = aTableParam.getPagesPerLeaf() * aBlockDevice.getBlockSize();
			mHashSeed = new SecureRandom().nextInt();
			mPointersPerNode = mNodeSize / BlockPointer.SIZE;

			mRoot = new HashTableRoot(this, true);
			mWasEmptyInstance = true;
			mChanged = true;
		}
		else
		{
			Log.i("open table %s", mTableName);
			Log.inc();

			mRoot = new HashTableRoot(this, false);

			unmarshalHeader(aTableHeader);

			mRoot.loadRoot();
		}

		Log.dec();
	}


	public byte[] marshalHeader()
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(BlockPointer.SIZE + 4 + 4 + 4 + 3);
		mRoot.marshal(buffer);
		buffer.writeInt32(mHashSeed);
		buffer.writeVar32(mNodeSize);
		buffer.writeVar32(mLeafSize);
		mBlockAccessor.getCompressionParam().marshal(buffer);

		return buffer.trim().array();
	}


	private void unmarshalHeader(byte[] aTableHeader)
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.wrap(aTableHeader);

		mRoot.unmarshal(buffer);

		mHashSeed = buffer.readInt32();
		mNodeSize = buffer.readVar32();
		mLeafSize = buffer.readVar32();
		mPointersPerNode = mNodeSize / BlockPointer.SIZE;

		CompressionParam compressionParam = new CompressionParam();
		compressionParam.unmarshal(buffer);

		mBlockAccessor.setCompressionParam(compressionParam);
	}


	public boolean get(ArrayMapEntry aEntry)
	{
		return mRoot.getValue(aEntry, 0);
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

		mRoot.put(aEntry, oldEntry);

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return oldEntry.get();
	}


	public ArrayMapEntry remove(ArrayMapEntry aEntry)
	{
		checkOpen();

		Result<ArrayMapEntry> oldEntry = new Result<>();

		boolean modified = mRoot.removeValue(aEntry, oldEntry, 0);

		mChanged |= modified;

		return oldEntry.get();
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		checkOpen();

		return mRoot.iterator();
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

				mRoot.writeBlock();

				if (mCommitChangesToBlockDevice)
				{
					mBlockAccessor.getBlockDevice().commit();
				}

				mChanged = false;

				Log.i("table commit finished; root block is %s", mRoot.getBlockPointer());

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

		mRoot.rollback(mWasEmptyInstance);

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

			if (node instanceof HashTableLeaf)
			{
				for (ArrayMapEntry entry : (HashTableLeaf)node)
				{
					if (entry.getFlags() == TableInstance.FLAG_BLOB)
					{
						Blob.deleteBlob(mBlockAccessor, entry.getValue());
					}
				}
			}

			freeBlock(node.getBlockPointer());
		});

		mRoot = new HashTableRoot(this, true);

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
		mRoot.close();
	}


	public int size()
	{
		checkOpen();

		Result<Integer> result = new Result<>(0);

		visit(node ->
		{
			mCost.mTreeTraversal++;

			if (node instanceof HashTableLeaf)
			{
				HashTableLeaf leaf = (HashTableLeaf)node;
				result.set(result.get() + leaf.size());
			}
		});

		return result.get();
	}


	public String integrityCheck()
	{
		Log.i("integrity check");

		return mRoot.integrityCheck();
	}


	public int getEntryMaximumLength()
	{
		return mLeafSize - HashTableLeaf.OVERHEAD;
	}


	void visit(HashTableVisitor aVisitor)
	{
		mRoot.visit(aVisitor);
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

		mRoot.scan(aScanResult);
	}


	int computeIndex(byte[] aKey, int aLevel)
	{
		return MurmurHash3.hash32(aKey, mHashSeed ^ aLevel) & (mPointersPerNode - 1);
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


	BlockPointer writeBlock(Node aNode, int aRange)
	{
		assert mPerformanceTool.tick("writeBlock");

		BlockPointer blockPointer = mBlockAccessor.writeBlock(aNode.array(), 0, aNode.array().length, mTransactionId.get(), aNode.getType(), aRange);

		mCost.mWriteBlock++;
		mCost.mWriteBlockBytes += blockPointer.getAllocatedSize();

		if (aNode instanceof HashTableLeaf)
		{
			((HashTableLeaf)aNode).mBlockPointer = blockPointer;
		}

		return blockPointer;
	}


	BlockPointer writeIfNotEmpty(HashTableLeaf aLeaf, int aRange)
	{
		if (aLeaf.isEmpty())
		{
			return new BlockPointer().setBlockType(BlockType.HOLE).setRange(aRange);
		}

		return writeBlock(aLeaf, aRange);
	}
}
