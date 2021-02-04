package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.terifan.raccoon.HashTreeTable.FakeInnerNode;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;
import org.terifan.security.messagedigest.MurmurHash3;
import org.terifan.util.Debug;


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

	private LeafNode[] mNodes;
	private int mPrefixLength;


	public ExtendibleHashTable()
	{
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

		int blockSize = mBlockAccessor.getBlockDevice().getBlockSize();
		int pointersPerPage = blockSize / BlockPointer.SIZE;
		mPrefixLength = (int)Math.ceil(Math.log(pointersPerPage) / Math.log(2));

		mHashSeed = new SecureRandom().nextLong();
		mDirectory = new byte[blockSize];

		LeafNode node = new LeafNode();
		node.mMap = new ArrayMap(blockSize * 10);
		node.mSize = mPrefixLength;
		node.mDirty = true;

		mNodes = new LeafNode[1 << mPrefixLength];
		Arrays.fill(mNodes, node);

		System.out.println(mPrefixLength);
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
		mPerformanceTool = aPerformanceTool;
		mCost = aCost;

		Log.i("open table %s", mTableName);
		Log.inc();

		unmarshalHeader(aTableHeader);

		mDirectory = readBlock(mRootNodeBlockPointer);

		int pointersPerPage = mDirectory.length / BlockPointer.SIZE;
		mPrefixLength = (int)Math.ceil(Math.log(pointersPerPage) / Math.log(2));

		mNodes = new LeafNode[1 << mPrefixLength];

		System.out.println(mPrefixLength);
		Debug.hexDump(mDirectory);

		Log.dec();
	}


	private byte[] marshalHeader()
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(BlockPointer.SIZE + 8 + 3);
		mRootNodeBlockPointer.marshal(buffer);
		buffer.writeInt64(mHashSeed);
		mBlockAccessor.getCompressionParam().marshal(buffer);

		return buffer.trim().array();
	}


	private void unmarshalHeader(byte[] aTableHeader)
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.wrap(aTableHeader);

		mRootNodeBlockPointer = new BlockPointer();
		mRootNodeBlockPointer.unmarshal(buffer);

		mHashSeed = buffer.readInt64();

		CompressionParam compressionParam = new CompressionParam();
		compressionParam.unmarshal(buffer);

		mBlockAccessor.setCompressionParam(compressionParam);
	}


	@Override
	public boolean get(ArrayMapEntry aEntry)
	{
		checkOpen();

		int index = computeIndex(computeHash(aEntry.getKey()));

		return loadNode(index).mMap.get(aEntry);
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

		int index = computeIndex(computeHash(aEntry.getKey()));

		LeafNode node = loadNode(index);

		node.mMap.put(aEntry, oldEntry);
		node.mDirty = true;

		System.out.println(node.mMap);

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return oldEntry.get();

//		long hash = computeHash(aEntry.getKey());
//
//		if (mRootNode instanceof HashTreeTableLeafNode)
//		{
//			Log.d("put root value");
//
//			if (mRootNode.put(aEntry, oldEntry, hash, 0))
//			{
//				Log.dec();
//				assert mModCount == modCount : "concurrent modification";
//
//				return oldEntry.get();
//			}
//
//			Log.d("upgrade root from leaf to node");
//
//			mRootNode = ((HashTreeTableLeafNode)mRootNode).splitRootLeaf();
//		}
//
//		mRootNode.put(aEntry, oldEntry, hash, 0);
//
//		Log.dec();
//		assert mModCount == modCount : "concurrent modification";
//
//		return oldEntry.get();
	}


	@Override
	public ArrayMapEntry remove(ArrayMapEntry aEntry)
	{
		return null;
//		checkOpen();
//
//		Result<ArrayMapEntry> oldEntry = new Result<>();
//
//		boolean modified = mRootNode.remove(aEntry, oldEntry, computeHash(aEntry.getKey()), 0);
//
//		mChanged |= modified;
//
//		return oldEntry.get();
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		checkOpen();

		return new ExtendibleHashTableMapIterator();
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

				flush();

				if (mRootNodeBlockPointer != null)
				{
					freeBlock(mRootNodeBlockPointer);
				}

				mRootNodeBlockPointer = writeBlock(mDirectory, BlockType.INDEX, 0L);

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


	private void flush()
	{
		for (int i = 0; i < mNodes.length; i++)
		{
			LeafNode node = mNodes[i];

			if (node != null && node.mDirty)
			{
				if (node.mBlockPointer != null)
				{
					freeBlock(node.mBlockPointer);
				}
				node.mBlockPointer = writeBlock(node.mMap.array(), BlockType.LEAF, node.mSize);
				node.mBlockPointer.marshal(ByteArrayBuffer.wrap(mDirectory).position(BlockPointer.SIZE * i));
				node.mDirty = false;
			}
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
			mDirectory = new byte[BlockPointer.SIZE];
		}
		else
		{
			Log.d("rollback %s", mRootNodeBlockPointer.getBlockType() == BlockType.LEAF ? "root map" : "root node");

			mDirectory = new byte[BlockPointer.SIZE];
		}

		mChanged = false;
	}


	@Override
	public void removeAll()
	{
//		checkOpen();
//
//		int modCount = ++mModCount;
//
//		mRootNode.clear();
//		mChanged = true;
//
//		mRootNode = new HashTreeTableLeafNode(this, new FakeInnerNode());
//		mRootNodeBlockPointer = null;
//
//		assert mModCount == modCount : "concurrent modification";
	}


	/**
	 * Clean-up resources only
	 */
	@Override
	public void close()
	{
		mClosed = true;

		mBlockAccessor = null;
		mDirectory = null;
		mNodes = null;
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
		return null;
//		Log.i("integrity check");
//
//		return mRootNode.integrityCheck();
	}


	@Override
	public int getEntryMaximumLength()
	{
		return 1024;
//		return mLeafSize - HashTreeTableLeafNode.OVERHEAD;
	}


	void visit(HashTreeTableVisitor aVisitor)
	{
//		mRootNode.visit(aVisitor);
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


	int computeIndex(long aHash)
	{
		return (int)(0x7fffffff & (aHash >>> (64 - mPrefixLength)));
	}


	/*
	 *                 split           split + split   grow + split
	 *
	 * 000 +----- 8    000 +----- 4    000 +----- 1    0000 +----- 2
	 * 001 |           001 |           001 +----- 1    0001 |
	 * 010 |           010 |           010 +----- 2    0010 +----- 1
	 * 011 |           011 |           011 |           0011 +----- 1
	 * 100 |           100 +----- 4    100 +----- 4    0100 +----- 4
	 * 101 |           101 |           101 |           0101 |
	 * 110 |           110 |           110 |           0110 |
	 * 111 |           111 |           111 |           0111 |
	 * 	           	           	           	           1000 +----- 8
	 * 	           	           	           	           1001 |
	 * 	           	           	           	           1010 |
	 * 	           	           	           	           1011 |
	 * 	           	           	           	           1100 |
	 * 	           	           	           	           1101 |
	 * 	           	           	           	           1110 |
	 * 	           	           	           	           1111 |
	 */
	private void growDirectory()
	{
		LeafNode[] newNodes = new LeafNode[2 * mNodes.length];

		for (int dst = 0, src = 0; dst < newNodes.length; )
		{
			newNodes[dst++] = mNodes[src++];
		}
		for (int i = 0; i < newNodes.length; )
		{
			newNodes[i].mSize *= 2;
			i += newNodes[i].mSize;
		}

		byte[] newDirectory = new byte[2 * mDirectory.length];

		for (int src = 0, dst = 0; dst < newDirectory.length; src += BlockPointer.SIZE, dst += BlockPointer.SIZE * 2)
		{
			System.arraycopy(mDirectory, src, newDirectory, dst, BlockPointer.SIZE);
		}

		mDirectory = newDirectory;
		mNodes = newNodes;
	}


	private void split(LeafNode aNode)
	{
		LeafNode lowNode = new LeafNode();
		lowNode.mDirty = true;
		lowNode.mSize = aNode.mSize - 1;
		lowNode.mMap = new ArrayMap(aNode.mMap.getCapacity());

		LeafNode highNode = new LeafNode();
		highNode.mDirty = true;
		highNode.mSize = aNode.mSize - 1;
		highNode.mMap = new ArrayMap(aNode.mMap.getCapacity());

		for (ArrayMapEntry entry : aNode.mMap)
		{
			if ((computeIndex(computeHash(entry.getKey())) & 1) == 0)
			{
				lowNode.mMap.put(entry, null);
			}
			else
			{
				highNode.mMap.put(entry, null);
			}
		}
	}


	private LeafNode loadNode(int aIndex)
	{
		LeafNode node = mNodes[aIndex];

		if (node == null)
		{
			BlockPointer bp = new BlockPointer();
			int offset = 0;
			int size = 0;
			for (int i = 0; offset < mDirectory.length;)
			{
				size = 1 << (int)BlockPointer.readUserData(mDirectory, offset);
				if (aIndex >= i && aIndex < aIndex + size)
				{
					bp.unmarshal(ByteArrayBuffer.wrap(mDirectory).position(offset));
					break;
				}
				i += size;
				offset += BlockPointer.SIZE * size;
			}
			node = new LeafNode();
			node.mBlockPointer = bp;
			node.mMap = new ArrayMap(readBlock(bp));
			node.mSize = size;
			Arrays.fill(mNodes, offset, offset + size, node);
		}

		return node;
	}


	private class LeafNode
	{
		BlockPointer mBlockPointer;
		ArrayMap mMap;
		int mSize;
		boolean mDirty;
	}


	void freeBlock(BlockPointer aBlockPointer)
	{
		mCost.mFreeBlock++;
		mCost.mFreeBlockBytes += aBlockPointer.getAllocatedBlocks();

		mBlockAccessor.freeBlock(aBlockPointer);
	}


	byte[] readBlock(BlockPointer aBlockPointer)
	{
		assert mPerformanceTool.tick("readBlock");

		mCost.mReadBlock++;
		mCost.mReadBlockBytes += aBlockPointer.getAllocatedBlocks();

		return mBlockAccessor.readBlock(aBlockPointer);
	}


	BlockPointer writeBlock(byte[] aContent, BlockType aBlockType, long aUserData)
	{
		assert mPerformanceTool.tick("writeBlock");

		BlockPointer blockPointer = mBlockAccessor.writeBlock(aContent, 0, aContent.length, mTransactionId.get(), aBlockType, aUserData);

		mCost.mWriteBlock++;
		mCost.mWriteBlockBytes += blockPointer.getAllocatedBlocks();

		return blockPointer;
	}


	private class ExtendibleHashTableMapIterator implements Iterator<ArrayMapEntry>
	{
		private ExtendibleHashTableIterator mIterator;
		private Iterator<ArrayMapEntry> mIt;


		public ExtendibleHashTableMapIterator()
		{
			mIterator = new ExtendibleHashTableIterator();
		}


		@Override
		public boolean hasNext()
		{
			if (mIt == null)
			{
				if (!mIterator.hasNext())
				{
					return false;
				}

				mIt = mIterator.next().mMap.iterator();
			}

			if (!mIt.hasNext())
			{
				mIt = null;
				return hasNext();
			}

			return true;
		}


		@Override
		public ArrayMapEntry next()
		{
			return mIt.next();
		}
	}


	private class ExtendibleHashTableIterator implements Iterator<LeafNode>
	{
		private int mIndex;
		private int mOffset;
		private LeafNode mNode;


		@Override
		public boolean hasNext()
		{
			if (mNode == null)
			{
				if (mOffset == mDirectory.length)
				{
					return false;
				}

				int size = 1 << (int)BlockPointer.readUserData(mDirectory, mOffset);

				BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(mDirectory).position(mOffset));

				mIndex += size;
				mOffset += BlockPointer.SIZE * size;

				mNode = new LeafNode();
				mNode.mBlockPointer = bp;
				mNode.mMap = new ArrayMap(readBlock(bp));
				mNode.mSize = size;
			}

			return true;
		}


		@Override
		public LeafNode next()
		{
			LeafNode n = mNode;
			mNode = null;
			return n;
		}
	}
}
