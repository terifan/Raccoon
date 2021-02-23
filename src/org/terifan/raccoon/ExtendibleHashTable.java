package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.ByteArrayUtil;
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

	private LeafNode[] mNodes;
	private int mPrefixLength;

	private int mLeafSize = 64*1024;


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

		setupEmptyTable();

		mWasEmptyInstance = true;

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
		mPrefixLength = (int)Math.ceil(Math.log(mDirectory.length / BlockPointer.SIZE) / Math.log(2));
		mNodes = new LeafNode[1 << mPrefixLength];

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
		node.mDirty = true;

		while (!node.mMap.put(aEntry, oldEntry))
		{
			if (node.mRange == 0)
			{
				growDirectory();
				index <<= 1;
			}
			node = splitNode(index, node);
		}

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return oldEntry.get();
	}


	@Override
	public ArrayMapEntry remove(ArrayMapEntry aEntry)
	{
		checkOpen();

		assert mPerformanceTool.tick("remove");

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> oldEntry = new Result<>();

		int index = computeIndex(computeHash(aEntry.getKey()));

		LeafNode node = loadNode(index);

		boolean modified = node.mMap.remove(aEntry, oldEntry);

		mChanged |= modified;
		node.mDirty |= modified;

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return oldEntry.get();
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

				assert integrityCheck() == null : integrityCheck();

				freeBlock(mRootNodeBlockPointer);

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
		ByteArrayBuffer buf = ByteArrayBuffer.wrap(mDirectory);

		for (int i = 0; i < mNodes.length; i++)
		{
			LeafNode node = mNodes[i];

			if (node != null && node.mDirty)
			{
				freeBlock(node.mBlockPointer);

				node.mBlockPointer = writeBlock(node.mMap.array(), BlockType.LEAF, node.mRange);
				node.mBlockPointer.marshal(buf.position(i * BlockPointer.SIZE));
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

			// fake blockpointer required for growing directory
			new BlockPointer()
				.setBlockType(BlockType.FREE)
				.setUserData(mPrefixLength)
				.marshal(ByteArrayBuffer.wrap(mDirectory));
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
		checkOpen();

		int modCount = ++mModCount;

		BlockPointer bp = new BlockPointer();
		ByteArrayBuffer buf = ByteArrayBuffer.wrap(mDirectory);

		for (int i = 0; i < mNodes.length; i++)
		{
			if (mNodes[i] != null)
			{
				freeBlock(mNodes[i].mBlockPointer);
			}

			if (BlockPointer.readBlockType(mDirectory, i * BlockPointer.SIZE) != BlockType.FREE)
			{
				freeBlock(bp.unmarshal(buf.position(i * BlockPointer.SIZE)));
			}
		}

		setupEmptyTable();

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


	private void setupEmptyTable()
	{
		mHashSeed = new SecureRandom().nextLong();

		mDirectory = new byte[mBlockAccessor.getBlockDevice().getBlockSize()];
		mPrefixLength = (int)Math.ceil(Math.log(mDirectory.length / BlockPointer.SIZE) / Math.log(2));
		mNodes = new LeafNode[1 << mPrefixLength];

		LeafNode node = new LeafNode();
		node.mMap = new ArrayMap(mLeafSize);
		node.mRange = mPrefixLength;
		node.mDirty = true;

		Arrays.fill(mNodes, node);

		// fake blockpointer required when growing directory first time
		new BlockPointer()
			.setBlockType(BlockType.LEAF)
			.setUserData(mPrefixLength)
			.marshal(ByteArrayBuffer.wrap(mDirectory));

		mChanged = true;
	}


	@Override
	public String integrityCheck()
	{
		Log.i("integrity check");

		for (int index = 0; index < mNodes.length;)
		{
			if (mNodes[index] != null)
			{
				LeafNode expected = mNodes[index];

				long range = 1 << expected.mRange;

				if (range <= 0 || index + range > mNodes.length)
				{
					return "ExtendibleHashTable node has bad range";
				}

				for (long j = range; --j >= 0; index++)
				{
					if (mNodes[index] != expected)
					{
						return "ExtendibleHashTable node error at " + index;
					}
				}
			}
			else
			{
				index++;
			}
		}

		for (int offset = 0; offset < mDirectory.length; )
		{
			if (BlockPointer.readBlockType(mDirectory, offset) != BlockType.LEAF)
			{
				Log.hexDump(mDirectory);
				return "ExtendibleHashTable directory has bad block type";
			}

			long range = BlockPointer.SIZE * (1 << BlockPointer.readUserData(mDirectory, offset));

			if (range <= 0 || offset + range > mDirectory.length)
			{
				return "ExtendibleHashTable directory has bad range";
			}

			offset += BlockPointer.SIZE;
			range -= BlockPointer.SIZE;

			for (long j = range; --j >= 0; offset++)
			{
				if (mDirectory[offset] != 0)
				{
					return "ExtendibleHashTable directory error at " + offset;
				}
			}
		}

		return null;
	}


	@Override
	public int getEntryMaximumLength()
	{
		return 1024;
	}


	private void visit(HashTreeTableVisitor aVisitor)
	{
//		mRootNode.visit(aVisitor);
	}


	private void checkOpen()
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


	private long computeHash(byte[] aKey)
	{
		return MurmurHash3.hash64(aKey, mHashSeed);
	}


	private int computeIndex(long aHash)
	{
		return (int)(0x7fffffff & (aHash >>> (64 - mPrefixLength)));
	}


	/*
	 * 000 +----- 1    0000 +----- 2
	 * 001 +----- 1    0001 |
	 * 010 +----- 2    0010 +----- 2
	 * 011 |           0011 |
	 * 100 +----- 4    0100 +----- 4
	 * 101 |           0101 |
	 * 110 |           0110 |
	 * 111 |           0111 |
	 *     	           1000 +----- 8
	 *     	           1001 |
	 *     	           1010 |
	 *                 1011 |
	 *     	           1100 |
	 *     	           1101 |
	 *     	           1110 |
	 *     	           1111 |
	 */
	private void growDirectory()
	{
		LeafNode[] newNodes = new LeafNode[2 * mNodes.length];
		byte[] newDirectory = new byte[2 * mDirectory.length];

		for (int src = 0, dst = 0; src < mNodes.length; src++, dst+=2)
		{
			newNodes[dst + 0] = mNodes[src];
			newNodes[dst + 1] = mNodes[src];
			System.arraycopy(mDirectory, src * BlockPointer.SIZE, newDirectory, dst * BlockPointer.SIZE, BlockPointer.SIZE);
		}

		for (int i = 0; i < newNodes.length; )
		{
			long range;
			if (newNodes[i] != null)
			{
				range = ++newNodes[i].mRange;
			}
			else
			{
				range = 0;
			}

			i += 1 << range;
		}

		for (int i = 0; i < newNodes.length; )
		{
			int range = (int)BlockPointer.readUserData(newDirectory, i * BlockPointer.SIZE);
			range++;
			BlockPointer.writeUserData(newDirectory, i * BlockPointer.SIZE, range);

			i += 1 << range;
		}

		mDirectory = newDirectory;
		mNodes = newNodes;
		mPrefixLength++;

		assert integrityCheck() == null : integrityCheck();
	}


	/*
	 * 000 +----- 4 [a,b,c,d]   000 +----- 2 [a,b]
	 * 001 |                    001 |
	 * 010 |                    010 +----- 2 [c,d]
	 * 011 |                    011 |
	 * 100 +----- 4 [e,f,g,h]   100 +----- 4 [e,f,g,h]
	 * 101 |                    101 |
	 * 110 |                    110 |
	 * 111 |                    111 |
	 */
	private LeafNode splitNode(int aIndex, LeafNode aNode)
	{
		assert integrityCheck() == null : integrityCheck();

		int start = aIndex;
		while (start > 0 && mNodes[start - 1] == mNodes[aIndex])
		{
			start--;
		}

		long newRange = aNode.mRange - 1;

		LeafNode lowNode = new LeafNode();
		lowNode.mDirty = true;
		lowNode.mRange = newRange;
		lowNode.mMap = new ArrayMap(mLeafSize);

		LeafNode highNode = new LeafNode();
		highNode.mDirty = true;
		highNode.mRange = newRange;
		highNode.mMap = new ArrayMap(mLeafSize);

		int partition = 1 << newRange;
		int mid = start + partition;

		assert aIndex >= start && aIndex < mid + partition;

		for (ArrayMapEntry entry : aNode.mMap)
		{
			if (computeIndex(computeHash(entry.getKey())) < mid)
			{
				lowNode.mMap.put(entry, null);
			}
			else
			{
				highNode.mMap.put(entry, null);
			}
		}

		freeBlock(aNode.mBlockPointer);

		Arrays.fill(mNodes, start, mid, lowNode);
		Arrays.fill(mNodes, mid, mid + partition, highNode);

		assert integrityCheck() == null : integrityCheck();

		return aIndex < mid ? lowNode : highNode;
	}


	private LeafNode loadNode(int aIndex)
	{
		LeafNode node = mNodes[aIndex];

		if (node != null)
		{
			return node;
		}

		int range = 0;

		for (int index = 0; index < mNodes.length;)
		{
			if (mNodes[index] != null)
			{
				range = 1 << mNodes[index].mRange;

				if (aIndex >= index && aIndex < index + range)
				{
					return mNodes[index];
				}
			}
			else
			{
				int offset = index * BlockPointer.SIZE;

				long rangeBits = BlockPointer.readUserData(mDirectory, offset);
				range = 1 << rangeBits;

				if (aIndex >= index && aIndex < index + range)
				{
					BlockPointer bp = new BlockPointer();
					bp.unmarshal(ByteArrayBuffer.wrap(mDirectory).position(offset));

					node = new LeafNode();
					node.mBlockPointer = bp;
					node.mMap = new ArrayMap(readBlock(bp));
					node.mRange = rangeBits;

					Arrays.fill(mNodes, index, index + range, node);
					return node;
				}
			}

			index += range;
		}

		throw new IllegalStateException();
	}


	private void freeBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer != null)
		{
			mCost.mFreeBlock++;
			mCost.mFreeBlockBytes += aBlockPointer.getAllocatedBlocks();

			mBlockAccessor.freeBlock(aBlockPointer);
		}
	}


	private byte[] readBlock(BlockPointer aBlockPointer)
	{
		assert mPerformanceTool.tick("readBlock");

		mCost.mReadBlock++;
		mCost.mReadBlockBytes += aBlockPointer.getAllocatedBlocks();

		return mBlockAccessor.readBlock(aBlockPointer);
	}


	private BlockPointer writeBlock(byte[] aContent, BlockType aBlockType, long aUserData)
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
		private LeafNode mNode;


		@Override
		public boolean hasNext()
		{
			if (mNode == null)
			{
				if (mIndex == mNodes.length)
				{
					return false;
				}

				mNode = loadNode(mIndex);

				mIndex += 1 << mNode.mRange;
			}

			return true;
		}


		@Override
		public LeafNode next()
		{
			LeafNode tmp = mNode;
			mNode = null;
			return tmp;
		}
	}


	private class LeafNode
	{
		long id = System.nanoTime();

		BlockPointer mBlockPointer;
		ArrayMap mMap;
		long mRange;
		boolean mDirty;


		@Override
		public String toString()
		{
			return "[" + id + " "+ String.format("%08x", hashCode()) + ", range=" + mRange + ", dirty=" + mDirty + "]";
		}
	}
}