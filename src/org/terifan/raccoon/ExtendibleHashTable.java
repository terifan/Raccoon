package org.terifan.raccoon;

import java.io.IOException;
import org.terifan.raccoon.storage.BlockPointer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.terifan.raccoon.io.DatabaseIOException;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;
import org.terifan.security.messagedigest.MurmurHash3;


final class ExtendibleHashTable implements ITableImplementation
{
	private String mTableName;
	private TransactionGroup mTransactionId;
	private BlockAccessor mBlockAccessor;
	private BlockPointer mRootBlockPointer;
	private Directory mDirectory;
	private LeafNode[] mNodes;
	private long mHashSeed;
	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private boolean mChanged;
	private boolean mCommitChangesToBlockDevice;
	private int mModCount;

	private int mLeafSize = 128 * 1024;


	public ExtendibleHashTable()
	{
	}


	@Override
	public void create(IManagedBlockDevice aBlockDevice, TransactionGroup aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName)
	{
		mTableName = aTableName;
		mTransactionId = aTransactionId;
		mBlockAccessor = new BlockAccessor(aBlockDevice, aCompressionParam);
		mCommitChangesToBlockDevice = aCommitChangesToBlockDevice;

		Log.i("create table %s", mTableName);
		Log.inc();

		setupEmptyTable();

		mWasEmptyInstance = true;

		Log.dec();
	}


	@Override
	public void open(IManagedBlockDevice aBlockDevice, TransactionGroup aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName, byte[] aTableHeader)
	{
		mTableName = aTableName;
		mTransactionId = aTransactionId;
		mBlockAccessor = new BlockAccessor(aBlockDevice, aCompressionParam);
		mCommitChangesToBlockDevice = aCommitChangesToBlockDevice;

		Log.i("open table %s", mTableName);
		Log.inc();

		unmarshalHeader(aTableHeader);

		mDirectory = new Directory(mRootBlockPointer);
		mNodes = new LeafNode[1 << mDirectory.getPrefixLength()];

		Log.dec();
	}


	private byte[] marshalHeader()
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(BlockPointer.SIZE + 8 + 3);

		mRootBlockPointer.marshal(buffer);

		buffer.writeInt64(mHashSeed);

		mBlockAccessor.getCompressionParam().marshal(buffer);

		return buffer.trim().array();
	}


	private void unmarshalHeader(byte[] aTableHeader)
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.wrap(aTableHeader);

		mRootBlockPointer = new BlockPointer();
		mRootBlockPointer.unmarshal(buffer);

		mHashSeed = buffer.readInt64();

		CompressionParam compressionParam = new CompressionParam();
		compressionParam.unmarshal(buffer);

		mBlockAccessor.setCompressionParam(compressionParam);
	}


	@Override
	public boolean get(ArrayMapEntry aEntry)
	{
		checkOpen();

		return loadNode(computeIndex(aEntry)).mMap.get(aEntry);
	}


	@Override
	public ArrayMapEntry put(ArrayMapEntry aEntry)
	{
		checkOpen();

		if (aEntry.getKey().length + aEntry.getValue().length > getEntrySizeLimit())
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.getKey().length + ", value: " + aEntry.getValue().length + ", maximum: " + getEntrySizeLimit());
		}

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		mChanged = true;

		Result<ArrayMapEntry> oldEntry = new Result<>();

		int index = computeIndex(aEntry);

		LeafNode node = loadNode(index);

		for (;;)
		{
			if (node.mMap.put(aEntry, oldEntry))
			{
				node.mChanged = true;
				break;
			}

			if (node.mRangeBits == 0)
			{
				growDirectory();

				index = computeIndex(aEntry);
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

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> oldEntry = new Result<>();

		LeafNode node = loadNode(computeIndex(aEntry));

		boolean changed = node.mMap.remove(aEntry, oldEntry);

		mChanged |= changed;
		node.mChanged |= changed;

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
		iterator().forEachRemaining(list::add);

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

				freeBlock(mRootBlockPointer);

				mRootBlockPointer = mDirectory.writeBuffer();

				if (mCommitChangesToBlockDevice)
				{
					mBlockAccessor.getBlockDevice().commit();
				}

				mChanged = false;

				Log.i("table commit finished; root block is %s", mRootBlockPointer);

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

			if (node != null && node.mChanged)
			{
				freeBlock(node.mBlockPointer);

				node.mBlockPointer = writeBlock(node.mMap.array(), BlockType.LEAF, node.mRangeBits);
				node.mChanged = false;

				mDirectory.setBlockPointer(i, node.mBlockPointer);
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

			setupEmptyTable();
		}
		else
		{
			Log.d("rollback");

			mDirectory = new Directory(mRootBlockPointer);
			mNodes = new LeafNode[1 << mDirectory.getPrefixLength()];
		}

		mChanged = false;
	}


	@Override
	public void removeAll(Consumer<ArrayMapEntry> aConsumer)
	{
		checkOpen();

		int modCount = ++mModCount;

		new ExtendibleHashTableNodeIterator().forEachRemaining(node->
		{
			new ArrayMap(node.mMap.array()).forEach(aConsumer);

			freeBlock(node.mBlockPointer);

			node.mBlockPointer = null;
		});

		setupEmptyTable();

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

		new ExtendibleHashTableNodeIterator().forEachRemaining(node -> result.set(result.get() + node.mMap.size()));

		return result.get();
	}


	private void setupEmptyTable()
	{
		mDirectory = new Directory(mBlockAccessor.getBlockDevice().getBlockSize());
		mNodes = new LeafNode[1 << mDirectory.getPrefixLength()];

		LeafNode node = new LeafNode();
		node.mMap = new ArrayMap(mLeafSize);
		node.mRangeBits = mDirectory.getPrefixLength();
		node.mChanged = true;

		Arrays.fill(mNodes, node);

		// fake blockpointer required when growing directory first time
		new BlockPointer()
			.setBlockType(BlockType.SPECIAL)
			.setUserData(node.mRangeBits)
			.marshal(ByteArrayBuffer.wrap(mDirectory.mBuffer));

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

				long range = 1 << expected.mRangeBits;

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

		return mDirectory.integrityCheck();
	}


	@Override
	public int getEntrySizeLimit()
	{
		return 1024;
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
		aScanResult.tables++;

		new ExtendibleHashTableNodeIterator().forEachRemaining(node->scanLeaf(aScanResult, node));
	}


	private void scanLeaf(ScanResult aScanResult, LeafNode aNode)
	{
		aScanResult.enterLeafNode(aNode.mBlockPointer, aNode.mMap.array());
		aScanResult.leafNodes++;

		for (ArrayMapEntry entry : aNode.mMap)
		{
			aScanResult.records++;
			aScanResult.record();

			if (entry.getType() == TableInstance.TYPE_BLOB)
			{
				try
				{
					new LobByteChannelImpl(mBlockAccessor, null, entry.getValue(), LobOpenOption.READ).scan(aScanResult);
				}
				catch (IOException e)
				{
					throw new DatabaseIOException(e);
				}
			}
		}

		aScanResult.exitLeafNode();
	}


	private int computeIndex(ArrayMapEntry aEntry)
	{
		return (int)(0x7fffffff & (MurmurHash3.hash64(aEntry.getKey(), mHashSeed) >>> (64 - mDirectory.getPrefixLength())));
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
		byte[] newBuffer = new byte[2 * mDirectory.mBuffer.length];

		for (int src = 0, dst = 0; src < mNodes.length; src++, dst+=2)
		{
			newNodes[dst + 0] = mNodes[src];
			newNodes[dst + 1] = mNodes[src];
			System.arraycopy(mDirectory.mBuffer, src * BlockPointer.SIZE, newBuffer, dst * BlockPointer.SIZE, BlockPointer.SIZE);
		}

		for (int i = 0; i < newNodes.length; )
		{
			long range;
			if (newNodes[i] != null)
			{
				range = ++newNodes[i].mRangeBits;
			}
			else
			{
				range = 0;
			}

			i += 1 << range;
		}

		for (int i = 0; i < newNodes.length; )
		{
			int range = (int)BlockPointer.readUserData(newBuffer, i * BlockPointer.SIZE);
			range++;
			BlockPointer.writeUserData(newBuffer, i * BlockPointer.SIZE, range);

			i += 1 << range;
		}

		mDirectory.mBuffer = newBuffer;
		mDirectory.mPrefixLength++;
		mNodes = newNodes;

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

		long newRange = aNode.mRangeBits - 1;

		LeafNode lowNode = new LeafNode();
		lowNode.mChanged = true;
		lowNode.mRangeBits = newRange;
		lowNode.mMap = new ArrayMap(mLeafSize);

		LeafNode highNode = new LeafNode();
		highNode.mChanged = true;
		highNode.mRangeBits = newRange;
		highNode.mMap = new ArrayMap(mLeafSize);

		int partition = 1 << newRange;
		int mid = start + partition;

		assert aIndex >= start && aIndex < mid + partition;

		for (ArrayMapEntry entry : aNode.mMap)
		{
			if (computeIndex(entry) < mid)
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
				range = 1 << mNodes[index].mRangeBits;

				if (aIndex >= index && aIndex < index + range)
				{
					return mNodes[index];
				}
			}
			else
			{
				range = 1 << mDirectory.getRangeBits(index);

				if (aIndex >= index && aIndex < index + range)
				{
					BlockPointer bp = mDirectory.readBlockPointer(index);

					node = new LeafNode();
					node.mBlockPointer = bp;
					node.mMap = new ArrayMap(readBlock(bp));
					node.mRangeBits = mDirectory.getRangeBits(index);

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
			mBlockAccessor.freeBlock(aBlockPointer);
		}
	}


	private byte[] readBlock(BlockPointer aBlockPointer)
	{
		return mBlockAccessor.readBlock(aBlockPointer);
	}


	private BlockPointer writeBlock(byte[] aContent, BlockType aBlockType, long aUserData)
	{
		return mBlockAccessor.writeBlock(aContent, 0, aContent.length, mTransactionId.get(), aBlockType, aUserData);
	}


	private class ExtendibleHashTableMapIterator implements Iterator<ArrayMapEntry>
	{
		private ExtendibleHashTableNodeIterator mIterator;
		private Iterator<ArrayMapEntry> mIt;


		public ExtendibleHashTableMapIterator()
		{
			mIterator = new ExtendibleHashTableNodeIterator();
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


	private class ExtendibleHashTableNodeIterator implements Iterator<LeafNode>
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

				mIndex += 1 << mNode.mRangeBits;
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
		BlockPointer mBlockPointer;
		ArrayMap mMap;
		long mRangeBits;
		boolean mChanged;
	}


	private class Directory
	{
		private byte[] mBuffer;
		private int mPrefixLength;


		private Directory(int aCapacity)
		{
			mBuffer = new byte[aCapacity];
			mPrefixLength = (int)Math.ceil(Math.log(mBuffer.length / BlockPointer.SIZE) / Math.log(2));
		}


		private Directory(BlockPointer aRootNodeBlockPointer)
		{
			mBuffer = readBlock(aRootNodeBlockPointer);
			mPrefixLength = (int)Math.ceil(Math.log(mBuffer.length / BlockPointer.SIZE) / Math.log(2));
		}


		private int getPrefixLength()
		{
			return mPrefixLength;
		}


		private BlockPointer writeBuffer()
		{
			return writeBlock(mBuffer, BlockType.INDEX, 0L);
		}


		private void setBlockPointer(int aIndex, BlockPointer aBlockPointer)
		{
			aBlockPointer.marshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));
		}


		private long getRangeBits(int aIndex)
		{
			return BlockPointer.readUserData(mBuffer, aIndex * BlockPointer.SIZE);
		}


		private BlockPointer readBlockPointer(int aIndex)
		{
			BlockPointer bp = new BlockPointer();
			bp.unmarshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));
			return bp;
		}


		private String integrityCheck()
		{
			for (int offset = 0; offset < mBuffer.length; )
			{
				BlockType blockType = BlockPointer.readBlockType(mBuffer, offset);

				if (blockType != BlockType.LEAF && blockType != BlockType.SPECIAL)
				{
					return "ExtendibleHashTable directory has bad block type";
				}

				long rangeBits = BlockPointer.readUserData(mBuffer, offset);
				long range = BlockPointer.SIZE * (1 << rangeBits);

				if (range <= 0 || offset + range > mBuffer.length)
				{
					return "ExtendibleHashTable directory has bad range";
				}

				offset += BlockPointer.SIZE;
				range -= BlockPointer.SIZE;

				for (long j = range; --j >= 0; offset++)
				{
					if (mBuffer[offset] != 0)
					{
						return "ExtendibleHashTable directory error at " + offset;
					}
				}
			}

			return null;
		}
	}
}