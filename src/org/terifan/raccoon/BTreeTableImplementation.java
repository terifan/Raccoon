package org.terifan.raccoon;

import java.io.IOException;
import org.terifan.raccoon.storage.BlockPointer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.terifan.raccoon.io.DatabaseIOException;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;
import org.terifan.security.messagedigest.MurmurHash3;


final class BTreeTableImplementation extends TableImplementation
{
	private final static int PAGE_HEADER_SIZE = 1;

	private BlockPointer mRootBlockPointer;
	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private boolean mChanged;
	private int mModCount;

	private ArrayMap mRootNode;

	private int mIndexSize = 512;
	private int mLeafSize = 512;
//	private int mIndexSize = 8 * 1024;
//	private int mLeafSize = 8 * 1024;


	public BTreeTableImplementation(IManagedBlockDevice aBlockDevice, TransactionGroup aTransactionGroup, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName)
	{
		super(aBlockDevice, aTransactionGroup, aCommitChangesToBlockDevice, aCompressionParam, aTableParam, aTableName);
	}


	@Override
	public void openOrCreateTable(byte[] aTableHeader)
	{
		if (aTableHeader == null)
		{
			Log.i("create table %s", mTableName);
			Log.inc();

			setupEmptyTable();

			mWasEmptyInstance = true;

			Log.dec();
		}
		else
		{
			Log.i("open table %s", mTableName);
			Log.inc();

			unmarshalHeader(aTableHeader);

//			mDirectory = new Directory(mRootBlockPointer);
//			mNodes = new LeafNode[1 << mDirectory.getPrefixLength()];

			Log.dec();
		}
	}


	private byte[] marshalHeader()
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(BlockPointer.SIZE + 3);

		mRootBlockPointer.marshal(buffer);

		mBlockAccessor.getCompressionParam().marshal(buffer);

		return buffer.trim().array();
	}


	private void unmarshalHeader(byte[] aTableHeader)
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.wrap(aTableHeader);

		mRootBlockPointer = new BlockPointer();
		mRootBlockPointer.unmarshal(buffer);

		CompressionParam compressionParam = new CompressionParam();
		compressionParam.unmarshal(buffer);

		mBlockAccessor.setCompressionParam(compressionParam);
	}


	@Override
	public boolean get(ArrayMapEntry aEntry)
	{
		checkOpen();

//		return loadNode(computeIndex(aEntry)).mMap.get(aEntry);
		return false;
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

		ArrayMapEntry entry = new ArrayMapEntry(aEntry.getKey());

		ArrayMap.SearchResult state = mRootNode.nearest(entry);

		System.out.println(state + " '" + new String(entry.getKey()).trim() + "'");

		Result<ArrayMapEntry> r = new Result<>();

		mRootNode.put(aEntry, r);

		mRootNode.forEach(e->System.out.println("**" + new String(e.getKey()).replaceAll("[^\\w]*", "")));


//		0:
//		Index[cafe=1, filip=2, steve=3, *=4]
//
//		1:
//		Index[babe=5, *=6]
//
//		2:
//		Data[.....]

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

//		return entry.get();
		return null;
	}


	@Override
	public ArrayMapEntry remove(ArrayMapEntry aEntry)
	{
		checkOpen();

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> oldEntry = new Result<>();

//		LeafNode node = loadNode(computeIndex(aEntry));
//
//		boolean changed = node.mMap.remove(aEntry, oldEntry);
//
//		mChanged |= changed;
//		node.mChanged |= changed;

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return oldEntry.get();
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		checkOpen();

		return new TableIterator();
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

				flushImpl();

				assert integrityCheck() == null : integrityCheck();

				freeBlock(mRootBlockPointer);

//				mRootBlockPointer = mDirectory.writeBuffer();

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


	private void flushImpl()
	{
//		for (int i = 0; i < mNodes.length; i++)
//		{
//			LeafNode node = mNodes[i];
//
//			if (node != null && node.mChanged)
//			{
//				freeBlock(node.mBlockPointer);
//
//				node.mBlockPointer = writeBlock(node.mMap.array(), BlockType.LEAF, node.mRangeBits);
//				node.mChanged = false;
//
//				mDirectory.setBlockPointer(i, node.mBlockPointer);
//			}
//		}
	}


	@Override
	public long flush()
	{
		return 0l;
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

//			setupEmptyTable();
		}
		else
		{
			Log.d("rollback");

//			mDirectory = new Directory(mRootBlockPointer);
//			mNodes = new LeafNode[1 << mDirectory.getPrefixLength()];
		}

		mChanged = false;
	}


	@Override
	public void removeAll(Consumer<ArrayMapEntry> aConsumer)
	{
		checkOpen();

		int modCount = ++mModCount;

//		new ExtendibleHashTableNodeIterator().forEachRemaining(node->
//		{
//			new ArrayMap(node.mMap.array()).forEach(aConsumer);
//
//			freeBlock(node.mBlockPointer);
//
//			node.mBlockPointer = null;
//		});
//
//		setupEmptyTable();

		assert mModCount == modCount : "concurrent modification";
	}


	/**
	 * Clean-up resources
	 */
	@Override
	public void close()
	{
		if (!mClosed)
		{
			mClosed = true;
//			mDirectory = null;
//			mNodes = null;
			mBlockAccessor = null;
		}
	}


	@Override
	public int size()
	{
		checkOpen();

		Result<Integer> result = new Result<>(0);

		new NodeIterator().forEachRemaining(node -> result.set(result.get() + node.mMap.size()));

		return result.get();
	}


	@Override
	public String integrityCheck()
	{
		Log.i("integrity check");

//		for (int index = 0; index < mNodes.length;)
//		{
//			if (mNodes[index] != null)
//			{
//				LeafNode expected = mNodes[index];
//
//				long range = 1 << expected.mRangeBits;
//
//				if (range <= 0 || index + range > mNodes.length)
//				{
//					return "ExtendibleHashTable node has bad range";
//				}
//
//				for (long j = range; --j >= 0; index++)
//				{
//					if (mNodes[index] != expected)
//					{
//						return "ExtendibleHashTable node error at " + index;
//					}
//				}
//			}
//			else
//			{
//				index++;
//			}
//		}
//
//		return mDirectory.integrityCheck();

		return null;
	}


	@Override
	public int getEntrySizeLimit()
	{
		return mLeafSize / 2;
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

//		new TableIterator().forEachRemaining(node->scanLeaf(aScanResult, node));
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
		return mBlockAccessor.writeBlock(aContent, 0, aContent.length, mTransactionGroup.get(), aBlockType, aUserData);
	}


	private void setupEmptyTable()
	{
		mRootNode = new ArrayMap(new byte[mLeafSize], PAGE_HEADER_SIZE, mLeafSize - PAGE_HEADER_SIZE);
	}


	private class TableIterator implements Iterator<ArrayMapEntry>
	{
		private NodeIterator mIterator;
		private Iterator<ArrayMapEntry> mIt;


		public TableIterator()
		{
			mIterator = new NodeIterator();
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


	private class NodeIterator implements Iterator<LeafNode>
	{
		private int mIndex;
		private LeafNode mNode;


		@Override
		public boolean hasNext()
		{
//			if (mNode == null)
//			{
//				if (mIndex == mNodes.length)
//				{
//					return false;
//				}
//
//				mNode = loadNode(mIndex);
//
//				mIndex += 1 << mNode.mRangeBits;
//			}

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
}