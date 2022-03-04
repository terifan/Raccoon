package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.terifan.raccoon.ArrayMap.NearResult;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


class BTreeTableImplementation extends TableImplementation
{
	static byte[] POINTER_PLACEHOLDER = new byte[BlockPointer.SIZE];
	static int mIndexSize = 800;
	static int mLeafSize = 512;

	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private boolean mChanged;
	private int mModCount;

	private BTreeNode mRoot;


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

			Log.dec();
		}
	}


	private byte[] marshalHeader()
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(BlockPointer.SIZE + 3);

		mRoot.mBlockPointer.setBlockType(mRoot instanceof BTreeIndex ? BlockType.INDEX : BlockType.LEAF);
		mRoot.mBlockPointer.marshal(buffer);

		mBlockAccessor.getCompressionParam().marshal(buffer);

		return buffer.trim().array();
	}


	private void unmarshalHeader(byte[] aTableHeader)
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.wrap(aTableHeader);

		CompressionParam compressionParam = new CompressionParam();
		compressionParam.unmarshal(buffer);

		mBlockAccessor.setCompressionParam(compressionParam);

		BlockPointer bp = new BlockPointer();
		bp.unmarshal(buffer);

		mRoot = BTreeNode.newNode(bp);
		mRoot.mBlockPointer = bp;
		mRoot.mMap = new ArrayMap(readBlock(bp));
	}


	@Override
	public boolean get(ArrayMapEntry aEntry)
	{
		checkOpen();

		return mRoot.mMap.get(aEntry);
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

		Result<ArrayMapEntry> result = new Result<>();

		MarshalledKey key = new MarshalledKey(aEntry.getKey());

		if (mRoot.put(null, aEntry, key, result))
		{
			if (mRoot instanceof BTreeLeaf)
			{
				System.out.println("!");
				mRoot = ((BTreeLeaf)mRoot).upgrade();
			}
			else
			{
				System.out.println("*");
				mRoot = ((BTreeIndex)mRoot).grow();
			}
		}

		mChanged = true;

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return result.get();
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

		return new BTreeEntryIterator(mRoot);
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

				assert integrityCheck() == null : integrityCheck();

				freeBlock(mRoot.mBlockPointer);

				mRoot.mBlockPointer = writeBlock(mRoot.mMap.array(), BlockType.LEAF);

				if (mCommitChangesToBlockDevice)
				{
					mBlockAccessor.getBlockDevice().commit();
				}

				mChanged = false;

				Log.i("table commit finished; root block is %s", mRoot.mBlockPointer);

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

			setupEmptyTable();
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

		setupEmptyTable();

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
			mRoot = null;
			mBlockAccessor = null;
		}
	}


	@Override
	public int size()
	{
		checkOpen();

		Result<Integer> result = new Result<>(0);

		new BTreeNodeIterator(mRoot).forEachRemaining(node -> result.set(result.get() + node.mMap.size()));

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

		scan(mRoot, aScanResult);
	}


	private void scan(BTreeNode aNode, ScanResult aScanResult)
	{
		if (aNode instanceof BTreeIndex)
		{
			boolean first = true;
			aScanResult.log.append("'");
			for (ArrayMapEntry entry : aNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(":");
				}
				first = false;
				aScanResult.log.append(new String(entry.getKey()).replaceAll("[^\\w]*", "").replace("'", ""));
			}
			aScanResult.log.append("'[");

			first = true;
			for (ArrayMapEntry entry : aNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(",");
				}
				first = false;

				BTreeNode node = ((BTreeIndex)aNode).mChildren.get(new MarshalledKey(entry.getKey()));

				if (node == null)
				{
					throw new IllegalStateException();
//					BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(entry.getValue()));
//
//					node = BTreeNode.newNode(bp);
//					node.mMap = new ArrayMap(readBlock(bp));
				}

				scan(node, aScanResult);
			}

			aScanResult.log.append("]");
		}
		else
		{
			aScanResult.log.append("[");
			boolean first = true;
			for (ArrayMapEntry entry : aNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(",");
				}
				first = false;
				aScanResult.log.append("'" + new String(entry.getKey()).replaceAll("[^\\w]*", "") + "'");
			}
			aScanResult.log.append("]");
		}
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


	private BlockPointer writeBlock(byte[] aContent, BlockType aBlockType)
	{
		return mBlockAccessor.writeBlock(aContent, 0, aContent.length, mTransactionGroup.get(), aBlockType, 0);
	}


	private void setupEmptyTable()
	{
		mRoot = new BTreeLeaf();
		mRoot.mMap = new ArrayMap(mLeafSize);
	}
}