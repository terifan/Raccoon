package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.terifan.raccoon.ArrayMap.NearResult;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


class BTreeTableImplementation extends TableImplementation
{
	private final static byte[] POINTER_PLACEHOLDER = new byte[BlockPointer.SIZE];

	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private boolean mChanged;
	private int mModCount;

	private BTreeNode mRoot;

	private int mIndexSize = 512;
	private int mLeafSize = 512;


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

		mRoot.mBlockPointer.setBlockType(mRoot.mIndexNode ? BlockType.INDEX : BlockType.LEAF);
		mRoot.mBlockPointer.marshal(buffer);

		mBlockAccessor.getCompressionParam().marshal(buffer);

		return buffer.trim().array();
	}


	private void unmarshalHeader(byte[] aTableHeader)
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.wrap(aTableHeader);

		mRoot = new BTreeNode(null, false);
		mRoot.mBlockPointer = new BlockPointer();
		mRoot.mBlockPointer.unmarshal(buffer);

		CompressionParam compressionParam = new CompressionParam();
		compressionParam.unmarshal(buffer);

		mBlockAccessor.setCompressionParam(compressionParam);

		mRoot.mMap = new ArrayMap(readBlock(mRoot.mBlockPointer));
		mRoot.mIndexNode = mRoot.mBlockPointer.getBlockType() == BlockType.INDEX;

		if (mRoot.mIndexNode)
		{
			mRoot.mChildren = new HashMap<>();
		}
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

		if (putImpl(mRoot, aEntry, new ArrayMapEntry(aEntry.getKey()), result))
		{
			ArrayMap[] maps = mRoot.mMap.split();

			if (mRoot.mIndexNode)
			{
			}
			else
			{
				ArrayMapEntry a = maps[1].get(0, new ArrayMapEntry());
				ArrayMapEntry b = new ArrayMapEntry("".getBytes(), POINTER_PLACEHOLDER, (byte)0);

				BTreeNode na = new BTreeNode(maps[0], false);
				BTreeNode nb = new BTreeNode(maps[1], false);

				mRoot = new BTreeNode(new ArrayMap(mIndexSize), true);

				mRoot.mMap.put(new ArrayMapEntry(a.getKey(), POINTER_PLACEHOLDER, (byte)0), null);
				mRoot.mMap.put(new ArrayMapEntry(b.getKey(), POINTER_PLACEHOLDER, (byte)0), null);

				mRoot.mChildren.put(new MarshalledKey(a.getKey()), na);
				mRoot.mChildren.put(new MarshalledKey(b.getKey()), nb);
			}
		}

		mChanged = true;

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return result.get();
	}


	private boolean putImpl(BTreeNode aNode, ArrayMapEntry aEntry, ArrayMapEntry aKey, Result<ArrayMapEntry> aResult)
	{
		if (aNode.mBlockPointer != null && aNode.mBlockPointer.getBlockType() == BlockType.INDEX)
		{
			NearResult state = aNode.mMap.nearest(aKey);

			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(aKey.getValue()));

			BTreeNode node = new BTreeNode(new ArrayMap(readBlock(bp)), bp.getBlockType() == BlockType.INDEX);
			node.mIndexNode = bp.getBlockType() == BlockType.INDEX;

			if (putImpl(node, aEntry, aKey, aResult))
			{
				ArrayMap[] maps = node.mMap.split();
				System.out.println("#");
			}

			return false;
		}

		return !aNode.mMap.insert(aEntry, aResult);
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
		if (aNode.mIndexNode)
		{
			aScanResult.log.append("index[");

			boolean first = true;
			for (ArrayMapEntry entry : aNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(",");
				}
				first = false;

				System.out.println("*"+new String(entry.getKey()).replaceAll("[^\\w]*", ""));

				BTreeNode node = aNode.mChildren.get(new MarshalledKey(entry.getKey()));

				aScanResult.log.append(new String(entry.getKey()).replaceAll("[^\\w]*", "") + "=");

				if (node == null)
				{
					BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(entry.getValue()));

					node = new BTreeNode(new ArrayMap(readBlock(bp)), bp.getBlockType() == BlockType.INDEX);
				}

				scan(node, aScanResult);
			}

			aScanResult.log.append("]");
		}
		else
		{
			aScanResult.log.append("leaf"+aNode.mMap);
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
		mRoot = new BTreeNode(new ArrayMap(mLeafSize), false);
	}
}