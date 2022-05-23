package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


class BTreeTableImplementation extends TableImplementation
{
	static byte[] POINTER_PLACEHOLDER = new BlockPointer().setBlockType(BlockType.ILLEGAL).marshal(ByteArrayBuffer.alloc(BlockPointer.SIZE)).array();
	static int mIndexSize = 512;
	static int mLeafSize = 1024;

	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private boolean mChanged;
	private int mModCount;
	private BTreeNode mRoot;

	private TreeMap<Long, BTreeNode> mLRU;
	private long mNodeCounter;


	public BTreeTableImplementation(IManagedBlockDevice aBlockDevice, TransactionGroup aTransactionGroup, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName)
	{
		super(aBlockDevice, aTransactionGroup, aCommitChangesToBlockDevice, aCompressionParam, aTableParam, aTableName);

		mLRU = new TreeMap<>();
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

		mBlockAccessor.getCompressionParam().marshal(buffer);

		mRoot.mBlockPointer.setBlockType(mRoot instanceof BTreeIndex ? BlockType.INDEX : BlockType.LEAF);
		mRoot.mBlockPointer.marshal(buffer);

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

		mRoot = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(this) : new BTreeLeaf(this);
		mRoot.mBlockPointer = bp;
		mRoot.mMap = new ArrayMap(readBlock(bp));
	}


	@Override
	public boolean get(ArrayMapEntry aEntry)
	{
		checkOpen();

aEntry.setKey(Arrays.copyOfRange(aEntry.getKey(), 2, aEntry.getKey().length));

		return mRoot.get(new MarshalledKey(aEntry.getKey()), aEntry);
	}


	@Override
	public ArrayMapEntry put(ArrayMapEntry aEntry)
	{
		checkOpen();

aEntry.setKey(Arrays.copyOfRange(aEntry.getKey(), 2, aEntry.getKey().length));

		aEntry = new ArrayMapEntry(new MarshalledKey(aEntry.getKey()).marshall(), aEntry.getValue(), aEntry.getType());

		if (aEntry.getKey().length + aEntry.getValue().length > getEntrySizeLimit())
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.getKey().length + ", value: " + aEntry.getValue().length + ", maximum: " + getEntrySizeLimit());
		}

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> result = new Result<>();

		if (mRoot.put(null, new MarshalledKey(aEntry.getKey()), aEntry, result))
		{
			if (mRoot instanceof BTreeLeaf)
			{
				mRoot = ((BTreeLeaf)mRoot).upgrade();
			}
			else
			{
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

aEntry.setKey(Arrays.copyOfRange(aEntry.getKey(), 2, aEntry.getKey().length));

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
				Log.i("commit table");
				Log.inc();

				assert integrityCheck() == null : integrityCheck();

				mRoot.commit();

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

		return integrityCheck(mRoot);
	}


	private String integrityCheck(BTreeNode aNode)
	{
		if (aNode instanceof BTreeIndex)
		{
			BTreeIndex indexNode = (BTreeIndex)aNode;

			for (ArrayMapEntry entry : indexNode.mMap)
			{
				MarshalledKey key = MarshalledKey.unmarshall(entry.getKey());

				BTreeNode node = indexNode.mChildren.get(key);

				if (node != null)
				{
//					String s = new String(key.marshall()).replaceAll("[^\\w]*", "").replace("'", "").replace("_", "");

					System.out.println(node.mNodeId);

					String result = integrityCheck(node);

					if (result != null)
					{
						return result;
					}
				}
			}
		}
		else
		{
			BTreeLeaf leafNode = (BTreeLeaf)aNode;

			for (ArrayMapEntry entry : leafNode.mMap)
			{
			}
		}

		return null;
	}


	@Override
	public int getEntrySizeLimit()
	{
		return mLeafSize / 3;
	}


	private void checkOpen()
	{
		if (mClosed)
		{
			throw new IllegalStateException("Table is closed");
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
			BTreeIndex indexNode = (BTreeIndex)aNode;

			boolean first = true;
			aScanResult.log.append("'");
			for (ArrayMapEntry entry : indexNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(":");
				}
				first = false;
				MarshalledKey key = MarshalledKey.unmarshall(entry.getKey());
				String s = new String(key.marshall()).replaceAll("[^\\w]*", "").replace("'", "").replace("_", "");
				aScanResult.log.append(s.isEmpty()?"*":s);
			}
			aScanResult.log.append("'");
			aScanResult.log.append(indexNode.mModified ? "#f00" : "#0f0");

			first = true;
			aScanResult.log.append("[");
			for (ArrayMapEntry entry : indexNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(",");
				}
				first = false;
				MarshalledKey key = MarshalledKey.unmarshall(entry.getKey());
				BTreeNode node = indexNode.mChildren.get(key);

				if (node == null)
				{
//					BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(entry.getValue()));
//
//					node = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(this) : new BTreeLeaf(this);
//					node.mBlockPointer = bp;
//					node.mMap = new ArrayMap(readBlock(bp));
//
//					indexNode.mChildren.put(key, node);

					aScanResult.log.append("'*'");
				}
				else
				{
					scan(node, aScanResult);
				}
			}
			aScanResult.log.append("]");
		}
		else
		{
			BTreeLeaf leafNode = (BTreeLeaf)aNode;

			aScanResult.log.append("[");
			boolean first = true;
			for (ArrayMapEntry entry : leafNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(",");
				}
				first = false;
				aScanResult.log.append("'" + new String(entry.getKey()).replaceAll("[^\\w]*", "").replace("_", "") + "'");
			}
			aScanResult.log.append("]");
			aScanResult.log.append(leafNode.mModified ? "#f00" : "#0f0");
		}
	}


	protected void freeBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer != null)
		{
			mBlockAccessor.freeBlock(aBlockPointer);
		}
	}


	protected byte[] readBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer.getBlockType() != BlockType.INDEX && aBlockPointer.getBlockType() != BlockType.LEAF)
		{
			throw new IllegalArgumentException("Attempt to read bad block: " + aBlockPointer);
		}

		return mBlockAccessor.readBlock(aBlockPointer);
	}


	protected BlockPointer writeBlock(byte[] aContent, BlockType aBlockType)
	{
		return mBlockAccessor.writeBlock(aContent, 0, aContent.length, mTransactionGroup.get(), aBlockType, 0);
	}


	private void setupEmptyTable()
	{
		mRoot = new BTreeLeaf(this);
		mRoot.mMap = new ArrayMap(mLeafSize);
	}


	TreeMap<Long, BTreeNode> getLRU()
	{
		return mLRU;
	}


	synchronized long incrementAndGetNodeCounter()
	{
		return ++mNodeCounter;
	}
}
