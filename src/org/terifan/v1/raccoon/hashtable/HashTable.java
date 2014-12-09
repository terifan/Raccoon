package org.terifan.v1.raccoon.hashtable;

import org.terifan.v1.raccoon.Entry;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.terifan.v1.raccoon.LeafNode.PutResult;
import org.terifan.v1.raccoon.io.IBlockDevice;
import org.terifan.v1.raccoon.Database;
import org.terifan.v1.raccoon.DatabaseException;
import org.terifan.v1.raccoon.security.MurmurHash3;
import org.terifan.v1.raccoon.util.Result;
import org.terifan.v1.raccoon.LeafNode;
import org.terifan.v1.raccoon.util.Logger;
import static org.terifan.v1.raccoon.Node.*;
import org.terifan.v1.raccoon.Stats;
import org.terifan.v1.raccoon.io.Streams;


public class HashTable implements Closeable, Iterable<Entry>
{
	private final static String TAG = HashTable.class.getName();

	private Database mDatabase;
	private BlockAccessor mBlockAccessor;
	private LeafNode mRootMap;
	private int mPageSize;
	private int mNodeSize;
	private int mLeafSize;
	private int mPointersPerNode;
	/*private*/ int mModCount;
	private long mHashSeed;
	private BlockPointer mRootBlockPointer;
	private IndexNode mRootNode;
	private boolean mCreateInstance;
	private boolean mClosed;
	private boolean mModified;

	Logger Log;

//	private HashMap<BlockPointer,LeafNode> mLeafs = new HashMap<>();


	public HashTable(Database aDatabasea, BlockPointer aRootBlockPointer, long aHashSeed, String aName)
	{
		Log = new Logger(aName);

		mDatabase = aDatabasea;
		mBlockAccessor = new BlockAccessor(this, mDatabase.getBlockDevice());
		mPageSize = mDatabase.getBlockDevice().getBlockSize();
		mNodeSize = 4 * mPageSize;
		mLeafSize = 8 * mPageSize;
//		mNodeSize = mPageSize;
//		mLeafSize = mPageSize;
		mHashSeed = aHashSeed;
		mPointersPerNode = mNodeSize / BlockPointer.SIZE;

		if (aRootBlockPointer == null)
		{
			Log.inc("create table");

			mCreateInstance = true;
			mRootMap = LeafNode.alloc(mLeafSize);
			mRootBlockPointer = mBlockAccessor.writeBlock(mRootMap, mPointersPerNode);
		}
		else
		{
			Log.inc("open table");

			mRootBlockPointer = new BlockPointer().decode(aRootBlockPointer.encode(new byte[BlockPointer.SIZE], 0), 0);

			loadRoot();
		}

		Log.dec();
	}


	public IBlockDevice getBlockDevice()
	{
		return mBlockAccessor.getBlockDevice();
	}


	public BlockPointer getRootBlockPointer()
	{
		return mRootBlockPointer;
	}


	private void loadRoot()
	{
		Log.i("load root ", mRootBlockPointer);

		if (mRootBlockPointer.getType() == LEAF)
		{
			mRootMap = readLeaf(mRootBlockPointer);
		}
		else
		{
			mRootNode = readNode(mRootBlockPointer);
		}
	}


	public synchronized byte[] getRaw(byte[] aKey, Result<Integer> aType)
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		byte[] value;

		if (mRootMap != null)
		{
			value = mRootMap.get(aKey, aType);
		}
		else
		{
			value = getValue(computeHash(aKey), 0, aKey, aType, mRootNode);
		}

		return value;
	}


	public synchronized byte[] get(byte[] aKey)
	{
		Result<Integer> type = new Result<>();
		byte[] value = getRaw(aKey, type);

		if (value == null)
		{
			return null;
		}
		if (type.get() == 0)
		{
			return value;
		}

		try
		{
			return Streams.fetch(new BlobInputStream(this, value));
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}


	public synchronized boolean put(byte[] aKey, Object aValue)
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		if (aKey.length > getKeyMaximumLength())
		{
			throw new IllegalArgumentException("Key length exceeds maximum length: " + aKey.length + ", max: " + getKeyMaximumLength());
		}

		int modCount = ++mModCount;
		Log.d("put").inc();

		mModified = true;

		byte[] value;
		int type;

//		if (aValue instanceof BlobPushStream)
//		{
//			value = ((BlobPushStream)aValue).getBlobOutputStream().getOutput();
//			type = 1;
//		}
//		else
		if (aValue instanceof InputStream)
		{
			value = Blob.writeBlob(this, aValue);
			type = 1;
		}
		else
		{
			value = (byte[])aValue;

			if (value.length > mLeafSize / 2)
			{
				value = Blob.writeBlob(this, value);
				type = 1;
			}
			else
			{
				type = 0;
			}
		}

		PutResult result = new PutResult();

		if (mRootMap != null)
		{
			mRootMap.put(type, aKey, value, result);

			if (result.overflow)
			{
				upgradeRootLeafToNode();
			}
		}

		if (mRootMap == null)
		{
			putValue(aKey, type, value, computeHash(aKey), 0, mRootNode, result);

//			for (java.util.Map.Entry<BlockPointer,LeafNode> entry : mLeafs.entrySet())
//			{
//				LeafNode leaf = entry.getValue();
//
//				if (leaf.mDirty)
//				{
//					BlockPointer blockPointer = entry.getKey();
//
//					mBlockDevice.freeBlock(blockPointer);
//
//					leaf.mParent.setPointer(leaf.mIndex, mBlockDevice.writeBlock(leaf, blockPointer.getRange()));
//
//					leaf.mDirty = false;
//				}
//			}
//
//			mLeafs.clear();
		}

		if (result.value != null && result.entryType == 1) // if old value was a blob
		{
			Blob.removeBlob(this, result.value);
		}

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return result.inserted;
	}


	public synchronized InputStream read(byte[] aKey)
	{
		Result<Integer> type = new Result<>();
		byte[] value = getRaw(aKey, type);

		if (value == null)
		{
			return null;
		}
		if (type.get() == 0)
		{
			return new ByteArrayInputStream(value);
		}

		return new BlobInputStream(this, value);
	}


	private void upgradeRootLeafToNode()
	{
		mRootNode = splitLeaf(mRootMap, 0);

		mBlockAccessor.freeBlock(mRootBlockPointer);

		mRootBlockPointer = mBlockAccessor.writeBlock(mRootNode, mPointersPerNode);

		mRootMap = null;
	}


	public synchronized boolean remove(byte[] aKey)
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		int modCount = ++mModCount;
		mModified = true;

		byte[] value;
		Result<Integer> type = new Result<>();

		if (mRootMap != null)
		{
			value = mRootMap.remove(aKey, type);
		}
		else
		{
			value = removeValue(computeHash(aKey), 0, aKey, type, mRootNode);
		}

		if (value != null && type.get() == 1) // if old value was a blob
		{
			Blob.removeBlob(this, value);
		}

		assert mModCount == modCount : "concurrent modification";

		return value != null;
	}


	@Override
	public synchronized Iterator<Entry> iterator()
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		if (mRootNode != null)
		{
			return new NodeIterator(this, mRootBlockPointer);
		}
		else if (!mRootMap.isEmpty())
		{
			return new NodeIterator(this, mRootMap);
		}
		else
		{
			return new ArrayList<Entry>().iterator();
		}
	}


	public synchronized List<Entry> list()
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		List<Entry> list = new ArrayList<>();

		for (Iterator<Entry> it = iterator(); it.hasNext(); )
		{
			list.add(it.next());
		}

		return list;
	}


	public synchronized void commit()
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		if (mModified)
		{
			int modCount = mModCount; // no increment
			Log.inc("commit table");

			mBlockAccessor.freeBlock(mRootBlockPointer);

			if (mRootMap != null)
			{
				mRootBlockPointer = mBlockAccessor.writeBlock(mRootMap, mPointersPerNode);
			}
			else
			{
				mRootBlockPointer = mBlockAccessor.writeBlock(mRootNode, mPointersPerNode);
			}

			Log.dec();
			assert mModCount == modCount : "concurrent modification";
		}

		mCreateInstance = false;
	}


	public synchronized void rollback()
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		Log.i("rollback");

		mRootNode = null;
		mRootMap = null;

		if (mCreateInstance)
		{
			mRootMap = LeafNode.alloc(mLeafSize);
		}
		else
		{
			loadRoot();
		}
	}


	public synchronized void clear()
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		Log.i("clear");

		int modCount = ++mModCount;
		mModified = true;

		if (mRootMap != null)
		{
			mRootMap.clear();
		}
		else
		{
			visit(new ClearTableVisitor());

			modCount++;

			mRootNode = null;
			mRootMap = LeafNode.alloc(mLeafSize);
		}

		mBlockAccessor.freeBlock(mRootBlockPointer);

		mRootBlockPointer = mBlockAccessor.writeBlock(mRootMap, mPointersPerNode);

		assert mModCount == modCount : "concurrent modification";
	}


	@Override
	public synchronized void close()
	{
		mClosed = true;

		mBlockAccessor = null;
		mRootMap = null;
		mRootNode = null;
	}


	public synchronized int size()
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		CountEntriesVisitor v = new CountEntriesVisitor();
		visit(v);
		return v.total;
	}


	private byte[] getValue(long aHash, int aLevel, byte[] aKey, Result<Integer> aType, IndexNode aNode)
	{
		Stats.getValue++;
		Log.i("get value");

		BlockPointer blockPointer = aNode.getPointer(aNode.findPointer(computeIndex(aHash, aLevel)));

		switch (blockPointer.getType())
		{
			case NODE:
				return getValue(aHash, aLevel + 1, aKey, aType, readNode(blockPointer));
			case LEAF:
				return readLeaf(blockPointer).get(aKey, aType);
			case HOLE:
			default:
				return null;
		}
	}


	private void putValue(byte[] aKey, int aType, byte[] aValue, long aHash, int aLevel, IndexNode aNode, PutResult aResult)
	{
		Stats.putValue++;
		Log.inc("put value");

		int index = aNode.findPointer(computeIndex(aHash, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);

		switch (blockPointer.getType())
		{
			case NODE:
				IndexNode node = readNode(blockPointer);
				putValue(aKey, aType, aValue, aHash, aLevel + 1, node, aResult);
				mBlockAccessor.freeBlock(blockPointer);
				aNode.setPointer(index, mBlockAccessor.writeBlock(node, blockPointer.getRange()));
				break;
			case LEAF:
				putValueLeaf(blockPointer, index, aKey, aType, aValue, aLevel, aNode, aHash, aResult);
				break;
			case HOLE:
				upgradeHoleToLeaf(aKey, aType, aValue, aNode, blockPointer, index, aResult);
				break;
		}

		Log.dec();
	}


	private void putValueLeaf(BlockPointer aBlockPointer, int aIndex, byte[] aKey, int aType, byte[] aValue, int aLevel, IndexNode aNode, long aHash, PutResult aResult)
	{
		Stats.putValueLeaf++;

		LeafNode map = readLeaf(aBlockPointer);

//		LeafNode map = mLeafs.get(aBlockPointer);
//		if (map == null)
//		{
//			map = readLeaf(aBlockPointer);
//			mLeafs.put(aBlockPointer, map);
//		}
//
//		map.mDirty = true;
//		map.mParent = aNode;

		map.put(aType, aKey, aValue, aResult);

		if (!aResult.overflow)
		{
//			Log.out.println("todo: free " + aBlockPointer);
//			Log.out.println("todo: setPointer " + aBlockPointer);

			mBlockAccessor.freeBlock(aBlockPointer);

			aNode.setPointer(aIndex, mBlockAccessor.writeBlock(map, aBlockPointer.getRange()));
		}
		else if (splitLeaf(map, aBlockPointer, aIndex, aLevel, aNode))
		{
			putValue(aKey, aType, aValue, aHash, aLevel, aNode, aResult); // note: recursive put
		}
		else
		{
			IndexNode node = splitLeaf(map, aLevel + 1);

			putValue(aKey, aType, aValue, aHash, aLevel + 1, node, aResult); // note: recursive put

			mBlockAccessor.freeBlock(aBlockPointer);

			aNode.setPointer(aIndex, mBlockAccessor.writeBlock(node, aBlockPointer.getRange()));
		}
	}


	private void upgradeHoleToLeaf(byte[] aKey, int aType, byte[] aValue, IndexNode aNode, BlockPointer aBlockPointer, int aIndex, PutResult aResult)
	{
		Stats.upgradeHoleToLeaf++;
		Log.inc("upgrade hole to leaf");

		LeafNode map = LeafNode.alloc(mLeafSize);

		map.put(aType, aKey, aValue, aResult);

		aNode.setPointer(aIndex, mBlockAccessor.writeBlock(map, aBlockPointer.getRange()));

		Log.dec();
	}


	private IndexNode splitLeaf(LeafNode aMap, int aLevel)
	{
		Stats.splitLeaf++;
		Log.inc("split leaf");

		LeafNode low = LeafNode.alloc(mLeafSize);
		LeafNode high = LeafNode.alloc(mLeafSize);
		int halfRange = mPointersPerNode / 2;

		for (int i = 0, sz = aMap.size(); i < sz; i++)
		{
			byte[] key = aMap.getKey(i);

			if (computeIndex(computeHash(key), aLevel) < halfRange)
			{
				aMap.copy(i, low);
			}
			else
			{
				aMap.copy(i, high);
			}
		}

		// create nodes pointing to leafs
		BlockPointer lowIndex = low.isEmpty() ? new BlockPointer().setType(HOLE).setRange(halfRange) : mBlockAccessor.writeBlock(low, halfRange);
		BlockPointer highIndex = high.isEmpty() ? new BlockPointer().setType(HOLE).setRange(halfRange) : mBlockAccessor.writeBlock(high, halfRange);

		IndexNode node = IndexNode.wrap(new byte[mNodeSize]);
		node.setPointer(0, lowIndex);
		node.setPointer(halfRange, highIndex);

		Log.dec();
		assert node.integrityCheck() == null : node.integrityCheck();

		return node;
	}


	private boolean splitLeaf(LeafNode aMap, BlockPointer aBlockPointer, int aIndex, int aLevel, IndexNode aNode)
	{
		if (aBlockPointer.getRange() == 1)
		{
			return false;
		}

		Stats.splitLeaf++;
		Log.inc("split leaf");

		LeafNode low = LeafNode.alloc(mLeafSize);
		LeafNode high = LeafNode.alloc(mLeafSize);
		int halfRange = aBlockPointer.getRange() / 2;

		int mid = aIndex + halfRange;

		for (int i = 0, sz = aMap.size(); i < sz; i++)
		{
			byte[] key = aMap.getKey(i);

			if (computeIndex(computeHash(key), aLevel) < mid)
			{
				aMap.copy(i, low);
			}
			else
			{
				aMap.copy(i, high);
			}
		}

		mBlockAccessor.freeBlock(aBlockPointer);

		// create nodes pointing to leafs
		BlockPointer lowIndex = low.isEmpty() ? new BlockPointer().setType(HOLE).setRange(halfRange) : mBlockAccessor.writeBlock(low, halfRange);
		BlockPointer highIndex = high.isEmpty() ? new BlockPointer().setType(HOLE).setRange(halfRange) : mBlockAccessor.writeBlock(high, halfRange);

		aNode.split(aIndex, lowIndex, highIndex);

		Log.dec();
		assert aNode.integrityCheck() == null : aNode.integrityCheck();

		return true;
	}


	private byte[] removeValue(long aHash, int aLevel, byte[] aKey, Result<Integer> aType, IndexNode aNode)
	{
		Stats.removeValue++;

		int index = aNode.findPointer(computeIndex(aHash, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);

		byte[] oldValue;

		switch (blockPointer.getType())
		{
			case NODE:
				IndexNode node = readNode(blockPointer);
				oldValue = removeValue(aHash, aLevel + 1, aKey, aType, node);
				if (oldValue != null)
				{
					mBlockAccessor.freeBlock(blockPointer);
					aNode.setPointer(index, mBlockAccessor.writeBlock(node, blockPointer.getRange()));
				}
				return oldValue;
			case LEAF:
				LeafNode map = readLeaf(blockPointer);
				oldValue = map.remove(aKey, aType);
				if (oldValue != null)
				{
					mBlockAccessor.freeBlock(blockPointer);
					aNode.setPointer(index, mBlockAccessor.writeBlock(map, blockPointer.getRange()));
				}
				return oldValue;
			case HOLE:
			default:
				return null;
		}
	}


	LeafNode readLeaf(BlockPointer aBlockPointer)
	{
		assert aBlockPointer.getType() == LEAF;

		if (aBlockPointer.getPageIndex() == mRootBlockPointer.getPageIndex() && mRootMap != null)
		{
			return mRootMap;
		}

		return LeafNode.wrap(mBlockAccessor.readBlock(aBlockPointer));
	}


	IndexNode readNode(BlockPointer aBlockPointer)
	{
		assert aBlockPointer.getType() == NODE;

		if (aBlockPointer.getPageIndex() == mRootBlockPointer.getPageIndex() && mRootNode != null)
		{
			return mRootNode;
		}

		return IndexNode.wrap(mBlockAccessor.readBlock(aBlockPointer));
	}


	private long computeHash(byte[] aData)
	{
		return MurmurHash3.hash_x64_64(aData, mHashSeed);
	}


	private int computeIndex(long aHash, int aLevel)
	{
		return (int)Long.rotateRight(aHash, 17 * aLevel) & (mPointersPerNode - 1);
	}


	public String integrityCheck()
	{
		Log.i("integrity check");

		if (mRootMap != null)
		{
			return mRootMap.integrityCheck();
		}

		return mRootNode.integrityCheck();
	}


	public int getKeyMaximumLength()
	{
		return mLeafSize - LeafNode.OVERHEAD;
	}


	private interface Visitor
	{
		int ROOT_POINTER = -1;

		void visit(int aPointerIndex, BlockPointer aBlockPointer);
	}


	private void visit(Visitor aVisitor)
	{
		if (mRootNode != null)
		{
			visitNode(aVisitor, mRootBlockPointer);
		}

		aVisitor.visit(Visitor.ROOT_POINTER, mRootBlockPointer);
	}


	private void visitNode(Visitor aVisitor, BlockPointer aBlockPointer)
	{
		IndexNode node = readNode(aBlockPointer);

		for (int i = 0; i < mPointersPerNode; i++)
		{
			BlockPointer next = node.getPointer(i);

			if (next != null && next.getType() == NODE)
			{
				visitNode(aVisitor, next);
			}

			aVisitor.visit(i, next);
		}
	}


	private class ClearTableVisitor implements Visitor
	{
		@Override
		public void visit(int aPointerIndex, BlockPointer aBlockPointer)
		{
			if (aPointerIndex == Visitor.ROOT_POINTER)
			{
				return;
			}

			if (aBlockPointer != null && (aBlockPointer.getType() == NODE || aBlockPointer.getType() == LEAF))
			{
				mBlockAccessor.freeBlock(aBlockPointer);
			}
		}
	}


	private class CountEntriesVisitor implements Visitor
	{
		private long modCount = mModCount;
		public int total;


		@Override
		public void visit(int aPointerIndex, BlockPointer aBlockPointer)
		{
			assert mModCount == modCount;

			if (aBlockPointer != null && aBlockPointer.getType() == LEAF)
			{
				total += readLeaf(aBlockPointer).size();
			}
		}
	}


	public int getLeafSize()
	{
		return mLeafSize;
	}


	public int getNodeSize()
	{
		return mNodeSize;
	}


	long getTransactionId()
	{
		return mDatabase.getTransactionId();
	}
}
