package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.Entry;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.terifan.raccoon.ByteBufferMap.PutResult;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.security.MurmurHash3;
import org.terifan.raccoon.util.Result;
import org.terifan.raccoon.LeafNode;
import static org.terifan.raccoon.Node.*;
import org.terifan.raccoon.Stats;
import org.terifan.raccoon.io.Streams;
import org.terifan.raccoon.util.Log;


public class HashTable implements AutoCloseable, Iterable<Entry>
{
	private final static String TAG = HashTable.class.getName();

	private IManagedBlockDevice mBlockDevice;
	private BlockAccessor mBlockAccessor;
	private LeafNode mRootMap;
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

//	private HashMap<BlockPointer,LeafNode> mLeafs = new HashMap<>();


	public HashTable(IManagedBlockDevice aBlockDevice, BlockPointer aRootBlockPointer, long aHashSeed, int aNodeSize, int aLeafSize, long aTransactionId) throws IOException
	{
		mBlockDevice = aBlockDevice;
		mNodeSize = aNodeSize;
		mLeafSize = aLeafSize;
		mHashSeed = aHashSeed;
		mPointersPerNode = mNodeSize / BlockPointer.SIZE;
		mBlockAccessor = new BlockAccessor(mBlockDevice, mNodeSize, mLeafSize);

		if (aRootBlockPointer == null)
		{
			Log.i("create hash table");
			Log.inc();

			mCreateInstance = true;
			mRootMap = LeafNode.alloc(mLeafSize);
			mRootBlockPointer = mBlockAccessor.writeBlock(mRootMap, mPointersPerNode, aTransactionId);
		}
		else
		{
			Log.i("open hash table");
			Log.inc();

			mRootBlockPointer = new BlockPointer().decode(aRootBlockPointer.encode(new byte[BlockPointer.SIZE], 0), 0);

			loadRoot();
		}

		Log.dec();
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
			return Streams.fetch(new BlobInputStream(mBlockDevice, value));
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}


	public synchronized boolean contains(byte[] aKey)
	{
		Result<Integer> type = new Result<>();
		byte[] value = getRaw(aKey, type);
		return value != null;
	}


	public synchronized boolean put(byte[] aKey, Object aValue, long aTransactionId)
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
		Log.i("put");
		Log.inc();

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
			value = Blob.writeBlob(mBlockDevice, aValue, aTransactionId);
			type = 1;
		}
		else
		{
			value = (byte[])aValue;

			if (value.length > mLeafSize / 2)
			{
				value = Blob.writeBlob(mBlockDevice, value, aTransactionId);
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
			Log.v("put root value");

			mRootMap.put(type, aKey, value, result);

			if (result.overflow)
			{
				upgradeRootLeafToNode(aTransactionId);
			}
		}

		if (mRootMap == null)
		{
			putValue(aKey, type, value, computeHash(aKey), 0, mRootNode, result, aTransactionId);

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
			Blob.removeBlob(mBlockDevice, result.value);
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

		return new BlobInputStream(mBlockDevice, value);
	}


	private void upgradeRootLeafToNode(long aTransactionId)
	{
		Log.v("upgrade root leaf to node");

		mRootNode = splitLeaf(mRootMap, 0, aTransactionId);

		mBlockAccessor.freeBlock(mRootBlockPointer);

		mRootBlockPointer = mBlockAccessor.writeBlock(mRootNode, mPointersPerNode, aTransactionId);

		mRootMap = null;
	}


	public synchronized boolean remove(byte[] aKey, long aTransactionId)
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
			value = removeValue(computeHash(aKey), 0, aKey, type, mRootNode, aTransactionId);
		}

		if (value != null && type.get() == 1) // if old value was a blob
		{
			Blob.removeBlob(mBlockDevice, value);
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


	public synchronized boolean commit(long aTransactionId)
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		try
		{
			if (mModified)
			{
				int modCount = mModCount; // no increment
				Log.i("commit hash table");
				Log.inc();

				mBlockAccessor.freeBlock(mRootBlockPointer);

				if (mRootMap != null)
				{
					mRootBlockPointer = mBlockAccessor.writeBlock(mRootMap, mPointersPerNode, aTransactionId);
				}
				else
				{
					mRootBlockPointer = mBlockAccessor.writeBlock(mRootNode, mPointersPerNode, aTransactionId);
				}

				Log.dec();
				assert mModCount == modCount : "concurrent modification";

				return true;
			}

			return false;
		}
		finally
		{
			mCreateInstance = false;
		}
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


	public synchronized void clear(long aTransactionId)
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
			visit((aPointerIndex, aBlockPointer) ->
			{
				if (aPointerIndex != Visitor.ROOT_POINTER && aBlockPointer != null && (aBlockPointer.getType() == NODE || aBlockPointer.getType() == LEAF))
				{
					mBlockAccessor.freeBlock(aBlockPointer);
				}
			});

			modCount++;

			mRootNode = null;
			mRootMap = LeafNode.alloc(mLeafSize);
		}

		mBlockAccessor.freeBlock(mRootBlockPointer);

		mRootBlockPointer = mBlockAccessor.writeBlock(mRootMap, mPointersPerNode, aTransactionId);

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

		Result<Integer> result = new Result<>();

		visit((aPointerIndex, aBlockPointer)->
		{
			if (aBlockPointer != null && aBlockPointer.getType() == LEAF)
			{
				result.set(result.get() + readLeaf(aBlockPointer).size());
			}
		});

		return result.get();
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


	private void putValue(byte[] aKey, int aType, byte[] aValue, long aHash, int aLevel, IndexNode aNode, PutResult aResult, long aTransactionId)
	{
		Stats.putValue++;
		Log.v("put value");
		Log.inc();

		int index = aNode.findPointer(computeIndex(aHash, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);

		switch (blockPointer.getType())
		{
			case NODE:
				IndexNode node = readNode(blockPointer);
				putValue(aKey, aType, aValue, aHash, aLevel + 1, node, aResult, aTransactionId);
				mBlockAccessor.freeBlock(blockPointer);
				aNode.setPointer(index, mBlockAccessor.writeBlock(node, blockPointer.getRange(), aTransactionId));
				break;
			case LEAF:
				putValueLeaf(blockPointer, index, aKey, aType, aValue, aLevel, aNode, aHash, aResult, aTransactionId);
				break;
			case HOLE:
				upgradeHoleToLeaf(aKey, aType, aValue, aNode, blockPointer, index, aResult, aTransactionId);
				break;
		}

		Log.dec();
	}


	private void putValueLeaf(BlockPointer aBlockPointer, int aIndex, byte[] aKey, int aType, byte[] aValue, int aLevel, IndexNode aNode, long aHash, PutResult aResult, long aTransactionId)
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

			aNode.setPointer(aIndex, mBlockAccessor.writeBlock(map, aBlockPointer.getRange(), aTransactionId));
		}
		else if (splitLeaf(map, aBlockPointer, aIndex, aLevel, aNode, aTransactionId))
		{
			putValue(aKey, aType, aValue, aHash, aLevel, aNode, aResult, aTransactionId); // note: recursive put
		}
		else
		{
			IndexNode node = splitLeaf(map, aLevel + 1, aTransactionId);

			putValue(aKey, aType, aValue, aHash, aLevel + 1, node, aResult, aTransactionId); // note: recursive put

			mBlockAccessor.freeBlock(aBlockPointer);

			aNode.setPointer(aIndex, mBlockAccessor.writeBlock(node, aBlockPointer.getRange(), aTransactionId));
		}
	}


	private void upgradeHoleToLeaf(byte[] aKey, int aType, byte[] aValue, IndexNode aNode, BlockPointer aBlockPointer, int aIndex, PutResult aResult, long aTransactionId)
	{
		Stats.upgradeHoleToLeaf++;
		Log.v("upgrade hole to leaf");
		Log.inc();

		LeafNode map = LeafNode.alloc(mLeafSize);

		map.put(aType, aKey, aValue, aResult);

		aNode.setPointer(aIndex, mBlockAccessor.writeBlock(map, aBlockPointer.getRange(), aTransactionId));

		Log.dec();
	}


	private IndexNode splitLeaf(LeafNode aMap, int aLevel, long aTransactionId)
	{
		Log.inc();
		Log.v("split leaf");

		Stats.splitLeaf++;

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
		BlockPointer lowIndex = low.isEmpty() ? new BlockPointer().setType(HOLE).setRange(halfRange) : mBlockAccessor.writeBlock(low, halfRange, aTransactionId);
		BlockPointer highIndex = high.isEmpty() ? new BlockPointer().setType(HOLE).setRange(halfRange) : mBlockAccessor.writeBlock(high, halfRange, aTransactionId);

		IndexNode node = IndexNode.wrap(new byte[mNodeSize]);
		node.setPointer(0, lowIndex);
		node.setPointer(halfRange, highIndex);

		Log.dec();
		assert node.integrityCheck() == null : node.integrityCheck();

		return node;
	}


	private boolean splitLeaf(LeafNode aMap, BlockPointer aBlockPointer, int aIndex, int aLevel, IndexNode aNode, long aTransactionId)
	{
		if (aBlockPointer.getRange() == 1)
		{
			return false;
		}

		Stats.splitLeaf++;
		Log.inc();
		Log.v("split leaf");

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
		BlockPointer lowIndex = low.isEmpty() ? new BlockPointer().setType(HOLE).setRange(halfRange) : mBlockAccessor.writeBlock(low, halfRange, aTransactionId);
		BlockPointer highIndex = high.isEmpty() ? new BlockPointer().setType(HOLE).setRange(halfRange) : mBlockAccessor.writeBlock(high, halfRange, aTransactionId);

		aNode.split(aIndex, lowIndex, highIndex);

		Log.dec();
		assert aNode.integrityCheck() == null : aNode.integrityCheck();

		return true;
	}


	private byte[] removeValue(long aHash, int aLevel, byte[] aKey, Result<Integer> aType, IndexNode aNode, long aTransactionId)
	{
		Stats.removeValue++;

		int index = aNode.findPointer(computeIndex(aHash, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);

		byte[] oldValue;

		switch (blockPointer.getType())
		{
			case NODE:
				IndexNode node = readNode(blockPointer);
				oldValue = removeValue(aHash, aLevel + 1, aKey, aType, node, aTransactionId);
				if (oldValue != null)
				{
					mBlockAccessor.freeBlock(blockPointer);
					aNode.setPointer(index, mBlockAccessor.writeBlock(node, blockPointer.getRange(), aTransactionId));
				}
				return oldValue;
			case LEAF:
				LeafNode map = readLeaf(blockPointer);
				oldValue = map.remove(aKey, aType);
				if (oldValue != null)
				{
					mBlockAccessor.freeBlock(blockPointer);
					aNode.setPointer(index, mBlockAccessor.writeBlock(map, blockPointer.getRange(), aTransactionId));
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
}