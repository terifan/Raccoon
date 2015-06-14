package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.io.BlockPointer;
import org.terifan.raccoon.io.BlockAccessor;
import org.terifan.raccoon.Entry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.terifan.raccoon.ByteBufferMap;
import org.terifan.raccoon.security.MurmurHash3;
import org.terifan.raccoon.util.Result;
import org.terifan.raccoon.LeafNode;
import org.terifan.raccoon.Node;
import org.terifan.raccoon.Stats;
import org.terifan.raccoon.util.Log;
import static org.terifan.raccoon.Node.*;


public class HashTable implements AutoCloseable, Iterable<Entry>
{
	private final static String TAG = HashTable.class.getName();

	private BlockAccessor mBlockAccessor;
	private BlockPointer mRootBlockPointer;
	private LeafNode mRootMap;
	private IndexNode mRootNode;
	private int mNodeSize;
	private int mLeafSize;
	private int mPointersPerNode;
	/*private*/ int mModCount;
	private long mHashSeed;
	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private boolean mModified;
	private boolean mStandAlone;


	public HashTable(BlockAccessor aBlockAccessor, BlockPointer aRootBlockPointer, long aHashSeed, int aNodeSize, int aLeafSize, long aTransactionId, boolean aStandAlone) throws IOException
	{
		assert aNodeSize % BlockPointer.SIZE == 0;
		assert aNodeSize % aBlockAccessor.getBlockDevice().getBlockSize() == 0;
		assert aLeafSize % aBlockAccessor.getBlockDevice().getBlockSize() == 0;

		mBlockAccessor = aBlockAccessor;
		mNodeSize = aNodeSize;
		mLeafSize = aLeafSize;
		mHashSeed = aHashSeed;
		mPointersPerNode = mNodeSize / BlockPointer.SIZE;
		mStandAlone = aStandAlone;

		if (aRootBlockPointer == null)
		{
			Log.i("create hash table");
			Log.inc();

			mWasEmptyInstance = true;
			mRootMap = new LeafNode(mLeafSize);
			mRootBlockPointer = writeBlock(mRootMap, mPointersPerNode, aTransactionId);
		}
		else
		{
			Log.i("open hash table");
			Log.inc();

			mRootBlockPointer = new BlockPointer().unmarshal(aRootBlockPointer.marshal(new byte[BlockPointer.SIZE], 0), 0);

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


	public byte[] get(byte[] aKey)
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		if (mRootMap != null)
		{
			return mRootMap.get(aKey);
		}
		else
		{
			return getValue(computeHash(aKey), 0, aKey, mRootNode);
		}
	}


	public synchronized boolean contains(byte[] aKey)
	{
		return get(aKey) != null;
	}


	public synchronized byte[] put(byte[] aKey, byte[] aValue, long aTransactionId)
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		if (aKey.length + aValue.length > getEntryMaximumLength())
		{
			throw new IllegalArgumentException("Entry length exceeds maximum length: key: " + aKey.length + ", value: " + aValue.length + ", limit: " + getEntryMaximumLength());
		}

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		mModified = true;
		byte[] oldValue = null;

		if (mRootMap != null)
		{
			Log.v("put root value");

			oldValue = mRootMap.put(aKey, aValue);

			if (oldValue == ByteBufferMap.OVERFLOW)
			{
				Log.v("upgrade root leaf to node");

				mRootNode = splitLeaf(mRootMap, 0, aTransactionId);
				freeBlock(mRootBlockPointer);
				mRootBlockPointer = writeBlock(mRootNode, mPointersPerNode, aTransactionId);
				mRootMap = null;
			}
		}

		if (mRootMap == null)
		{
			oldValue = putValue(aKey, aValue, computeHash(aKey), 0, mRootNode, aTransactionId);
		}

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return oldValue;
	}


	public synchronized byte[] remove(byte[] aKey, long aTransactionId)
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		int modCount = ++mModCount;
		mModified = true;

		byte[] oldValue;

		if (mRootMap != null)
		{
			oldValue = mRootMap.remove(aKey);
		}
		else
		{
			oldValue = removeValue(computeHash(aKey), 0, aKey, mRootNode, aTransactionId);
		}
		
		assert mModCount == modCount : "concurrent modification";

		return oldValue;
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


	public synchronized ArrayList<Entry> list()
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		ArrayList<Entry> list = new ArrayList<>();

		for (Iterator<Entry> it = iterator(); it.hasNext(); )
		{
			list.add(it.next());
		}

		return list;
	}


	public synchronized boolean commit(long aTransactionId) throws IOException
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

				freeBlock(mRootBlockPointer);

				if (mRootMap != null)
				{
					mRootBlockPointer = writeBlock(mRootMap, mPointersPerNode, aTransactionId);
				}
				else
				{
					mRootBlockPointer = writeBlock(mRootNode, mPointersPerNode, aTransactionId);
				}

				if (mStandAlone)
				{
					mBlockAccessor.getBlockDevice().commit();
				}

				Log.dec();
				assert mModCount == modCount : "concurrent modification";

				return true;
			}
			else if (mWasEmptyInstance && mStandAlone)
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


	public synchronized void rollback() throws IOException
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}

		Log.i("rollback");

		if (mStandAlone)
		{
			mBlockAccessor.getBlockDevice().rollback();
		}

		mRootNode = null;
		mRootMap = null;

		if (mWasEmptyInstance)
		{
			Log.v("rollback empty");

			// occurs when the hashtable is created and never been commited thus rollback is to an empty hashtable
			mRootMap = new LeafNode(mLeafSize);
		}
		else
		{
			Log.v("rollback " + (mRootBlockPointer.getType() == LEAF ? "root map" : "root node"));

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
					freeBlock(aBlockPointer);
				}
			});

			mRootNode = null;
			mRootMap = new LeafNode(mLeafSize);
		}

		freeBlock(mRootBlockPointer);

		mRootBlockPointer = writeBlock(mRootMap, mPointersPerNode, aTransactionId);

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

		Result<Integer> result = new Result<>(0);

		visit((aPointerIndex, aBlockPointer)->
		{
			if (aBlockPointer != null && aBlockPointer.getType() == LEAF)
			{
				result.set(result.get() + readLeaf(aBlockPointer).size());
			}
		});

		return result.get();
	}


	private byte[] getValue(long aHash, int aLevel, byte[] aKey, IndexNode aNode)
	{
		Stats.getValue++;
		Log.i("get value");

		BlockPointer blockPointer = aNode.getPointer(aNode.findPointer(computeIndex(aHash, aLevel)));

		switch (blockPointer.getType())
		{
			case NODE:
				return getValue(aHash, aLevel + 1, aKey, readNode(blockPointer));
			case LEAF:
				return readLeaf(blockPointer).get(aKey);
			case HOLE:
				return null;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	private byte[] putValue(byte[] aKey, byte[] aValue, long aHash, int aLevel, IndexNode aNode, long aTransactionId)
	{
		Stats.putValue++;
		Log.v("put value");
		Log.inc();

		int index = aNode.findPointer(computeIndex(aHash, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);
		byte[] oldValue;

		switch (blockPointer.getType())
		{
			case NODE:
				IndexNode node = readNode(blockPointer);
				oldValue = putValue(aKey, aValue, aHash, aLevel + 1, node, aTransactionId);
				freeBlock(blockPointer);
				aNode.setPointer(index, writeBlock(node, blockPointer.getRange(), aTransactionId));
				break;
			case LEAF:
				oldValue = putValueLeaf(blockPointer, index, aKey, aValue, aLevel, aNode, aHash, aTransactionId);
				break;
			case HOLE:
				oldValue = upgradeHoleToLeaf(aKey, aValue, aNode, blockPointer, index, aTransactionId);
				break;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}

		Log.dec();

		return oldValue;
	}


	private byte[] putValueLeaf(BlockPointer aBlockPointer, int aIndex, byte[] aKey, byte[] aValue, int aLevel, IndexNode aNode, long aHash, long aTransactionId)
	{
		Stats.putValueLeaf++;

		LeafNode map = readLeaf(aBlockPointer);

		byte[] oldValue = map.put(aKey, aValue);

		if (oldValue != ByteBufferMap.OVERFLOW)
		{
			freeBlock(aBlockPointer);

			aNode.setPointer(aIndex, writeBlock(map, aBlockPointer.getRange(), aTransactionId));
		}
		else if (splitLeaf(map, aBlockPointer, aIndex, aLevel, aNode, aTransactionId))
		{
			oldValue = putValue(aKey, aValue, aHash, aLevel, aNode, aTransactionId); // recursive put
		}
		else
		{
			IndexNode node = splitLeaf(map, aLevel + 1, aTransactionId);

			oldValue = putValue(aKey, aValue, aHash, aLevel + 1, node, aTransactionId); // recursive put

			freeBlock(aBlockPointer);

			aNode.setPointer(aIndex, writeBlock(node, aBlockPointer.getRange(), aTransactionId));
		}

		return oldValue;
	}


	private byte[] upgradeHoleToLeaf(byte[] aKey, byte[] aValue, IndexNode aNode, BlockPointer aBlockPointer, int aIndex, long aTransactionId)
	{
		Stats.upgradeHoleToLeaf++;
		Log.v("upgrade hole to leaf");
		Log.inc();

		LeafNode map = new LeafNode(mLeafSize);

		byte[] oldValue = map.put(aKey, aValue);

		BlockPointer blockPointer = writeBlock(map, aBlockPointer.getRange(), aTransactionId);

		aNode.setPointer(aIndex, blockPointer);

		Log.dec();

		return oldValue;
	}


	private IndexNode splitLeaf(LeafNode aMap, int aLevel, long aTransactionId)
	{
		Log.inc();
		Log.v("split leaf");

		Stats.splitLeaf++;

		LeafNode low = new LeafNode(mLeafSize);
		LeafNode high = new LeafNode(mLeafSize);
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
		BlockPointer lowIndex = low.isEmpty() ? new BlockPointer().setType(HOLE).setRange(halfRange) : writeBlock(low, halfRange, aTransactionId);
		BlockPointer highIndex = high.isEmpty() ? new BlockPointer().setType(HOLE).setRange(halfRange) : writeBlock(high, halfRange, aTransactionId);

		IndexNode node = new IndexNode(new byte[mNodeSize]);
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

		assert aBlockPointer.getRange() >= 2;

		Stats.splitLeaf++;
		Log.inc();
		Log.v("split leaf");

		LeafNode low = new LeafNode(mLeafSize);
		LeafNode high = new LeafNode(mLeafSize);
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

		freeBlock(aBlockPointer);

		// create nodes pointing to leafs
		BlockPointer lowIndex = low.isEmpty() ? new BlockPointer().setType(HOLE).setRange(halfRange) : writeBlock(low, halfRange, aTransactionId);
		BlockPointer highIndex = high.isEmpty() ? new BlockPointer().setType(HOLE).setRange(halfRange) : writeBlock(high, halfRange, aTransactionId);

		aNode.split(aIndex, lowIndex, highIndex);

		Log.dec();
		assert aNode.integrityCheck() == null : aNode.integrityCheck();

		return true;
	}


	private byte[] removeValue(long aHash, int aLevel, byte[] aKey, IndexNode aNode, long aTransactionId)
	{
		Stats.removeValue++;

		int index = aNode.findPointer(computeIndex(aHash, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);

		byte[] oldValue;

		switch (blockPointer.getType())
		{
			case NODE:
				IndexNode node = readNode(blockPointer);
				oldValue = removeValue(aHash, aLevel + 1, aKey, node, aTransactionId);
				if (oldValue != null)
				{
					freeBlock(blockPointer);
					BlockPointer newBlockPointer = writeBlock(node, blockPointer.getRange(), aTransactionId);
					aNode.setPointer(index, newBlockPointer);
				}
				return oldValue;
			case LEAF:
				LeafNode map = readLeaf(blockPointer);
				oldValue = map.remove(aKey);
				if (oldValue != null)
				{
					freeBlock(blockPointer);
					BlockPointer newBlockPointer = writeBlock(map, blockPointer.getRange(), aTransactionId);
					aNode.setPointer(index, newBlockPointer);
				}
				return oldValue;
			case HOLE:
				return null;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	LeafNode readLeaf(BlockPointer aBlockPointer)
	{
		assert aBlockPointer.getType() == LEAF;

		if (aBlockPointer.getOffset() == mRootBlockPointer.getOffset() && mRootMap != null)
		{
			return mRootMap;
		}

		return new LeafNode(readBlock(aBlockPointer));
	}


	IndexNode readNode(BlockPointer aBlockPointer)
	{
		assert aBlockPointer.getType() == NODE;

		if (aBlockPointer.getOffset() == mRootBlockPointer.getOffset() && mRootNode != null)
		{
			return mRootNode;
		}

		return new IndexNode(readBlock(aBlockPointer));
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


	public int getEntryMaximumLength()
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


	private void freeBlock(BlockPointer aBlockPointer)
	{
		mBlockAccessor.freeBlock(aBlockPointer);
	}


	private byte[] readBlock(BlockPointer aBlockPointer)
	{
		return mBlockAccessor.readBlock(aBlockPointer);
	}


	private BlockPointer writeBlock(Node aNode, int aRange, long aTransactionId)
	{
		BlockPointer bp = mBlockAccessor.writeBlock(aNode.array(), 0, aNode.array().length);
		bp.setTransactionId(aTransactionId);
		bp.setType(aNode.getType());
		bp.setRange(aRange);
		return bp;
	}
}
