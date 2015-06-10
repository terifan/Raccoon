package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.io.Blob;
import org.terifan.raccoon.io.BlockPointer;
import org.terifan.raccoon.io.BlockAccessor;
import org.terifan.raccoon.io.BlobInputStream;
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
import org.terifan.raccoon.Node;
import static org.terifan.raccoon.Node.*;
import org.terifan.raccoon.Stats;
import org.terifan.raccoon.io.BlobOutputStream;
import org.terifan.raccoon.io.Streams;
import org.terifan.raccoon.util.Log;


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
	private boolean mCreateInstance;
	private boolean mClosed;
	private boolean mModified;

//	private HashMap<BlockPointer,LeafNode> mLeafs = new HashMap<>();


	public HashTable(BlockAccessor aBlockAccessor, BlockPointer aRootBlockPointer, long aHashSeed, int aNodeSize, int aLeafSize, long aTransactionId) throws IOException
	{
		assert aNodeSize % BlockPointer.SIZE == 0;
		assert aNodeSize % aBlockAccessor.getBlockDevice().getBlockSize() == 0;
		assert aLeafSize % aBlockAccessor.getBlockDevice().getBlockSize() == 0;

		mBlockAccessor = aBlockAccessor;
		mNodeSize = aNodeSize;
		mLeafSize = aLeafSize;
		mHashSeed = aHashSeed;
		mPointersPerNode = mNodeSize / BlockPointer.SIZE;

		if (aRootBlockPointer == null)
		{
			Log.i("create hash table");
			Log.inc();

			mCreateInstance = true;
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


	BlockAccessor getBlockAccessor()
	{
		return mBlockAccessor;
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

//		Log.hexDump(aKey);
//		Log.hexDump(value);

		return value;
	}


	public synchronized byte[] get(byte[] aKey)
	{
		Result<Integer> entryType = new Result<>();
		byte[] value = getRaw(aKey, entryType);

		if (value == null)
		{
			return null;
		}
		if (entryType.get() == 0)
		{
			return value;
		}

		try
		{
			return Streams.fetch(new BlobInputStream(mBlockAccessor, value));
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
			try (BlobOutputStream bos = new BlobOutputStream(mBlockAccessor, aTransactionId))
			{
				bos.write(Streams.fetch((InputStream)aValue));
				value = bos.finish();
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}

//			value = Blob.writeBlob(mBlockDevice, (InputStream)aValue, aTransactionId);
			type = 1;
		}
		else
		{
			value = (byte[])aValue;

			if (value.length > mLeafSize / 2)
			{
				try (BlobOutputStream bos = new BlobOutputStream(mBlockAccessor, aTransactionId))
				{
					bos.write(value);
					value = bos.finish();
				}
				catch (IOException e)
				{
					throw new DatabaseException(e);
				}

//				value = Blob.writeBlob(mBlockDevice, new ByteArrayInputStream(value), aTransactionId);
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

//			Log.hexDump(aKey);
//			Log.hexDump(value);

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
//					leaf.mParent.setPointer(leaf.mIndex, writeBlock(leaf, blockPointer.getRange()));
//
//					leaf.mDirty = false;
//				}
//			}
//
//			mLeafs.clear();
		}

		if (result.value != null && result.entryType == 1) // if old value was a blob
		{
			try
			{
				Blob.deleteBlob(mBlockAccessor, result.value);
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
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

		try
		{
			return new BlobInputStream(mBlockAccessor, value);
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}


	private void upgradeRootLeafToNode(long aTransactionId)
	{
		Log.v("upgrade root leaf to node");

		mRootNode = splitLeaf(mRootMap, 0, aTransactionId);

		freeBlock(mRootBlockPointer);

		mRootBlockPointer = writeBlock(mRootNode, mPointersPerNode, aTransactionId);

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
			try
			{
				Blob.deleteBlob(mBlockAccessor, value);
			}
			catch (IOException e)
			{
				throw new DatabaseException(e);
			}
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

				freeBlock(mRootBlockPointer);

				if (mRootMap != null)
				{
					mRootBlockPointer = writeBlock(mRootMap, mPointersPerNode, aTransactionId);
				}
				else
				{
					mRootBlockPointer = writeBlock(mRootNode, mPointersPerNode, aTransactionId);
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
			mRootMap = new LeafNode(mLeafSize);
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
					freeBlock(aBlockPointer);
				}
			});

			modCount++;

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
				freeBlock(blockPointer);
				aNode.setPointer(index, writeBlock(node, blockPointer.getRange(), aTransactionId));
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

			freeBlock(aBlockPointer);

			aNode.setPointer(aIndex, writeBlock(map, aBlockPointer.getRange(), aTransactionId));
		}
		else if (splitLeaf(map, aBlockPointer, aIndex, aLevel, aNode, aTransactionId))
		{
			putValue(aKey, aType, aValue, aHash, aLevel, aNode, aResult, aTransactionId); // note: recursive put
		}
		else
		{
			IndexNode node = splitLeaf(map, aLevel + 1, aTransactionId);

			putValue(aKey, aType, aValue, aHash, aLevel + 1, node, aResult, aTransactionId); // note: recursive put

			freeBlock(aBlockPointer);

			aNode.setPointer(aIndex, writeBlock(node, aBlockPointer.getRange(), aTransactionId));
		}
	}


	private void upgradeHoleToLeaf(byte[] aKey, int aType, byte[] aValue, IndexNode aNode, BlockPointer aBlockPointer, int aIndex, PutResult aResult, long aTransactionId)
	{
		Stats.upgradeHoleToLeaf++;
		Log.v("upgrade hole to leaf");
		Log.inc();

		LeafNode map = new LeafNode(mLeafSize);

		map.put(aType, aKey, aValue, aResult);

		aNode.setPointer(aIndex, writeBlock(map, aBlockPointer.getRange(), aTransactionId));

		Log.dec();
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
					freeBlock(blockPointer);
					aNode.setPointer(index, writeBlock(node, blockPointer.getRange(), aTransactionId));
				}
				return oldValue;
			case LEAF:
				LeafNode map = readLeaf(blockPointer);
				oldValue = map.remove(aKey, aType);
				if (oldValue != null)
				{
					freeBlock(blockPointer);
					aNode.setPointer(index, writeBlock(map, blockPointer.getRange(), aTransactionId));
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
