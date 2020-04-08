package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;
import org.terifan.security.messagedigest.MurmurHash3;


final class HashTable implements AutoCloseable, Iterable<ArrayMapEntry>
{
	final Cost mCost;
	private final String mTableName;
	private final TransactionGroup mTransactionId;
	/*private*/ BlockAccessor mBlockAccessor;
	private HashTableRoot mRoot;
	private int mNodeSize;
	private int mLeafSize;
	private int mPointersPerNode;
	private int mHashSeed;
	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private boolean mChanged;
	private boolean mCommitChangesToBlockDevice;
	/*private*/ final PerformanceTool mPerformanceTool;
	/*private*/ int mModCount;


	/**
	 * Open an existing HashTable or create a new HashTable with default settings.
	 */
	public HashTable(IManagedBlockDevice aBlockDevice, byte[] aTableHeader, TransactionGroup aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName, Cost aCost, PerformanceTool aPerformanceTool) throws IOException
	{
		mPerformanceTool = aPerformanceTool;
		mTableName = aTableName;
		mTransactionId = aTransactionId;
		mCost = aCost;
		mCommitChangesToBlockDevice = aCommitChangesToBlockDevice;
		mBlockAccessor = new BlockAccessor(aBlockDevice, aCompressionParam, aTableParam.getBlockReadCacheSize());

		if (aTableHeader == null)
		{
			Log.i("create table %s", mTableName);
			Log.inc();

			mNodeSize = aTableParam.getPagesPerNode() * aBlockDevice.getBlockSize();
			mLeafSize = aTableParam.getPagesPerLeaf() * aBlockDevice.getBlockSize();
			mHashSeed = new SecureRandom().nextInt();

			mPointersPerNode = mNodeSize / BlockPointer.SIZE;
			mRoot = new HashTableRoot(this, mLeafSize, mPointersPerNode);
			mWasEmptyInstance = true;
			mChanged = true;
		}
		else
		{
			Log.i("open table %s", mTableName);
			Log.inc();

			mRoot = new HashTableRoot(this);

			unmarshalHeader(aTableHeader);

			mRoot.loadRoot();
		}

		Log.dec();
	}


	public byte[] marshalHeader()
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(BlockPointer.SIZE + 4 + 4 + 4 + 3);
		mRoot.marshal(buffer);
		buffer.writeInt32(mHashSeed);
		buffer.writeVar32(mNodeSize);
		buffer.writeVar32(mLeafSize);
		mBlockAccessor.getCompressionParam().marshal(buffer);

		return buffer.trim().array();
	}


	private void unmarshalHeader(byte[] aTableHeader)
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.wrap(aTableHeader);

		mRoot.unmarshal(buffer);

		mHashSeed = buffer.readInt32();
		mNodeSize = buffer.readVar32();
		mLeafSize = buffer.readVar32();
		mPointersPerNode = mNodeSize / BlockPointer.SIZE;

		mRoot.init(mLeafSize, mPointersPerNode);

		CompressionParam compressionParam = new CompressionParam();
		compressionParam.unmarshal(buffer);

		mBlockAccessor.setCompressionParam(compressionParam);
	}


	public boolean get(ArrayMapEntry aEntry)
	{
		return mRoot.get(aEntry);
	}


	public boolean put(ArrayMapEntry aEntry)
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

		mRoot.put(aEntry);

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return aEntry.getValue() != null;
	}


	public boolean remove(ArrayMapEntry aEntry)
	{
		checkOpen();

		boolean modified = mRoot.remove(aEntry);

		mChanged |= modified;

		return modified;
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		checkOpen();

		return mRoot.iterator();
	}


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


	public boolean isChanged()
	{
		return mChanged;
	}


	public boolean commit() throws IOException
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

				mRoot.writeBlock();

				if (mCommitChangesToBlockDevice)
				{
					mBlockAccessor.getBlockDevice().commit();
				}

				mChanged = false;

				Log.i("table commit finished; root block is %s", mRoot.getRootBlockPointer());

				Log.dec();
				assert mModCount == modCount : "concurrent modification";

				return true;
			}

			if (mWasEmptyInstance && mCommitChangesToBlockDevice)
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


	public void rollback() throws IOException
	{
		checkOpen();

		Log.i("rollback");

		if (mCommitChangesToBlockDevice)
		{
			mBlockAccessor.getBlockDevice().rollback();
		}

		mRoot.rollback(mWasEmptyInstance);

		mChanged = false;
	}


	public void clear()
	{
		checkOpen();

		Log.i("clear");

		int modCount = ++mModCount;
		mChanged = true;

		mRoot.clear();

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
		mRoot.close();
	}


	public int size()
	{
		checkOpen();

		Result<Integer> result = new Result<>(0);

		visit((aPointerIndex, aBlockPointer) ->
		{
			mCost.mTreeTraversal++;

			if (aBlockPointer != null && aBlockPointer.getBlockType() == BlockType.LEAF)
			{
				result.set(result.get() + readLeaf(aBlockPointer).size());
			}
		});

		return result.get();
	}


	boolean getValue(byte[] aKey, int aLevel, ArrayMapEntry aEntry, HashTableNode aNode)
	{
		assert mPerformanceTool.tick("getValue");

		Log.i("get %s value", mTableName);

		mCost.mTreeTraversal++;

		BlockPointer blockPointer = aNode.getPointer(aNode.findPointer(computeIndex(aKey, aLevel)));

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				return getValue(aKey, aLevel + 1, aEntry, readNode(blockPointer));
			case LEAF:
				mCost.mValueGet++;
				HashTableLeaf leaf = readLeaf(blockPointer);
				boolean result = leaf.get(aEntry);
				leaf.gc();
				return result;
			case HOLE:
				return false;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	byte[] putValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel, HashTableNode aNode)
	{
		assert mPerformanceTool.tick("putValue");

		Log.d("put %s value", mTableName);
		Log.inc();

		mCost.mTreeTraversal++;

		int index = aNode.findPointer(computeIndex(aKey, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);
		byte[] oldValue;

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				HashTableNode node = readNode(blockPointer);
				oldValue = putValue(aEntry, aKey, aLevel + 1, node);
				freeBlock(blockPointer);
				aNode.setPointer(index, writeBlock(node, blockPointer.getRange()));
				node.gc();
				break;
			case LEAF:
				oldValue = putValueLeaf(blockPointer, index, aEntry, aLevel, aNode, aKey);
				break;
			case HOLE:
				oldValue = upgradeHoleToLeaf(aEntry, aNode, blockPointer, index);
				break;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}

		Log.dec();

		return oldValue;
	}


	private byte[] putValueLeaf(BlockPointer aBlockPointer, int aIndex, ArrayMapEntry aEntry, int aLevel, HashTableNode aNode, byte[] aKey)
	{
		assert mPerformanceTool.tick("putValueLeaf");

		mCost.mTreeTraversal++;

		HashTableLeaf map = readLeaf(aBlockPointer);

		byte[] oldValue;

		if (map.put(aEntry))
		{
			oldValue = aEntry.getValue();

			freeBlock(aBlockPointer);

			aNode.setPointer(aIndex, writeBlock(map, aBlockPointer.getRange()));

			mCost.mValuePut++;
		}
		else if (splitLeaf(map, aBlockPointer, aIndex, aLevel, aNode))
		{
			oldValue = putValue(aEntry, aKey, aLevel, aNode); // recursive put
		}
		else
		{
			HashTableNode node = splitLeaf(aBlockPointer, map, aLevel + 1);

			oldValue = putValue(aEntry, aKey, aLevel + 1, node); // recursive put

			aNode.setPointer(aIndex, writeBlock(node, aBlockPointer.getRange()));

			node.gc();
		}

		return oldValue;
	}


	private byte[] upgradeHoleToLeaf(ArrayMapEntry aEntry, HashTableNode aNode, BlockPointer aBlockPointer, int aIndex)
	{
		assert mPerformanceTool.tick("upgradeHoleToLeaf");

		Log.d("upgrade hole to leaf");
		Log.inc();

		mCost.mTreeTraversal++;

		HashTableLeaf node = new HashTableLeaf(mLeafSize);

		if (!node.put(aEntry))
		{
			throw new DatabaseException("Failed to upgrade hole to leaf");
		}

		byte[] oldValue = aEntry.getValue();

		BlockPointer blockPointer = writeBlock(node, aBlockPointer.getRange());
		aNode.setPointer(aIndex, blockPointer);

		node.gc();

		Log.dec();

		return oldValue;
	}


	HashTableNode splitLeaf(BlockPointer aBlockPointer, HashTableLeaf aLeafNode, int aLevel)
	{
		assert mPerformanceTool.tick("splitLeaf");

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		mCost.mTreeTraversal++;
		mCost.mBlockSplit++;

		freeBlock(aBlockPointer);

		HashTableLeaf lowLeaf = new HashTableLeaf(mLeafSize);
		HashTableLeaf highLeaf = new HashTableLeaf(mLeafSize);
		int halfRange = mPointersPerNode / 2;

		divideLeafEntries(aLeafNode, aLevel, halfRange, lowLeaf, highLeaf);

		// create nodes pointing to leafs
		BlockPointer lowIndex = writeIfNotEmpty(lowLeaf, halfRange);
		BlockPointer highIndex = writeIfNotEmpty(highLeaf, halfRange);

		HashTableNode node = new HashTableNode(new byte[mNodeSize]);
		node.setPointer(0, lowIndex);
		node.setPointer(halfRange, highIndex);

		lowLeaf.gc();
		highLeaf.gc();

		Log.dec();
		Log.dec();

		return node;
	}


	private boolean splitLeaf(HashTableLeaf aMap, BlockPointer aBlockPointer, int aIndex, int aLevel, HashTableNode aNode)
	{
		assert mPerformanceTool.tick("splitLeaf");

		if (aBlockPointer.getRange() == 1)
		{
			return false;
		}

		assert aBlockPointer.getRange() >= 2;

		mCost.mTreeTraversal++;
		mCost.mBlockSplit++;

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		freeBlock(aBlockPointer);

		HashTableLeaf lowLeaf = new HashTableLeaf(mLeafSize);
		HashTableLeaf highLeaf = new HashTableLeaf(mLeafSize);
		int halfRange = aBlockPointer.getRange() / 2;

		divideLeafEntries(aMap, aLevel, aIndex + halfRange, lowLeaf, highLeaf);

		// create nodes pointing to leafs
		BlockPointer lowIndex = writeIfNotEmpty(lowLeaf, halfRange);
		BlockPointer highIndex = writeIfNotEmpty(highLeaf, halfRange);

		aNode.split(aIndex, lowIndex, highIndex);

		lowLeaf.gc();
		highLeaf.gc();

		Log.dec();
		Log.dec();

		return true;
	}


	private void divideLeafEntries(HashTableLeaf aMap, int aLevel, int aHalfRange, HashTableLeaf aLowLeaf, HashTableLeaf aHighLeaf)
	{
		assert mPerformanceTool.tick("divideLeafEntries");

		for (ArrayMapEntry entry : aMap)
		{
			if (computeIndex(entry.getKey(), aLevel) < aHalfRange)
			{
				aLowLeaf.put(entry);
			}
			else
			{
				aHighLeaf.put(entry);
			}
		}
	}


	private BlockPointer writeIfNotEmpty(HashTableLeaf aLeaf, int aRange)
	{
		if (aLeaf.isEmpty())
		{
			return new BlockPointer().setBlockType(BlockType.HOLE).setRange(aRange);
		}

		return writeBlock(aLeaf, aRange);
	}


	boolean removeValue(byte[] aKey, int aLevel, ArrayMapEntry aEntry, HashTableNode aNode)
	{
		assert mPerformanceTool.tick("removeValue");

		mCost.mTreeTraversal++;

		int index = aNode.findPointer(computeIndex(aKey, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
			{
				HashTableNode node = readNode(blockPointer);
				if (removeValue(aKey, aLevel + 1, aEntry, node))
				{
					freeBlock(blockPointer);
					BlockPointer newBlockPointer = writeBlock(node, blockPointer.getRange());
					aNode.setPointer(index, newBlockPointer);
					return true;
				}
				return false;
			}
			case LEAF:
			{
				HashTableLeaf node = readLeaf(blockPointer);
				boolean found = node.remove(aEntry);

				if (found)
				{
					mCost.mEntityRemove++;

					freeBlock(blockPointer);
					BlockPointer newBlockPointer = writeBlock(node, blockPointer.getRange());
					aNode.setPointer(index, newBlockPointer);
				}

				node.gc();
				return found;
			}
			case HOLE:
				return false;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	HashTableLeaf readLeaf(BlockPointer aBlockPointer)
	{
		assert mPerformanceTool.tick("readLeaf");

		assert aBlockPointer.getBlockType() == BlockType.LEAF;

		mCost.mReadBlockLeaf++;

		return mRoot.readLeaf(aBlockPointer);
	}


	HashTableNode readNode(BlockPointer aBlockPointer)
	{
		assert mPerformanceTool.tick("readNode");

		assert aBlockPointer.getBlockType() == BlockType.INDEX;

		mCost.mReadBlockNode++;

		return mRoot.readNode(aBlockPointer);
	}


	private int computeIndex(byte[] aKey, int aLevel)
	{
		return MurmurHash3.hash32(aKey, mHashSeed ^ aLevel) & (mPointersPerNode - 1);
	}


	public String integrityCheck()
	{
		Log.i("integrity check");

		return mRoot.integrityCheck();
	}


	public int getEntryMaximumLength()
	{
		return mLeafSize - HashTableLeaf.OVERHEAD;
	}


	void visit(HashTableVisitor aVisitor)
	{
		mRoot.visit(aVisitor);
	}


	void freeBlock(BlockPointer aBlockPointer)
	{
		mCost.mFreeBlock++;
		mCost.mFreeBlockBytes += aBlockPointer.getAllocatedSize();

		mBlockAccessor.freeBlock(aBlockPointer);
	}


	byte[] readBlock(BlockPointer aBlockPointer)
	{
		assert mPerformanceTool.tick("readBlock");

		mCost.mReadBlock++;
		mCost.mReadBlockBytes += aBlockPointer.getAllocatedSize();

		return mBlockAccessor.readBlock(aBlockPointer);
	}


	BlockPointer writeBlock(Node aNode, int aRange)
	{
		assert mPerformanceTool.tick("writeBlock");

		BlockPointer blockPointer = mBlockAccessor.writeBlock(aNode.array(), 0, aNode.array().length, mTransactionId.get(), aNode.getType(), aRange);

		mCost.mWriteBlock++;
		mCost.mWriteBlockBytes += blockPointer.getAllocatedSize();

		return blockPointer;
	}


	void checkOpen() throws IllegalStateException
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}
	}


	public void scan(ScanResult aScanResult)
	{
		aScanResult.tables++;

		mRoot.scan(aScanResult);
	}
}
