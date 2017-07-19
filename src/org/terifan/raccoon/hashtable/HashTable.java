package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.Node;
import org.terifan.raccoon.TableImplementation;
import org.terifan.raccoon.RecordEntry;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.TableParam;
import org.terifan.raccoon.TransactionGroup;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.BlockType;
import org.terifan.raccoon.Cost;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;
import org.terifan.security.messagedigest.MurmurHash3;


public final class HashTable extends TableImplementation
{
	private final Cost mCost;
	private final String mTableName;
	private final TransactionGroup mTransactionId;
	private BlockAccessor mBlockAccessor;
	private BlockPointer mRootBlockPointer;
	private LeafNode mRootMap;
	private IndexNode mRootNode;
	private int mNodeSize;
	private int mLeafSize;
	private int mPointersPerNode;
	/*private*/ int mModCount;
	private int mHashSeed;
	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private boolean mChanged;
	private boolean mCommitChangesToBlockDevice;


	/**
	 * Open an existing HashTable or create a new HashTable with default settings.
	 */
	public HashTable(IManagedBlockDevice aBlockDevice, byte[] aTableHeader, TransactionGroup aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName, Cost aCost) throws IOException
	{
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
			mRootMap = new LeafNode(mLeafSize);
			mRootBlockPointer = writeBlock(mRootMap, mPointersPerNode);
			mWasEmptyInstance = true;
			mChanged = true;
		}
		else
		{
			Log.i("open table %s", mTableName);
			Log.inc();

			unmarshalHeader(aTableHeader);

			mPointersPerNode = mNodeSize / BlockPointer.SIZE;

			loadRoot();
		}

		Log.dec();
	}


	@Override
	public byte[] marshalHeader()
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(BlockPointer.SIZE + 4 + 4 + 4 + 3);
		mRootBlockPointer.marshal(buffer);
		buffer.writeInt32(mHashSeed);
		buffer.writeVar32(mNodeSize);
		buffer.writeVar32(mLeafSize);
		mBlockAccessor.getCompressionParam().marshal(buffer);

		return buffer.trim().array();
	}


	private void unmarshalHeader(byte[] aTableHeader)
	{
		mRootBlockPointer = new BlockPointer();

		ByteArrayBuffer buffer = new ByteArrayBuffer(aTableHeader);
		mRootBlockPointer.unmarshal(buffer);

		mHashSeed = buffer.readInt32();
		mNodeSize = buffer.readVar32();
		mLeafSize = buffer.readVar32();

		CompressionParam compressionParam = new CompressionParam();
		compressionParam.unmarshal(buffer);

		mBlockAccessor.setCompressionParam(compressionParam);
	}


	private void loadRoot()
	{
		Log.i("load root %s", mRootBlockPointer);

		if (mRootBlockPointer.getBlockType() == BlockType.LEAF)
		{
			mRootMap = readLeaf(mRootBlockPointer);
		}
		else
		{
			mRootNode = readNode(mRootBlockPointer);
		}
	}


	@Override
	public boolean get(RecordEntry aEntry)
	{
		checkOpen();

		if (mRootMap != null)
		{
			return mRootMap.get(aEntry);
		}

		return getValue(aEntry.getKey(), 0, aEntry, mRootNode);
	}


	@Override
	public boolean put(RecordEntry aEntry)
	{
		checkOpen();

		if (aEntry.getKey().length + aEntry.getValue().length > getEntryMaximumLength())
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.getKey().length + ", value: " + aEntry.getValue().length + ", maximum: " + getEntryMaximumLength());
		}

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		mChanged = true;

		if (mRootMap != null)
		{
			Log.d("put root value");

			if (!mRootMap.put(aEntry))
			{
				Log.d("upgrade root leaf to node");

				mRootNode = splitLeaf(mRootBlockPointer, mRootMap, 0);

				mRootBlockPointer = writeBlock(mRootNode, mPointersPerNode);
				mRootMap = null;
			}
		}

		if (mRootMap == null)
		{
			putValue(aEntry, aEntry.getKey(), 0, mRootNode);
		}

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return aEntry.getValue() != null;
	}


	@Override
	public boolean remove(RecordEntry aEntry)
	{
		checkOpen();

		boolean modified;

		if (mRootMap != null)
		{
			modified = mRootMap.remove(aEntry);
		}
		else
		{
			modified = removeValue(aEntry.getKey(), 0, aEntry, mRootNode);
		}

		mChanged |= modified;

		return modified;
	}


	@Override
	public Iterator<RecordEntry> iterator()
	{
		checkOpen();

		if (mRootNode != null)
		{
			return new NodeIterator(this, mRootBlockPointer);
		}
		else if (!mRootMap.isEmpty())
		{
			return new NodeIterator(this, mRootMap);
		}

		return new ArrayList<RecordEntry>().iterator();
	}


	@Override
	public ArrayList<RecordEntry> list()
	{
		checkOpen();

		ArrayList<RecordEntry> list = new ArrayList<>();

		for (Iterator<RecordEntry> it = iterator(); it.hasNext();)
		{
			list.add(it.next());
		}

		return list;
	}


	@Override
	public boolean isChanged()
	{
		return mChanged;
	}


	@Override
	public boolean commit() throws IOException
	{
		checkOpen();

		try
		{
			if (mChanged)
			{
				int modCount = mModCount; // no increment
				Log.i("commit hash table");
				Log.inc();

				freeBlock(mRootBlockPointer);

				if (mRootMap != null)
				{
					mRootBlockPointer = writeBlock(mRootMap, mPointersPerNode);
				}
				else
				{
					mRootBlockPointer = writeBlock(mRootNode, mPointersPerNode);
				}

				if (mCommitChangesToBlockDevice)
				{
					mBlockAccessor.getBlockDevice().commit();
				}

				mChanged = false;

				Log.i("table commit finished; root block is %s", mRootBlockPointer);

				Log.dec();
				assert mModCount == modCount : "concurrent modification";

				return true;
			}
			else if (mWasEmptyInstance && mCommitChangesToBlockDevice)
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


	@Override
	public void rollback() throws IOException
	{
		checkOpen();

		Log.i("rollback");

		if (mCommitChangesToBlockDevice)
		{
			mBlockAccessor.getBlockDevice().rollback();
		}

		mRootNode = null;
		mRootMap = null;
		mChanged = false;

		if (mWasEmptyInstance)
		{
			Log.d("rollback empty");

			// occurs when the hashtable is created and never been commited thus rollback is to an empty hashtable
			mRootMap = new LeafNode(mLeafSize);
		}
		else
		{
			Log.d("rollback %s", mRootBlockPointer.getBlockType() == BlockType.LEAF ? "root map" : "root node");

			loadRoot();
		}
	}


	@Override
	public void clear()
	{
		checkOpen();

		Log.i("clear");

		int modCount = ++mModCount;
		mChanged = true;

		if (mRootMap != null)
		{
			mRootMap.clear();
		}
		else
		{
			visit((aPointerIndex, aBlockPointer) ->
			{
				mCost.mTreeTraversal++;

				if (aPointerIndex >= 0 && aBlockPointer != null && (aBlockPointer.getBlockType() == BlockType.INDEX || aBlockPointer.getBlockType() == BlockType.LEAF))
				{
					freeBlock(aBlockPointer);
				}
			});

			mRootNode = null;
			mRootMap = new LeafNode(mLeafSize);
		}

		freeBlock(mRootBlockPointer);

		mRootBlockPointer = writeBlock(mRootMap, mPointersPerNode);

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
		mRootMap = null;
		mRootNode = null;
	}


	@Override
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


	private boolean getValue(byte[] aKey, int aLevel, RecordEntry aEntry, IndexNode aNode)
	{
		Log.i("get %s value", mTableName);

		mCost.mTreeTraversal++;

		BlockPointer blockPointer = aNode.getPointer(aNode.findPointer(computeIndex(aKey, aLevel)));

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				return getValue(aKey, aLevel + 1, aEntry, readNode(blockPointer));
			case LEAF:
				mCost.mValueGet++;

				return readLeaf(blockPointer).get(aEntry);
			case HOLE:
				return false;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	private byte[] putValue(RecordEntry aEntry, byte[] aKey, int aLevel, IndexNode aNode)
	{
		Log.d("put %s value", mTableName);
		Log.inc();

		mCost.mTreeTraversal++;

		int index = aNode.findPointer(computeIndex(aKey, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);
		byte[] oldValue;

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				IndexNode node = readNode(blockPointer);
				oldValue = putValue(aEntry, aKey, aLevel + 1, node);
				freeBlock(blockPointer);
				aNode.setPointer(index, writeBlock(node, blockPointer.getRange()));
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


	private byte[] putValueLeaf(BlockPointer aBlockPointer, int aIndex, RecordEntry aEntry, int aLevel, IndexNode aNode, byte[] aKey)
	{
		mCost.mTreeTraversal++;

		LeafNode map = readLeaf(aBlockPointer);

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
			IndexNode node = splitLeaf(aBlockPointer, map, aLevel + 1);

			oldValue = putValue(aEntry, aKey, aLevel + 1, node); // recursive put

			aNode.setPointer(aIndex, writeBlock(node, aBlockPointer.getRange()));
		}

		return oldValue;
	}


	private byte[] upgradeHoleToLeaf(RecordEntry aEntry, IndexNode aNode, BlockPointer aBlockPointer, int aIndex)
	{
		Log.d("upgrade hole to leaf");
		Log.inc();

		mCost.mTreeTraversal++;

		LeafNode map = new LeafNode(mLeafSize);

		if (!map.put(aEntry))
		{
			throw new DatabaseException("Failed to upgrade hole to leaf");
		}

		byte[] oldValue = aEntry.getValue();

		BlockPointer blockPointer = writeBlock(map, aBlockPointer.getRange());
		aNode.setPointer(aIndex, blockPointer);

		Log.dec();

		return oldValue;
	}


	private IndexNode splitLeaf(BlockPointer aBlockPointer, LeafNode aLeafNode, int aLevel)
	{
		Log.inc();
		Log.d("split leaf");
		Log.inc();

		mCost.mTreeTraversal++;
		mCost.mBlockSplit++;

		freeBlock(aBlockPointer);

		LeafNode lowLeaf = new LeafNode(mLeafSize);
		LeafNode highLeaf = new LeafNode(mLeafSize);
		int halfRange = mPointersPerNode / 2;

		divideLeafEntries(aLeafNode, aLevel, halfRange, lowLeaf, highLeaf);

		// create nodes pointing to leafs
		BlockPointer lowIndex = writeIfNotEmpty(lowLeaf, halfRange);
		BlockPointer highIndex = writeIfNotEmpty(highLeaf, halfRange);

		IndexNode node = new IndexNode(new byte[mNodeSize]);
		node.setPointer(0, lowIndex);
		node.setPointer(halfRange, highIndex);

		Log.dec();
		Log.dec();

		return node;
	}


	private boolean splitLeaf(LeafNode aMap, BlockPointer aBlockPointer, int aIndex, int aLevel, IndexNode aNode)
	{
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

		LeafNode lowLeaf = new LeafNode(mLeafSize);
		LeafNode highLeaf = new LeafNode(mLeafSize);
		int halfRange = aBlockPointer.getRange() / 2;

		divideLeafEntries(aMap, aLevel, aIndex + halfRange, lowLeaf, highLeaf);

		// create nodes pointing to leafs
		BlockPointer lowIndex = writeIfNotEmpty(lowLeaf, halfRange);
		BlockPointer highIndex = writeIfNotEmpty(highLeaf, halfRange);

		aNode.split(aIndex, lowIndex, highIndex);

		Log.dec();
		Log.dec();

		return true;
	}


	private void divideLeafEntries(LeafNode aMap, int aLevel, int aHalfRange, LeafNode aLowLeaf, LeafNode aHighLeaf)
	{
		for (RecordEntry entry : aMap)
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


	private BlockPointer writeIfNotEmpty(LeafNode aLeaf, int aRange)
	{
		if (aLeaf.isEmpty())
		{
			return new BlockPointer().setBlockType(BlockType.HOLE).setRange(aRange);
		}

		return writeBlock(aLeaf, aRange);
	}


	private boolean removeValue(byte[] aKey, int aLevel, RecordEntry aEntry, IndexNode aNode)
	{
		mCost.mTreeTraversal++;

		int index = aNode.findPointer(computeIndex(aKey, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				IndexNode node = readNode(blockPointer);
				if (removeValue(aKey, aLevel + 1, aEntry, node))
				{
					freeBlock(blockPointer);
					BlockPointer newBlockPointer = writeBlock(node, blockPointer.getRange());
					aNode.setPointer(index, newBlockPointer);
					return true;
				}
				return false;
			case LEAF:
				LeafNode map = readLeaf(blockPointer);
				if (map.remove(aEntry))
				{
					mCost.mEntityRemove++;

					freeBlock(blockPointer);
					BlockPointer newBlockPointer = writeBlock(map, blockPointer.getRange());
					aNode.setPointer(index, newBlockPointer);
					return true;
				}
				return false;
			case HOLE:
				return false;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	LeafNode readLeaf(BlockPointer aBlockPointer)
	{
		assert aBlockPointer.getBlockType() == BlockType.LEAF;

		if (aBlockPointer.getBlockIndex0() == mRootBlockPointer.getBlockIndex0() && mRootMap != null)
		{
			return mRootMap;
		}

		mCost.mReadBlockLeaf++;

		return new LeafNode(readBlock(aBlockPointer));
	}


	IndexNode readNode(BlockPointer aBlockPointer)
	{
		assert aBlockPointer.getBlockType() == BlockType.INDEX;

		if (aBlockPointer.getBlockIndex0() == mRootBlockPointer.getBlockIndex0() && mRootNode != null)
		{
			return mRootNode;
		}

		mCost.mReadBlockNode++;

		return new IndexNode(readBlock(aBlockPointer));
	}


	private int computeIndex(byte[] aKey, int aLevel)
	{
		return MurmurHash3.hash32(aKey, mHashSeed ^ aLevel) & (mPointersPerNode - 1);
	}


	@Override
	public String integrityCheck()
	{
		Log.i("integrity check");

		if (mRootMap != null)
		{
			return mRootMap.integrityCheck();
		}

		return mRootNode.integrityCheck();
	}


	@Override
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

		aVisitor.visit(-1, mRootBlockPointer); // start visit at root level
	}


	private void visitNode(Visitor aVisitor, BlockPointer aBlockPointer)
	{
		IndexNode node = readNode(aBlockPointer);

		for (int i = 0; i < mPointersPerNode; i++)
		{
			BlockPointer next = node.getPointer(i);

			if (next != null && next.getBlockType() == BlockType.INDEX)
			{
				visitNode(aVisitor, next);
			}

			aVisitor.visit(i, next);
		}
	}


	private void freeBlock(BlockPointer aBlockPointer)
	{
		mCost.mFreeBlock++;
		mCost.mFreeBlockBytes += aBlockPointer.getAllocatedSize();

		mBlockAccessor.freeBlock(aBlockPointer);
	}


	private byte[] readBlock(BlockPointer aBlockPointer)
	{
		mCost.mReadBlock++;
		mCost.mReadBlockBytes += aBlockPointer.getAllocatedSize();

		return mBlockAccessor.readBlock(aBlockPointer);
	}


	private BlockPointer writeBlock(Node aNode, int aRange)
	{
		BlockPointer blockPointer = mBlockAccessor.writeBlock(aNode.array(), 0, aNode.array().length, mTransactionId.get(), aNode.getType(), aRange);

		mCost.mWriteBlock++;
		mCost.mWriteBlockBytes += blockPointer.getAllocatedSize();

		return blockPointer;
	}


	private void checkOpen() throws IllegalStateException
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

		scan(aScanResult, mRootBlockPointer);
	}


	void scan(ScanResult aScanResult, BlockPointer aBlockPointer)
	{
		byte[] buffer = mBlockAccessor.readBlock(aBlockPointer);

		switch (aBlockPointer.getBlockType())
		{
			case INDEX:
				aScanResult.enterNode(aBlockPointer);
				aScanResult.indexBlocks++;

				IndexNode indexNode = new IndexNode(buffer);

				for (int i = 0; i < indexNode.getPointerCount(); i++)
				{
					BlockPointer pointer = indexNode.getPointer(i);

					if (pointer != null)
					{
						if (pointer.getBlockType() == BlockType.HOLE)
						{
							aScanResult.holes++;
						}
						else
						{
							scan(aScanResult, pointer);
						}
					}
				}
				aScanResult.exitNode();
				break;
			case LEAF:
				LeafNode leafNode = new LeafNode(buffer);

				aScanResult.enterLeaf(aBlockPointer, buffer);

				aScanResult.records += leafNode.size();

//				for (RecordEntry entry : leafNode)
//				{
//					aScanResult.entry();
//
//					if (entry.hasFlag(LeafEntry.FLAG_BLOB))
//					{
//						aScanResult.blobs++;
//
//						ByteArrayBuffer byteArrayBuffer = new ByteArrayBuffer(entry.getValue());
//						byteArrayBuffer.readInt8();
//						long len = byteArrayBuffer.readVar64();
//
//						while (byteArrayBuffer.remaining() > 0)
//						{
//							scan(aScanResult, new BlockPointer().unmarshal(byteArrayBuffer));
//						}
//					}
//					else
//					{
//						aScanResult.records++;
//					}
//				}
				aScanResult.exitLeaf();

				break;
			case BLOB_INDEX:
				aScanResult.blobIndices++;

				ByteArrayBuffer byteArrayBuffer = new ByteArrayBuffer(buffer);
				while (byteArrayBuffer.remaining() > 0)
				{
					aScanResult.enterBlob();

					scan(aScanResult, new BlockPointer().unmarshal(byteArrayBuffer));

					aScanResult.exitBlob();
				}
				break;
			case BLOB_DATA:
				aScanResult.blobData++;

				aScanResult.blobData();

				break;
			default:
				throw new IllegalStateException();
		}
	}
}
