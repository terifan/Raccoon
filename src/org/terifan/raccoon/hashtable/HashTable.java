package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.core.Node;
import org.terifan.raccoon.core.TableImplementation;
import org.terifan.raccoon.core.RecordEntry;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.PerformanceCounters;
import org.terifan.raccoon.core.ScanResult;
import static org.terifan.raccoon.PerformanceCounters.*;
import org.terifan.raccoon.TableParam;
import org.terifan.raccoon.TransactionCounter;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.core.BlockType;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;
import org.terifan.security.messagedigest.MurmurHash3;


public final class HashTable extends TableImplementation
{
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
	private TransactionCounter mTransactionId;


	/**
	 * Open an existing HashTable or create a new HashTable with default settings.
	 */
	public HashTable(IManagedBlockDevice aBlockDevice, byte[] aTableHeader, TransactionCounter aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam) throws IOException
	{
		mTransactionId = aTransactionId;
		mCommitChangesToBlockDevice = aCommitChangesToBlockDevice;
		mBlockAccessor = new BlockAccessor(aBlockDevice, aCompressionParam, aTableParam.getBlockReadCacheSize());

		if (aTableHeader == null)
		{
			Log.i("create hash table");
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
			Log.i("open hash table");
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
		ByteArrayBuffer buffer = new ByteArrayBuffer(BlockPointer.SIZE + 8 + 4 + 4 + 1);
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
		CompressionParam compressionParam = new CompressionParam();

		ByteArrayBuffer buffer = new ByteArrayBuffer(aTableHeader);
		mRootBlockPointer.unmarshal(buffer);
		mHashSeed = buffer.readInt32();
		mNodeSize = buffer.readVar32();
		mLeafSize = buffer.readVar32();
		compressionParam.unmarshal(buffer);

		mBlockAccessor.setCompressionParam(compressionParam);
	}


	private void loadRoot()
	{
		Log.i("load root %s", mRootBlockPointer);

		if (mRootBlockPointer.getType() == BlockType.NODE_LEAF)
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

		for (Iterator<RecordEntry> it = iterator(); it.hasNext(); )
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

				Log.i("commit finished; new root %s", mRootBlockPointer);

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
			Log.d("rollback %s", mRootBlockPointer.getType() == BlockType.NODE_LEAF ? "root map" : "root node");

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
				if (aPointerIndex >= 0 && aBlockPointer != null && (aBlockPointer.getType() == BlockType.NODE_INDEX || aBlockPointer.getType() == BlockType.NODE_LEAF))
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

		visit((aPointerIndex, aBlockPointer)->
		{
			if (aBlockPointer != null && aBlockPointer.getType() == BlockType.NODE_LEAF)
			{
				result.set(result.get() + readLeaf(aBlockPointer).size());
			}
		});

		return result.get();
	}


	private boolean getValue(byte[] aKey, int aLevel, RecordEntry aEntry, IndexNode aNode)
	{
		assert PerformanceCounters.increment(GET_VALUE);
		Log.i("get value");

		BlockPointer blockPointer = aNode.getPointer(aNode.findPointer(computeIndex(aKey, aLevel)));

		switch (blockPointer.getType())
		{
			case NODE_INDEX:
				return getValue(aKey, aLevel + 1, aEntry, readNode(blockPointer));
			case NODE_LEAF:
				return readLeaf(blockPointer).get(aEntry);
			case NODE_HOLE:
				return false;
			case NODE_FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	private byte[] putValue(RecordEntry aEntry, byte[] aKey, int aLevel, IndexNode aNode)
	{
		assert PerformanceCounters.increment(PUT_VALUE);
		Log.d("put value");
		Log.inc();

		int index = aNode.findPointer(computeIndex(aKey, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);
		byte[] oldValue;

		switch (blockPointer.getType())
		{
			case NODE_INDEX:
				IndexNode node = readNode(blockPointer);
				oldValue = putValue(aEntry, aKey, aLevel + 1, node);
				freeBlock(blockPointer);
				aNode.setPointer(index, writeBlock(node, blockPointer.getRange()));
				break;
			case NODE_LEAF:
				oldValue = putValueLeaf(blockPointer, index, aEntry, aLevel, aNode, aKey);
				break;
			case NODE_HOLE:
				oldValue = upgradeHoleToLeaf(aEntry, aNode, blockPointer, index);
				break;
			case NODE_FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}

		Log.dec();

		return oldValue;
	}


	private byte[] putValueLeaf(BlockPointer aBlockPointer, int aIndex, RecordEntry aEntry, int aLevel, IndexNode aNode, byte[] aKey)
	{
		assert PerformanceCounters.increment(PUT_VALUE_LEAF);

		LeafNode map = readLeaf(aBlockPointer);

		byte[] oldValue;

		if (map.put(aEntry))
		{
			oldValue = aEntry.getValue();

			freeBlock(aBlockPointer);

			aNode.setPointer(aIndex, writeBlock(map, aBlockPointer.getRange()));
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
		assert PerformanceCounters.increment(UPGRADE_HOLE_TO_LEAF);
		Log.d("upgrade hole to leaf");
		Log.inc();

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

		assert PerformanceCounters.increment(SPLIT_LEAF);

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

		assert PerformanceCounters.increment(SPLIT_LEAF);
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
			return new BlockPointer().setType(BlockType.NODE_HOLE).setRange(aRange);
		}

		return writeBlock(aLeaf, aRange);
	}


	private boolean removeValue(byte[] aKey, int aLevel, RecordEntry aEntry, IndexNode aNode)
	{
		assert PerformanceCounters.increment(REMOVE_VALUE);

		int index = aNode.findPointer(computeIndex(aKey, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);

		switch (blockPointer.getType())
		{
			case NODE_INDEX:
				IndexNode node = readNode(blockPointer);
				if (removeValue(aKey, aLevel + 1, aEntry, node))
				{
					freeBlock(blockPointer);
					BlockPointer newBlockPointer = writeBlock(node, blockPointer.getRange());
					aNode.setPointer(index, newBlockPointer);
					return true;
				}
				return false;
			case NODE_LEAF:
				LeafNode map = readLeaf(blockPointer);
				if (map.remove(aEntry))
				{
					freeBlock(blockPointer);
					BlockPointer newBlockPointer = writeBlock(map, blockPointer.getRange());
					aNode.setPointer(index, newBlockPointer);
					return true;
				}
				return false;
			case NODE_HOLE:
				return false;
			case NODE_FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	LeafNode readLeaf(BlockPointer aBlockPointer)
	{
		assert aBlockPointer.getType() == BlockType.NODE_LEAF;

		if (aBlockPointer.getOffset() == mRootBlockPointer.getOffset() && mRootMap != null)
		{
			return mRootMap;
		}

		return new LeafNode(readBlock(aBlockPointer));
	}


	IndexNode readNode(BlockPointer aBlockPointer)
	{
		assert aBlockPointer.getType() == BlockType.NODE_INDEX;

		if (aBlockPointer.getOffset() == mRootBlockPointer.getOffset() && mRootNode != null)
		{
			return mRootNode;
		}

		return new IndexNode(readBlock(aBlockPointer));
	}


	private int computeIndex(byte[] aKey, int aLevel)
	{
		return MurmurHash3.hash_x86_32(aKey, mHashSeed ^ aLevel) & (mPointersPerNode - 1);
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

			if (next != null && next.getType() == BlockType.NODE_INDEX)
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


	private BlockPointer writeBlock(Node aNode, int aRange)
	{
		return mBlockAccessor.writeBlock(aNode.array(), 0, aNode.array().length, mTransactionId.get(), aNode.getType(), aRange);
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

		switch (aBlockPointer.getType())
		{
			case NODE_INDEX:
				aScanResult.indexBlocks++;

				IndexNode indexNode = new IndexNode(buffer);
				for (int i = 0; i < indexNode.getPointerCount(); i++)
				{
					BlockPointer pointer = indexNode.getPointer(i);
					if (pointer != null)
					{
						scan(aScanResult, pointer);
					}
				}
				break;
			case NODE_LEAF:
				LeafNode leafNode = new LeafNode(buffer);
				for (RecordEntry entry : leafNode)
				{
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
						aScanResult.records++;
//					}
				}
				break;
			case BLOB_INDEX:
				aScanResult.blobIndices++;

				ByteArrayBuffer byteArrayBuffer = new ByteArrayBuffer(buffer);
				while (byteArrayBuffer.remaining() > 0)
				{
					scan(aScanResult, new BlockPointer().unmarshal(byteArrayBuffer));
				}
				break;
			case BLOB_DATA:
				aScanResult.blobData++;

				break;
			default:
				throw new IllegalStateException();
		}
	}
}
