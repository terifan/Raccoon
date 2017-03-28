package org.terifan.raccoon.hashtable;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.Stats;
import org.terifan.raccoon.TransactionCounter;
import org.terifan.raccoon.io.BlockAccessor;
import org.terifan.raccoon.io.BlockPointer;
import org.terifan.raccoon.io.BlockType;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;
import org.terifan.security.messagedigest.MurmurHash3;


public final class HashTable implements AutoCloseable, Iterable<LeafEntry>
{
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
	private boolean mChanged;
	private boolean mForwardCommits;
	private TransactionCounter mTransactionId;


	/**
	 * Open an existing HashTable or create a new HashTable with default settings.
	 */
	public HashTable(IManagedBlockDevice aBlockDevice, byte[] aTableHeader, TransactionCounter aTransactionId, boolean aStandAlone, CompressionParam aCompressionParam) throws IOException
	{
		mTransactionId = aTransactionId;

		if (aTableHeader == null)
		{
			mNodeSize = 4 * aBlockDevice.getBlockSize();
			mLeafSize = 8 * aBlockDevice.getBlockSize();
			mHashSeed = new SecureRandom().nextLong();
		}

		init(aBlockDevice, aTableHeader, aStandAlone, aCompressionParam);
	}


	private void init(IManagedBlockDevice aBlockDevice, byte[] aTableHeader, boolean aStandAlone, CompressionParam aCompressionParam) throws IOException
	{
		mBlockAccessor = new BlockAccessor(aBlockDevice);
		mForwardCommits = aStandAlone;

		if (aCompressionParam != null)
		{
			mBlockAccessor.setCompressionParam(aCompressionParam);
		}

		if (aTableHeader == null)
		{
			Log.i("create hash table");
			Log.inc();

			mPointersPerNode = mNodeSize / BlockPointer.SIZE;
			mWasEmptyInstance = true;
			mRootMap = new LeafNode(mLeafSize);
			mRootBlockPointer = writeBlock(mRootMap, mPointersPerNode);
			mChanged = true;
		}
		else
		{
			Log.i("open hash table");
			Log.inc();

			mRootBlockPointer = new BlockPointer();

			ByteArrayBuffer tmp = new ByteArrayBuffer(aTableHeader);
			mRootBlockPointer.unmarshal(tmp);
			mHashSeed = tmp.readInt64();
			mNodeSize = tmp.readVar32();
			mLeafSize = tmp.readVar32();
			mBlockAccessor.setCompressionParam(new CompressionParam(tmp.readVar32(), tmp.readVar32(), tmp.readVar32()));

			mPointersPerNode = mNodeSize / BlockPointer.SIZE;

			loadRoot();
		}

		Log.dec();
	}


	public byte[] getTableHeader()
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(BlockPointer.SIZE + 8 + 4 + 4 + 1);
		mRootBlockPointer.marshal(buffer);
		buffer.writeInt64(mHashSeed);
		buffer.writeVar32(mNodeSize);
		buffer.writeVar32(mLeafSize);
		buffer.writeVar32(mBlockAccessor.getCompressionParam().getLeaf());
		buffer.writeVar32(mBlockAccessor.getCompressionParam().getNode());
		buffer.writeVar32(mBlockAccessor.getCompressionParam().getBlob());

		return buffer.trim().array();
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


	public boolean get(LeafEntry aEntry)
	{
		checkOpen();

		if (mRootMap != null)
		{
			return mRootMap.get(aEntry);
		}
		else
		{
			return getValue(computeHash(aEntry.mKey), 0, aEntry, mRootNode);
		}
	}


	public boolean put(LeafEntry aEntry)
	{
		checkOpen();

		if (aEntry.mKey.length + aEntry.mValue.length > getEntryMaximumLength())
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.mKey.length + ", value: " + aEntry.mValue.length + ", maximum: " + getEntryMaximumLength());
		}

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		mChanged = true;

		if (mRootMap != null)
		{
			Log.v("put root value");

			if (!mRootMap.put(aEntry))
			{
				Log.v("upgrade root leaf to node");

				mRootNode = splitLeaf(mRootBlockPointer, mRootMap, 0);

				mRootBlockPointer = writeBlock(mRootNode, mPointersPerNode);
				mRootMap = null;
			}
		}

		if (mRootMap == null)
		{
			putValue(aEntry, computeHash(aEntry.mKey), 0, mRootNode);
		}

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return aEntry.mValue != null;
	}


	public boolean remove(LeafEntry aEntry)
	{
		checkOpen();

		boolean modified;

		if (mRootMap != null)
		{
			modified = mRootMap.remove(aEntry);
		}
		else
		{
			modified = removeValue(computeHash(aEntry.mKey), 0, aEntry, mRootNode);
		}

		mChanged |= modified;

		return modified;
	}


	@Override
	public Iterator<LeafEntry> iterator()
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
		else
		{
			return new ArrayList<LeafEntry>().iterator();
		}
	}


	public ArrayList<LeafEntry> list()
	{
		checkOpen();

		ArrayList<LeafEntry> list = new ArrayList<>();

		for (Iterator<LeafEntry> it = iterator(); it.hasNext(); )
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

				if (mForwardCommits)
				{
					mBlockAccessor.getBlockDevice().commit();
				}

				mChanged = false;

				Log.i("commit finished; new root %s", mRootBlockPointer);

				Log.dec();
				assert mModCount == modCount : "concurrent modification";

				return true;
			}
			else if (mWasEmptyInstance && mForwardCommits)
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

		if (mForwardCommits)
		{
			mBlockAccessor.getBlockDevice().rollback();
		}

		mRootNode = null;
		mRootMap = null;
		mChanged = false;

		if (mWasEmptyInstance)
		{
			Log.v("rollback empty");

			// occurs when the hashtable is created and never been commited thus rollback is to an empty hashtable
			mRootMap = new LeafNode(mLeafSize);
		}
		else
		{
			Log.v("rollback %s", mRootBlockPointer.getType() == BlockType.NODE_LEAF ? "root map" : "root node");

			loadRoot();
		}
	}


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
				if (aPointerIndex != Visitor.ROOT_POINTER && aBlockPointer != null && (aBlockPointer.getType() == BlockType.NODE_INDEX || aBlockPointer.getType() == BlockType.NODE_LEAF))
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


	private boolean getValue(long aHash, int aLevel, LeafEntry aEntry, IndexNode aNode)
	{
		Stats.getValue.incrementAndGet();
		Log.i("get value");

		int index = aNode.findPointer(computeIndex(aHash, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);

		switch (blockPointer.getType())
		{
			case NODE_INDEX:
				return getValue(aHash, aLevel + 1, aEntry, readNode(blockPointer));
			case NODE_LEAF:
				return readLeaf(blockPointer).get(aEntry);
			case NODE_HOLE:
				return false;
			case NODE_FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	private byte[] putValue(LeafEntry aEntry, long aHash, int aLevel, IndexNode aNode)
	{
		Stats.putValue.incrementAndGet();
		Log.v("put value");
		Log.inc();

		int index = aNode.findPointer(computeIndex(aHash, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);
		byte[] oldValue;

		switch (blockPointer.getType())
		{
			case NODE_INDEX:
				IndexNode node = readNode(blockPointer);
				oldValue = putValue(aEntry, aHash, aLevel + 1, node);
				freeBlock(blockPointer);
				aNode.setPointer(index, writeBlock(node, blockPointer.getRange()));
				break;
			case NODE_LEAF:
				oldValue = putValueLeaf(blockPointer, index, aEntry, aLevel, aNode, aHash);
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


	private byte[] putValueLeaf(BlockPointer aBlockPointer, int aIndex, LeafEntry aEntry, int aLevel, IndexNode aNode, long aHash)
	{
		Stats.putValueLeaf.incrementAndGet();

		LeafNode map = readLeaf(aBlockPointer);

		byte[] oldValue;

		if (map.put(aEntry))
		{
			oldValue = aEntry.mValue;

			freeBlock(aBlockPointer);

			aNode.setPointer(aIndex, writeBlock(map, aBlockPointer.getRange()));
		}
		else if (splitLeaf(map, aBlockPointer, aIndex, aLevel, aNode))
		{
			oldValue = putValue(aEntry, aHash, aLevel, aNode); // recursive put
		}
		else
		{
			IndexNode node = splitLeaf(aBlockPointer, map, aLevel + 1);

			oldValue = putValue(aEntry, aHash, aLevel + 1, node); // recursive put

			aNode.setPointer(aIndex, writeBlock(node, aBlockPointer.getRange()));
		}

		return oldValue;
	}


	private byte[] upgradeHoleToLeaf(LeafEntry aEntry, IndexNode aNode, BlockPointer aBlockPointer, int aIndex)
	{
		Stats.upgradeHoleToLeaf.incrementAndGet();
		Log.v("upgrade hole to leaf");
		Log.inc();

		LeafNode map = new LeafNode(mLeafSize);

		if (!map.put(aEntry))
		{
			throw new DatabaseException("Failed to upgrade hole to leaf");
		}

		byte[] oldValue = aEntry.mValue;

		BlockPointer blockPointer = writeBlock(map, aBlockPointer.getRange());
		aNode.setPointer(aIndex, blockPointer);

		Log.dec();

		return oldValue;
	}


	private IndexNode splitLeaf(BlockPointer aBlockPointer, LeafNode aMap, int aLevel)
	{
		Log.inc();
		Log.v("split leaf");
		Log.inc();

		Stats.splitLeaf.incrementAndGet();

		freeBlock(aBlockPointer);

		LeafNode lowLeaf = new LeafNode(mLeafSize);
		LeafNode highLeaf = new LeafNode(mLeafSize);
		int halfRange = mPointersPerNode / 2;

		divideLeafEntries(aMap, aLevel, halfRange, lowLeaf, highLeaf);

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

		Stats.splitLeaf.incrementAndGet();
		Log.inc();
		Log.v("split leaf");
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
		for (LeafEntry entry : aMap)
		{
			if (computeIndex(computeHash(entry.mKey), aLevel) < aHalfRange)
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


	private boolean removeValue(long aHash, int aLevel, LeafEntry aEntry, IndexNode aNode)
	{
		Stats.removeValue.incrementAndGet();

		int index = aNode.findPointer(computeIndex(aHash, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);

		switch (blockPointer.getType())
		{
			case NODE_INDEX:
				IndexNode node = readNode(blockPointer);
				if (removeValue(aHash, aLevel + 1, aEntry, node))
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


	public BlockAccessor getBlockAccessor()
	{
		return mBlockAccessor;
	}


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
				for (LeafEntry entry : leafNode)
				{
//					if (entry.hasFlag(LeafEntry.FLAG_BLOB))
//					{
//						aScanResult.blobs++;
//
//						ByteArrayBuffer byteArrayBuffer = new ByteArrayBuffer(entry.mValue);
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


//	<T> Stream<T> stream()
//	{
//		checkOpen();
//
//		final NodeIterator nodeIterator;
//
//		if (mRootNode != null)
//		{
//			nodeIterator = new NodeIterator(this, mRootBlockPointer);
//		}
//		else if (!mRootMap.isEmpty())
//		{
//			nodeIterator = new NodeIterator(this, mRootMap);
//		}
//		else
//		{
//			return new ArrayList<T>().stream();
//		}
//
//		return StreamSupport.stream(new AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.IMMUTABLE | Spliterator.NONNULL)
//		{
//			int i;
//			@Override
//			public boolean tryAdvance(Consumer<? super T> aConsumer)
//			{
//				if (!nodeIterator.hasNext())
//				{
//					return false;
//				}
//				aConsumer.accept(nodeIterator.next());
//				return true;
//			}
//		}, false);
//	}
}
