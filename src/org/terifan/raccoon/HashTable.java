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
	private final String mTableName;
	private final TransactionGroup mTransactionId;
	private BlockAccessor mBlockAccessor;
	private BlockPointer mRootBlockPointer;

	private Node mRoot;

	private int mNodeSize;
	private int mLeafSize;
	private int mHashSeed;
	private int mPointersPerNode;
	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private boolean mChanged;
	private boolean mCommitChangesToBlockDevice;
	/*private*/ int mModCount;


	/**
	 * Open an existing HashTable or create a new HashTable with default settings.
	 */
	public HashTable(IManagedBlockDevice aBlockDevice, byte[] aTableHeader, TransactionGroup aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName, Cost aCost) throws IOException
	{
		mTableName = aTableName;
		mTransactionId = aTransactionId;
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
			mRoot = new HashTableLeaf(this, null);
			mRootBlockPointer = mRoot.writeBlock(0, mPointersPerNode);
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

		mRoot = null;
		mRoot = read(mRootBlockPointer, null);
	}


	public boolean get(ArrayMapEntry aEntry)
	{
		checkOpen();

//		return mRoot.get(aEntry, 0);

		Node node = mRoot;
		int level = 0;

		while (node.getBlockType() == BlockType.INDEX)
		{
			BlockPointer blockPointer = ((HashTableNode)node).getPointer(((HashTableNode)node).findPointer(computeIndex(aEntry.getKey(), level++)));

			if (blockPointer.getBlockType() == BlockType.HOLE)
			{
				return false;
			}

			node = read(blockPointer, (HashTableNode)node);
		}

		return node.get(aEntry, level);
	}


	public boolean put(ArrayMapEntry aEntry)
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




		BlockPointer blockPointer = mRootBlockPointer;
		Node node = mRoot;
		int level = 0;
		int index = 0;
		ArrayList<Integer> indices = new ArrayList<>();

		while (node.getBlockType() == BlockType.INDEX)
		{
			HashTableNode parent = (HashTableNode)node;

			index = parent.findPointer(computeIndex(aEntry.getKey(), level++));

			indices.add(parent.findPointer(index));

			blockPointer = parent.getPointer(index);

			if (blockPointer.getBlockType() == BlockType.HOLE)
			{
				break;
			}

			node = read(blockPointer, parent);
		}

		for (;;)
		{
			if (blockPointer.getBlockType() == BlockType.HOLE)
			{
				node = new HashTableLeaf(this, (HashTableNode)node);

				if (!node.put(aEntry, level))
				{
					throw new DatabaseException("Failed to upgrade hole to leaf");
				}

				BlockPointer newBlockPointer = node.writeBlock(blockPointer.getRangeOffset(), blockPointer.getRangeSize());

				node.getParent().setPointer(index, newBlockPointer);

				break;
			}
			else if (node.put(aEntry, level))
			{
				if (level != 0)
				{
					node.freeBlock();

					BlockPointer newBlockPointer = node.writeBlock(blockPointer.getRangeOffset(), blockPointer.getRangeSize());

					node.getParent().setPointer(index, newBlockPointer);
				}

				break;
			}
			else
			{
				if (level == 0)
				{
					System.out.println("expand");

					node.freeBlock();

					node = mRoot = ((HashTableLeaf)node).splitLeaf(level);

					mRootBlockPointer = mRoot.writeBlock(0, mPointersPerNode);

					level++;
				}
				else if (node.getBlockPointer().getRangeOffset() > 1)
				{
					System.out.println("split");

					((HashTableLeaf)node).splitLeaf(index, level, node.getParent());

					node = node.getParent();
				}
				else
				{
					level++;

					node = ((HashTableLeaf)node).splitLeaf(level);
				}

				index = ((HashTableNode)node).findPointer(computeIndex(aEntry.getKey(), level));

				indices.add(index);

				blockPointer = ((HashTableNode)node).getPointer(index);

				if (blockPointer.getBlockType() != BlockType.HOLE)
				{
					node = read(blockPointer, ((HashTableNode)node));
				}
			}
		}




		while (node != null && node.getParent() != null)
		{
			node.freeBlock();

			BlockPointer newBlockPointer = node.writeBlock(blockPointer.getRangeOffset(), blockPointer.getRangeSize());

			index = indices.remove(indices.size() - 1);

			node.getParent().setPointer(index, newBlockPointer);

			node = node.getParent();
		}





//		if (mRoot.getBlockType() == BlockType.LEAF)
//		{
//			Log.d("put root value");
//
//			if (mRoot.put(aEntry, 0))
//			{
//				Log.dec();
//				assert mModCount == modCount : "concurrent modification";
//
//				return aEntry.getValue() != null;
//			}
//
//			Log.d("upgrade root leaf to node");
//
//			mRoot.freeBlock();
//
//			mRoot = ((HashTableLeaf)mRoot).splitLeaf(0);
//
//			mRootBlockPointer = mRoot.writeBlock(mPointersPerNode);
//		}
//
//		mRoot.put(aEntry, 0);

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return aEntry.getValue() != null;
	}


	public boolean remove(ArrayMapEntry aEntry)
	{
		checkOpen();

		boolean modified;

		modified = mRoot.remove(aEntry, 0);

		mChanged |= modified;

		return modified;
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		checkOpen();

		if (mRoot.getBlockType() == BlockType.INDEX)
		{
			return new HashTableNodeIterator(this, mRootBlockPointer);
		}
		if (!((HashTableLeaf)mRoot).isEmpty())
		{
			return new HashTableNodeIterator(this, ((HashTableLeaf)mRoot).iterator());
		}

		return new ArrayList<ArrayMapEntry>().iterator();
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
		checkOpen();

		try
		{
			if (mChanged)
			{
				int modCount = mModCount; // no increment
				Log.i("commit hash table");
				Log.inc();

				mRoot.freeBlock();
				mRootBlockPointer = mRoot.writeBlock(0, mPointersPerNode);

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

		mChanged = false;

		if (mWasEmptyInstance)
		{
			Log.d("rollback empty");

			// occurs when the hashtable is created and never been commited thus rollback is to an empty hashtable
			mRoot = new HashTableLeaf(this, null);
		}
		else
		{
			Log.d("rollback %s", mRootBlockPointer.getBlockType() == BlockType.LEAF ? "root map" : "root node");

			loadRoot();
		}
	}


	public void clear()
	{
		checkOpen();

		Log.i("clear");

		int modCount = ++mModCount;
		mChanged = true;

		if (mRoot.getBlockType() == BlockType.LEAF)
		{
			((HashTableLeaf)mRoot).clear();
			mRoot.freeBlock();
		}
		else
		{
			visit((aPointerIndex, aBlockPointer, aParent) ->
			{
				if (aPointerIndex >= 0 && aBlockPointer != null && (aBlockPointer.getBlockType() == BlockType.INDEX || aBlockPointer.getBlockType() == BlockType.LEAF))
				{
					if (aBlockPointer.getBlockType() == BlockType.INDEX)
						new HashTableNode(this, aParent, aBlockPointer).freeBlock();
					else
						new HashTableLeaf(this, aParent, aBlockPointer).freeBlock();
				}
			}, null);

			mRoot.freeBlock();

			mRoot = new HashTableLeaf(this, null);
		}

		mRootBlockPointer = mRoot.writeBlock(0, mPointersPerNode);

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
		mRoot = null;
	}


	public int size()
	{
		checkOpen();

		Result<Integer> result = new Result<>(0);

		visit((aPointerIndex, aBlockPointer, aParent) ->
		{
			if (aBlockPointer != null && aBlockPointer.getBlockType() == BlockType.LEAF)
			{
				result.set(result.get() + readLeaf(aBlockPointer, aParent).size());
			}
		}, null);

		return result.get();
	}


	HashTableLeaf readLeaf(BlockPointer aBlockPointer, HashTableNode aParent)
	{
		assert aBlockPointer.getBlockType() == BlockType.LEAF;

		if (mRoot != null && aBlockPointer.getBlockIndex0() == mRootBlockPointer.getBlockIndex0())
		{
			return (HashTableLeaf)mRoot;
		}

		return new HashTableLeaf(this, aParent, aBlockPointer);
	}


	HashTableNode readNode(BlockPointer aBlockPointer, HashTableNode aParent)
	{
		assert aBlockPointer.getBlockType() == BlockType.INDEX;

		if (mRoot != null && aBlockPointer.getBlockIndex0() == mRootBlockPointer.getBlockIndex0())
		{
			return (HashTableNode)mRoot;
		}

		return new HashTableNode(this, aParent, aBlockPointer);
	}


	Node read(BlockPointer aBlockPointer, HashTableNode aParent)
	{
		if (mRoot != null && aBlockPointer.getBlockIndex0() == mRootBlockPointer.getBlockIndex0() && mRoot.getBlockType() == aBlockPointer.getBlockType())
		{
			return mRoot;
		}

		if (aBlockPointer.getBlockType() == BlockType.INDEX)
		{
			return new HashTableNode(this, aParent, aBlockPointer);
		}

		return new HashTableLeaf(this, aParent, aBlockPointer);
	}


	int computeIndex(byte[] aKey, int aLevel)
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
		return mLeafSize - HashTableLeaf.getOverhead();
	}


	private void visit(HashTableVisitor aVisitor, HashTableNode aParent)
	{
		if (mRoot.getBlockType() == BlockType.INDEX)
		{
			visitNode(aVisitor, mRootBlockPointer, aParent);
		}

		aVisitor.visit(-1, mRootBlockPointer, aParent); // start visit at root level
	}


	private void visitNode(HashTableVisitor aVisitor, BlockPointer aBlockPointer, HashTableNode aParent)
	{
		HashTableNode node = readNode(aBlockPointer, aParent);

		for (int i = 0; i < mPointersPerNode; i++)
		{
			BlockPointer next = node.getPointer(i);

			if (next != null && next.getBlockType() == BlockType.INDEX)
			{
				visitNode(aVisitor, next, aParent);
			}

			aVisitor.visit(i, next, aParent);
		}
	}


	private void checkOpen() throws IllegalStateException
	{
		if (mClosed)
		{
			throw new IllegalStateException("HashTable is closed");
		}
	}


	public void scan(ScanResult aScanResult, HashTableNode aParent)
	{
		aScanResult.tables++;

		scan(aScanResult, mRootBlockPointer, aParent);
	}


	void scan(ScanResult aScanResult, BlockPointer aBlockPointer, HashTableNode aParent)
	{
		switch (aBlockPointer.getBlockType())
		{
			case INDEX:
				aScanResult.enterNode(aBlockPointer);
				aScanResult.indexBlocks++;

				HashTableNode indexNode = new HashTableNode(this, aParent, aBlockPointer);

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
							scan(aScanResult, pointer, aParent);
						}
					}
				}
				aScanResult.exitNode();
				break;
			case LEAF:
				HashTableLeaf leafNode = new HashTableLeaf(this, aParent, aBlockPointer);

				aScanResult.enterLeaf(aBlockPointer, leafNode.array());

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

				ByteArrayBuffer byteArrayBuffer = new ByteArrayBuffer(mBlockAccessor.readBlock(aBlockPointer));
				while (byteArrayBuffer.remaining() > 0)
				{
					aScanResult.enterBlob();

					scan(aScanResult, new BlockPointer().unmarshal(byteArrayBuffer), aParent);

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


	public BlockAccessor getBlockAccessor()
	{
		return mBlockAccessor;
	}


	public int getHashSeed()
	{
		return mHashSeed;
	}


	public int getLeafSize()
	{
		return mLeafSize;
	}


	public int getPointersPerNode()
	{
		return mPointersPerNode;
	}


	public TransactionGroup getTransactionId()
	{
		return mTransactionId;
	}


	public int getNodeSize()
	{
		return mNodeSize;
	}


	public String getTableName()
	{
		return mTableName;
	}
}
