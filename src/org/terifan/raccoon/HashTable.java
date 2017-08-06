package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import java.io.IOException;
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

	private Node mRoot;

	private int mNodeSize;
	private int mLeafSize;
	private long mHashSeed;
	private final int mPointersPerNode;
	private final int mBitsPerNode;
	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private boolean mChanged;
	private boolean mCommitChangesToBlockDevice;
	/*private*/ int mModCount;
	private final byte[] mTableHeader;


	/**
	 * Open an existing HashTable or create a new HashTable with default settings.
	 */
	public HashTable(IManagedBlockDevice aBlockDevice, byte[] aTableHeader, TransactionGroup aTransactionId, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName, Cost aCost) throws IOException
	{
		mTableName = aTableName;
		mTransactionId = aTransactionId;
		mCommitChangesToBlockDevice = aCommitChangesToBlockDevice;
		mBlockAccessor = new BlockAccessor(aBlockDevice, aCompressionParam, aTableParam.getBlockReadCacheSize());
		mTableHeader = aTableHeader;

		if (aTableHeader == null)
		{
			Log.i("create table %s", mTableName);
			Log.inc();

			mNodeSize = aTableParam.getPagesPerNode() * aBlockDevice.getBlockSize();
			mLeafSize = aTableParam.getPagesPerLeaf() * aBlockDevice.getBlockSize();
			mHashSeed = 4; //new SecureRandom().nextInt();

			mPointersPerNode = mNodeSize / BlockPointer.SIZE;
			mBitsPerNode = (int)(Math.log(mPointersPerNode) / Math.log(2));
			mRoot = new HashTableLeaf(this, null);
			mRoot.writeBlock(0, mPointersPerNode, 0);
			mWasEmptyInstance = true;
			mChanged = true;
		}
		else
		{
			Log.i("open table %s", mTableName);
			Log.inc();

			unmarshalHeader();

			mPointersPerNode = mNodeSize / BlockPointer.SIZE;
			mBitsPerNode = (int)(Math.log(mPointersPerNode) / Math.log(2));
		}

		assert mPointersPerNode == (1 << mBitsPerNode);

		Log.dec();
	}


	public byte[] marshalHeader()
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(BlockPointer.SIZE + 8 + 4 + 4 + 3);
		mRoot.getBlockPointer().marshal(buffer);
		buffer.writeInt64(mHashSeed);
		buffer.writeVar32(mNodeSize);
		buffer.writeVar32(mLeafSize);
		mBlockAccessor.getCompressionParam().marshal(buffer);

		return buffer.trim().array();
	}


	private void unmarshalHeader()
	{
		BlockPointer bp = new BlockPointer();

		ByteArrayBuffer buffer = new ByteArrayBuffer(mTableHeader);
		bp.unmarshal(buffer);

		mHashSeed = buffer.readInt64();
		mNodeSize = buffer.readVar32();
		mLeafSize = buffer.readVar32();

		CompressionParam compressionParam = new CompressionParam();
		compressionParam.unmarshal(buffer);

		mBlockAccessor.setCompressionParam(compressionParam);

		mRoot = null;
		mRoot = read(bp, null);
	}


	public boolean get(ArrayMapEntry aEntry)
	{
		checkOpen();

		long hashCode = computeHashCode(aEntry.getKey());
		Node node = mRoot;

		while (node.getBlockType() == BlockType.INDEX)
		{
			BlockPointer blockPointer = ((HashTableNode)node).getPointerByHash(hashCode);

			if (blockPointer.getBlockType() == BlockType.HOLE)
			{
				return false;
			}

			node = ((HashTableNode)node).readBlock(blockPointer);
		}

		return ((HashTableLeaf)node).get(aEntry);
	}


	public boolean put(ArrayMapEntry aEntry)
	{
		checkOpen();

		if (aEntry.getKey().length + aEntry.getValue().length + 1 > getEntryMaximumLength())
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.getKey().length + ", value: " + aEntry.getValue().length + ", maximum: " + getEntryMaximumLength());
		}

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		mChanged = true;

		long hashCode = computeHashCode(aEntry.getKey());
		Node node = mRoot;

		BlockPointer blockPointer = mRoot.getBlockPointer();

		while (blockPointer.getBlockType() == BlockType.INDEX)
		{
			blockPointer = ((HashTableNode)node).getPointerByHash(hashCode);

			if (blockPointer.getBlockType() == BlockType.HOLE)
			{
				break;
			}

			node = ((HashTableNode)node).readBlock(blockPointer);
		}

		for (;;)
		{
			if (blockPointer.getBlockType() == BlockType.HOLE)
			{
				node = ((HashTableNode)node).upgrade(blockPointer, aEntry);
				break;
			}
			if (((HashTableLeaf)node).put(aEntry))
			{
				break;
			}

			node = ((HashTableLeaf)node).split();

			blockPointer = ((HashTableNode)node).getPointerByHash(hashCode);

			if (blockPointer.getBlockType() != BlockType.HOLE)
			{
				node = ((HashTableNode)node).readBlock(blockPointer);
			}
		}

		for (;node != null && node.getParent() != null; node = node.getParent())
		{
			node.freeBlock();
			node.writeBlock();
		}

		mRoot = node;

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return aEntry.getValue() != null;
	}


	public boolean remove(ArrayMapEntry aEntry)
	{
		checkOpen();

		boolean modified;

		modified = mRoot.remove(aEntry);

		mChanged |= modified;

		return modified;
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		checkOpen();

		if (mRoot.getBlockType() == BlockType.INDEX)
		{
			return new HashTableNodeIterator(this, mRoot.getBlockPointer());
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
				mRoot.writeBlock();
				
				mRoot.flush();

				if (mCommitChangesToBlockDevice)
				{
					mBlockAccessor.getBlockDevice().commit();
				}

				mChanged = false;

				Log.i("table commit finished; root block is %s", mRoot.getBlockPointer());

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
			Log.d("rollback %s", mRoot.getBlockPointer().getBlockType() == BlockType.LEAF ? "root map" : "root node");

			unmarshalHeader();
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
					{
						new HashTableNode(this, aParent, aBlockPointer).freeBlock();
					}
					else
					{
						new HashTableLeaf(this, aParent, aBlockPointer).freeBlock();
					}
				}
			}, null);

			mRoot.freeBlock();

			mRoot = new HashTableLeaf(this, null);
		}

		mRoot.writeBlock(0, mPointersPerNode, 0);

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
				result.set(result.get() + ((HashTableLeaf)aParent.readBlock(aBlockPointer)).size());
			}
		}, null);

		return result.get();
	}


	Node read(BlockPointer aBlockPointer, HashTableNode aParent)
	{
		if (aBlockPointer.getBlockType() == BlockType.LEAF)
		{
			return new HashTableLeaf(this, aParent, aBlockPointer);
		}

		return new HashTableNode(this, aParent, aBlockPointer);
	}


	long computeHashCode(byte[] aKey)
	{
		return MurmurHash3.hash64(aKey, mHashSeed);
	}


	int computeIndex(long aHashCode, int aLevel)
	{
		return (int)Long.rotateRight(aHashCode, mBitsPerNode * aLevel) & (mPointersPerNode - 1);
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
			visitNode(aVisitor, mRoot.getBlockPointer(), aParent);
		}

		aVisitor.visit(-1, mRoot.getBlockPointer(), aParent); // start visit at root level
	}


	private void visitNode(HashTableVisitor aVisitor, BlockPointer aBlockPointer, HashTableNode aParent)
	{
		HashTableNode node = (HashTableNode)aParent.readBlock(aBlockPointer);

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


	public BlockAccessor getBlockAccessor()
	{
		return mBlockAccessor;
	}


	public int getLeafSize()
	{
		return mLeafSize;
	}


	public int getPointersPerNode()
	{
		return mPointersPerNode;
	}


	public long getTransactionId()
	{
		return mTransactionId.get();
	}


	public int getNodeSize()
	{
		return mNodeSize;
	}


	public String getTableName()
	{
		return mTableName;
	}


	public void scan(ScanResult aScanResult, HashTableNode aParent)
	{
		aScanResult.tables++;

		HashTable.this.scan(aScanResult, mRoot.getBlockPointer(), aParent);
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
							HashTable.this.scan(aScanResult, pointer, aParent);
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

					HashTable.this.scan(aScanResult, new BlockPointer().unmarshal(byteArrayBuffer), aParent);

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


	void scan()
	{
		System.out.println();
		System.out.println(mRoot.getBlockPointer());

		if (mRoot.getBlockType() == BlockType.INDEX)
		{
			scanNode((HashTableNode)mRoot, 1);
		}

		System.out.println();
	}


	private void scanNode(HashTableNode aNode, int aLevel)
	{
		for (int i = 0; i < aNode.getPointerCount(); i++)
		{
			BlockPointer blockPointer = aNode.getPointer(i);
			
			if (blockPointer != null && blockPointer.getLevel() != aLevel)
			{
				throw new IllegalStateException(blockPointer.getLevel()+" != "+aLevel);
			}

			for (int j = 0; j < aLevel; j++)
			{
				System.out.print("... ");
			}
			System.out.println(blockPointer == null ? "-" : blockPointer);

			if (blockPointer != null)
			{
				if (blockPointer.getBlockType() == BlockType.INDEX)
				{
					HashTableNode node = (HashTableNode)aNode.readBlock(blockPointer);

					scanNode(node, aLevel + 1);
				}
				else if (blockPointer.getBlockType() == BlockType.LEAF)
				{
					HashTableLeaf leaf = (HashTableLeaf)aNode.readBlock(blockPointer);

					for (ArrayMapEntry entry : leaf.getMap())
					{
						for (int j = 0; j <= aLevel; j++)
						{
							System.out.print("... ");
						}
						System.out.print("[");
						for (int j = 0; j < aLevel; j++)
						{
							if (j > 0)
							{
								System.out.print("-");
							}
							System.out.print(computeIndex(computeHashCode(entry.getKey()), j));
						}
						System.out.print("] ");
						System.out.println(new String(entry.getKey()));
					}
				}
			}
		}
	}
}
