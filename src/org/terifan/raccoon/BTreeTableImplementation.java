package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.terifan.raccoon.ArrayMap.InsertResult;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


public class BTreeTableImplementation extends TableImplementation
{
	static byte[] BLOCKPOINTER_PLACEHOLDER = new BlockPointer().setBlockType(BlockType.ILLEGAL).marshal(ByteArrayBuffer.alloc(BlockPointer.SIZE)).array();
	static int INDEX_SIZE = 600;
	static int LEAF_SIZE = 600;

	private boolean mWasEmptyInstance;
	private boolean mClosed;
	private int mModCount;
	private BTreeNode mRoot;

	private long mGenerationCounter;
	private long mNodeCounter;


	public BTreeTableImplementation(IManagedBlockDevice aBlockDevice, TransactionGroup aTransactionGroup, boolean aCommitChangesToBlockDevice, CompressionParam aCompressionParam, TableParam aTableParam, String aTableName)
	{
		super(aBlockDevice, aTransactionGroup, aCommitChangesToBlockDevice, aCompressionParam, aTableParam, aTableName);
	}


	@Override
	public void openOrCreateTable(byte[] aTableHeader)
	{
		if (aTableHeader == null)
		{
			Log.i("create table %s", mTableName);
			Log.inc();

			setupEmptyTable();

			mWasEmptyInstance = true;

			Log.dec();
		}
		else
		{
			Log.i("open table %s", mTableName);
			Log.inc();

			unmarshalHeader(aTableHeader);

			Log.dec();
		}
	}


	private byte[] marshalHeader()
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.alloc(BlockPointer.SIZE + 3);

		mBlockAccessor.getCompressionParam().marshal(buffer);

		mRoot.mBlockPointer.setBlockType(mRoot instanceof BTreeIndex ? BlockType.INDEX : BlockType.LEAF);
		mRoot.mBlockPointer.marshal(buffer);

		return buffer.trim().array();
	}


	private void unmarshalHeader(byte[] aTableHeader)
	{
		ByteArrayBuffer buffer = ByteArrayBuffer.wrap(aTableHeader);

		CompressionParam compressionParam = new CompressionParam();
		compressionParam.unmarshal(buffer);

		mBlockAccessor.setCompressionParam(compressionParam);

		BlockPointer bp = new BlockPointer();
		bp.unmarshal(buffer);

		mRoot = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(bp.getBlockLevel()) : new BTreeLeaf();
		mRoot.mBlockPointer = bp;
		mRoot.mMap = new ArrayMap(readBlock(bp));
		mRoot.mNodeId = nextNodeIndex();
	}


	@Override
	public boolean get(ArrayMapEntry aEntry)
	{
		checkOpen();

		mGenerationCounter++;

		aEntry.setKey(Arrays.copyOfRange(aEntry.getKey(), 2, aEntry.getKey().length));

		return mRoot.get(this, new MarshalledKey(aEntry.getKey()), aEntry);
	}


	@Override
	public ArrayMapEntry put(ArrayMapEntry aEntry)
	{
		checkOpen();

		mGenerationCounter++;

		aEntry.setKey(Arrays.copyOfRange(aEntry.getKey(), 2, aEntry.getKey().length));

		aEntry = new ArrayMapEntry(new MarshalledKey(aEntry.getKey()).array(), aEntry.getValue(), aEntry.getType());

		if (aEntry.getKey().length + aEntry.getValue().length > getEntrySizeLimit())
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.getKey().length + ", value: " + aEntry.getValue().length + ", maximum: " + getEntrySizeLimit());
		}

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> result = new Result<>();

		if (mRoot.put(this, new MarshalledKey(aEntry.getKey()), aEntry, result) == InsertResult.RESIZED)
		{
			if (mRoot instanceof BTreeLeaf)
			{
				mRoot = ((BTreeLeaf)mRoot).upgrade(this);
			}
			else
			{
				mRoot = ((BTreeIndex)mRoot).grow(this);
			}
		}

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return result.get();
	}


	@Override
	public ArrayMapEntry remove(ArrayMapEntry aEntry)
	{
		checkOpen();

		mGenerationCounter++;

		aEntry.setKey(Arrays.copyOfRange(aEntry.getKey(), 2, aEntry.getKey().length));

		int modCount = ++mModCount;
		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> oldEntry = new Result<>();

		mRoot.remove(this, new MarshalledKey(aEntry.getKey()), oldEntry);

		if (mRoot.mLevel > 1 && ((BTreeIndex)mRoot).mMap.size() == 1)
		{
			mRoot = ((BTreeIndex)mRoot).shrink(this);
		}
		if (mRoot.mLevel == 1 && ((BTreeIndex)mRoot).mMap.size() == 1)
		{
			mRoot = ((BTreeIndex)mRoot).downgrade(this);
		}

		Log.dec();
		assert mModCount == modCount : "concurrent modification";

		return oldEntry.get();
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		checkOpen();

		return new BTreeEntryIterator(mRoot);
	}


	@Override
	public ArrayList<ArrayMapEntry> list()
	{
		checkOpen();

		ArrayList<ArrayMapEntry> list = new ArrayList<>();
		iterator().forEachRemaining(list::add);

		return list;
	}


	@Override
	public boolean isChanged()
	{
		return mRoot.mModified;
	}


	@Override
	public byte[] commit(TransactionGroup mTransactionGroup, AtomicBoolean oChanged)
	{
		checkOpen();

		mCommitHistory.clear();

		try
		{
			if (mRoot.mModified)
			{
				int modCount = mModCount; // no increment
				Log.i("commit table");
				Log.inc();

				assert integrityCheck() == null : integrityCheck();

				mRoot.commit(this, mTransactionGroup);

				if (mCommitChangesToBlockDevice)
				{
					mBlockAccessor.getBlockDevice().commit();
				}

				mRoot.postCommit();

				Log.i("table commit finished; root block is %s", mRoot.mBlockPointer);

				Log.dec();
				assert mModCount == modCount : "concurrent modification";

				if (oChanged != null)
				{
					oChanged.set(true);
				}

				return marshalHeader();
			}

			if (mWasEmptyInstance && mCommitChangesToBlockDevice)
			{
				mBlockAccessor.getBlockDevice().commit();
			}

			if (oChanged != null)
			{
				oChanged.set(false);
			}

			return marshalHeader();
		}
		finally
		{
			mWasEmptyInstance = false;
		}
	}

	private HashSet<BlockPointer> mCommitHistory = new HashSet<>();

	void hasCommitted(BTreeNode aNode)
	{
		if (!mCommitHistory.add(aNode.mBlockPointer)) System.out.println(aNode);
	}


	@Override
	public long flush(TransactionGroup mTransactionGroup)
	{
		return 0l;
	}


	@Override
	public void rollback()
	{
		checkOpen();

		Log.i("rollback");

		if (mCommitChangesToBlockDevice)
		{
			mBlockAccessor.getBlockDevice().rollback();
		}

		if (mWasEmptyInstance)
		{
			Log.d("rollback empty");

			setupEmptyTable();
		}
		else
		{
			Log.d("rollback");

//			mDirectory = new Directory(mRootBlockPointer);
//			mNodes = new LeafNode[1 << mDirectory.getPrefixLength()];
		}
	}


	@Override
	public void removeAll(Consumer<ArrayMapEntry> aConsumer)
	{
		checkOpen();

		int modCount = ++mModCount;

//		new ExtendibleHashTableNodeIterator().forEachRemaining(node->
//		{
//			new ArrayMap(node.mMap.array()).forEach(aConsumer);
//
//			freeBlock(node.mBlockPointer);
//
//			node.mBlockPointer = null;
//		});
		setupEmptyTable();

		assert mModCount == modCount : "concurrent modification";
	}


	/**
	 * Clean-up resources
	 */
	@Override
	public void close()
	{
		if (!mClosed)
		{
			mClosed = true;
			mRoot = null;
			mBlockAccessor = null;
		}
	}


	@Override
	public int size()
	{
		checkOpen();

		Result<Integer> result = new Result<>(0);

//		new BTreeNodeIterator(mRoot).forEachRemaining(node -> result.set(result.get() + node.mMap.size()));
		visit(mRoot, node -> result.set(result.get() + node.mMap.size()));

		return result.get();
	}


	@Override
	public String integrityCheck()
	{
		Log.i("integrity check");

		return integrityCheck(mRoot);
	}


	private String integrityCheck(BTreeNode aNode)
	{
		if (aNode instanceof BTreeIndex)
		{
			BTreeIndex indexNode = (BTreeIndex)aNode;

			for (ArrayMapEntry entry : indexNode.mMap)
			{
				MarshalledKey key = new MarshalledKey(entry.getKey());

				BTreeNode node = indexNode.mChildNodes.get(key);

				if (node != null)
				{
//					String s = new String(key.marshall()).replaceAll("[^\\w]*", "").replace("'", "").replace("_", "");

					String result = integrityCheck(node);

					if (result != null)
					{
						return result;
					}
				}
			}
		}
		else
		{
			BTreeLeaf leafNode = (BTreeLeaf)aNode;

			for (ArrayMapEntry entry : leafNode.mMap)
			{
			}
		}

		return null;
	}


	@Override
	public int getEntrySizeLimit()
	{
		return LEAF_SIZE / 3;
	}


	private void checkOpen()
	{
		if (mClosed)
		{
			throw new IllegalStateException("Table is closed");
		}
	}


	@Override
	public void scan(ScanResult aScanResult)
	{
		aScanResult.tables++;

		scan(mRoot, aScanResult);
	}


	private void scan(BTreeNode aNode, ScanResult aScanResult)
	{
		if (aNode instanceof BTreeIndex)
		{
			BTreeIndex indexNode = (BTreeIndex)aNode;

			int fillRatio = indexNode.mMap.getUsedSpace() * 100 / INDEX_SIZE;
			aScanResult.log.append("{" + indexNode.mNodeId + ": " + fillRatio + "%" + "}");

			boolean first = true;
			aScanResult.log.append("'");
			for (ArrayMapEntry entry : indexNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(":");
				}
				first = false;
				MarshalledKey key = new MarshalledKey(entry.getKey());
				String s = new String(key.array()).replaceAll("[^\\w]*", "").replace("'", "").replace("_", "");
				aScanResult.log.append(s.isEmpty() ? "*" : s);
			}
			aScanResult.log.append("'");
			aScanResult.log.append(indexNode.mModified ? "#f00#f00#fff" : "#0f0");
			if (indexNode.mMap.size() == 1)
			{
				aScanResult.log.append("#f00");
				System.out.println("single index");
			}
			else if (fillRatio > 100)
			{
				aScanResult.log.append("#f80");
				System.out.println("fat index");
//				System.exit(0);
			}

			first = true;
			aScanResult.log.append("[");

			for (int i = 0, sz = indexNode.mMap.size(); i < sz; i++)
			{
				if (!first)
				{
					aScanResult.log.append(",");
				}
				first = false;

				BTreeNode child = indexNode.getNode(this, i);

				ArrayMapEntry entry = new ArrayMapEntry();
				indexNode.mMap.get(i, entry);
				indexNode.mChildNodes.put(new MarshalledKey(entry.getKey()), child);

				scan(child, aScanResult);
			}

			aScanResult.log.append("]");
		}
		else
		{
			BTreeLeaf node = (BTreeLeaf)aNode;
			int fillRatio = node.mMap.getUsedSpace() * 100 / LEAF_SIZE;

			aScanResult.log.append("{" + node.mNodeId + ": " + fillRatio + "%" + "}");
			aScanResult.log.append("[");
			boolean first = true;
			for (ArrayMapEntry entry : node.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(",");
				}
				first = false;
				aScanResult.log.append("'" + new String(entry.getKey()).replaceAll("[^\\w]*", "").replace("_", "") + "'");
			}
			aScanResult.log.append("]");
			aScanResult.log.append(node.mModified ? "#f00#f00#fff" : "#0f0");
			if (fillRatio > 100)
			{
				aScanResult.log.append("#f80");
				System.out.println("fat leaf");
				System.exit(0);
			}
		}
	}


	protected void freeBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer != null)
		{
			mBlockAccessor.freeBlock(aBlockPointer);
		}
	}


	protected byte[] readBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer.getBlockType() != BlockType.INDEX && aBlockPointer.getBlockType() != BlockType.LEAF)
		{
			throw new IllegalArgumentException("Attempt to read bad block: " + aBlockPointer);
		}

		return mBlockAccessor.readBlock(aBlockPointer);
	}


	protected BlockPointer writeBlock(TransactionGroup mTransactionGroup, byte[] aContent, int aLevel, BlockType aBlockType)
	{
		return mBlockAccessor.writeBlock(aContent, 0, aContent.length, mTransactionGroup.get(), aBlockType, 0).setBlockLevel(aLevel);
	}


	private void setupEmptyTable()
	{
		mRoot = new BTreeLeaf();
		mRoot.mMap = new ArrayMap(LEAF_SIZE);
		mRoot.mNodeId = nextNodeIndex();
	}


	synchronized long nextNodeIndex()
	{
		return ++mNodeCounter;
	}


	public long getGenerationCounter()
	{
		return mGenerationCounter;
	}


	private void visit(BTreeNode aNode, Consumer<BTreeLeaf> aConsumer)
	{
		if (aNode instanceof BTreeIndex)
		{
			BTreeIndex indexNode = (BTreeIndex)aNode;

			for (int i = 0, sz = indexNode.mMap.size(); i < sz; i++)
			{
				BTreeNode node = indexNode.getNode(this, i);

				ArrayMapEntry entry = new ArrayMapEntry();
				indexNode.mMap.get(i, entry);
				indexNode.mChildNodes.put(new MarshalledKey(entry.getKey()), node);

				visit(node, aConsumer);
			}
		}
		else
		{
			BTreeLeaf leafNode = (BTreeLeaf)aNode;
			aConsumer.accept(leafNode);
		}
	}
}
