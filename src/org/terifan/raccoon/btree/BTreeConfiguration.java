package org.terifan.raccoon.btree;

import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;
import org.terifan.raccoon.document.Document;


public class BTreeConfiguration extends Document
{
	private final static long serialVersionUID = 1L;

	private final static String ROOT = "root";
	private final static String SIZE_THRESHOLD = "st";
	private final static String NODE_SIZE = "ns";
	private final static String LEAF_SIZE = "ls";
	private final static String NODE_COMPRESSOR = "nc";
	private final static String LEAF_COMPRESSOR = "lc";

	private final int mSizeThreshold;
	private final int mNodeSize;
	private final int mLeafSize;
	private final int mNodeCompressor;
	private final int mLeafCompressor;


	public BTreeConfiguration()
	{
		this(new Document());
	}


	public BTreeConfiguration(Document aDocument)
	{
		putAll(aDocument);

		mSizeThreshold = computeIfAbsent(SIZE_THRESHOLD, e -> 1024);
		mNodeSize = computeIfAbsent(NODE_SIZE, e -> 4096);
		mLeafSize = computeIfAbsent(LEAF_SIZE, e -> 4096);
		mNodeCompressor = computeIfAbsent(NODE_COMPRESSOR, e -> CompressorAlgorithm.ZLE.ordinal());
		mLeafCompressor = computeIfAbsent(LEAF_COMPRESSOR, e -> CompressorAlgorithm.LZJB.ordinal());
	}


	BlockPointer getRoot()
	{
		return containsKey(ROOT) ? new BlockPointer().unmarshal(getBinary(ROOT)) : null;
	}


	void putRoot(BlockPointer aBlockPointer)
	{
		put(ROOT, aBlockPointer.marshal());
	}


	public int getSizeThreshold()
	{
		return mSizeThreshold;
	}


	int getNodeCompressor()
	{
		return mNodeCompressor;
	}


	int getLeafCompressor()
	{
		return mLeafCompressor;
	}


	int getLeafSize()
	{
		return mLeafSize;
	}


	int getNodeSize()
	{
		return mNodeSize;
	}


	@Override
	public String toString()
	{
		return "BTreeConfiguration{" + "mSizeThreshold=" + mSizeThreshold + ", mNodeSize=" + mNodeSize + ", mLeafSize=" + mLeafSize + ", mNodeCompressor=" + mNodeCompressor + ", mLeafCompressor=" + mLeafCompressor + ", root=" + getRoot() + '}';
	}
}
