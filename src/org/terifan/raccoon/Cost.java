package org.terifan.raccoon;


public class Cost
{
	public int mEntityReturn;
	public int mReadBlockNode;
	public int mReadBlockLeaf;
	public int mWriteBlockNode;
	public int mWriteBlockLeaf;
	public int mMarshalEntity;
	public int mUnmarshalEntity;
	public int mUnmarshalValues;
	public int mUnmarshalKeys;
	public int mFreeBlock;
	public int mReadBlock;
	public int mWriteBlockBytes;
	public int mReadBlockBytes;
	public int mWriteBlock;
	public int mFreeBlockBytes;
	public int mEntityRemove;
	public int mTreeTraversal;
	public int mBlockSplit;
	public int mValuePut;
	public int mValueGet;


	@Override
	public String toString()
	{
		return "Cost{" + "mEntityReturn=" + mEntityReturn + ", mReadBlockNode=" + mReadBlockNode + ", mReadBlockLeaf=" + mReadBlockLeaf + ", mWriteBlockNode=" + mWriteBlockNode + ", mWriteBlockLeaf=" + mWriteBlockLeaf + ", mMarshalEntity=" + mMarshalEntity + ", mUnmarshalEntity=" + mUnmarshalEntity + ", mUnmarshalValues=" + mUnmarshalValues + ", mUnmarshalKeys=" + mUnmarshalKeys + ", mFreeBlock=" + mFreeBlock + ", mReadBlock=" + mReadBlock + ", mWriteBlockBytes=" + mWriteBlockBytes + ", mReadBlockBytes=" + mReadBlockBytes + ", mWriteBlock=" + mWriteBlock + ", mFreeBlockBytes=" + mFreeBlockBytes + ", mEntityRemove=" + mEntityRemove + ", mTreeTraversal=" + mTreeTraversal + ", mBlockSplit=" + mBlockSplit + ", mValuePut=" + mValuePut + ", mValueGet=" + mValueGet + '}';
	}
}
