package org.terifan.raccoon;

import java.util.ArrayList;
import org.terifan.bundle.Document;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.storage.BlockPointer;


public class ApplicationHeader
{
	private Document mMetadata;
	private BlockPointer mBlockPointer;
	private long mTransactionId;


	public ApplicationHeader()
	{
		mMetadata = new Document()
			.putBundle("collections", new Document());
	}


	public void readFromDevice(IManagedBlockDevice aBlockDevice)
	{
		mBlockPointer = new BlockPointer().unmarshal(aBlockDevice.getApplicationHeader().getBinary("root"));

		byte[] buffer = new BlockAccessor(aBlockDevice, CompressionParam.BEST_COMPRESSION).readBlock(mBlockPointer);

		mMetadata = Document.unmarshal(buffer);
		mTransactionId = mBlockPointer.getTransactionId(); // TODO: use trans id from super block?
	}


	public void writeToDevice(IManagedBlockDevice aBlockDevice)
	{
		BlockAccessor blockAccessor = new BlockAccessor(aBlockDevice, CompressionParam.BEST_COMPRESSION);

		if (mBlockPointer != null)
		{
			blockAccessor.freeBlock(mBlockPointer);
		}

		byte[] buffer = mMetadata.marshal();

		mBlockPointer = blockAccessor.writeBlock(buffer, 0, buffer.length, mTransactionId, BlockType.APPLICATION_HEADER);

		aBlockDevice.getApplicationHeader().putBinary("root", mBlockPointer.marshal());
	}


	ArrayList<String> list()
	{
		return new ArrayList<>(mMetadata.getBundle("collections").keySet());
	}


	Document get(String aCollectionName)
	{
		return mMetadata.getBundle("collections").getBundle(aCollectionName);
	}


	void put(String aCollectionName, Document aConfiguration)
	{
		mMetadata.getBundle("collections").putBundle(aCollectionName, aConfiguration);
	}


	public synchronized void nextTransaction()
	{
		mTransactionId++;
	}


	public synchronized long getTransaction()
	{
		return mTransactionId;
	}
}
