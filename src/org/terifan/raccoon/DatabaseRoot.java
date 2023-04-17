package org.terifan.raccoon;

import org.terifan.raccoon.io.CompressionParam;
import java.util.ArrayList;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.storage.BlockPointer;


public class DatabaseRoot
{
	private Document mMetadata;
	private BlockPointer mBlockPointer;
	private long mTransactionId;


	public DatabaseRoot()
	{
		mMetadata = new Document()
			.put("collections", new Document());
	}


	public void readFromDevice(IManagedBlockDevice aBlockDevice)
	{
		mBlockPointer = new BlockPointer().unmarshal(aBlockDevice.getApplicationMetadata().getBinary("root"));

		byte[] buffer = new BlockAccessor(aBlockDevice, CompressionParam.BEST_COMPRESSION, true).readBlock(mBlockPointer);

		mMetadata = new Document().fromByteArray(buffer);
		mTransactionId = mBlockPointer.getTransactionId(); // TODO: use trans id from super block?
	}


	public void writeToDevice(IManagedBlockDevice aBlockDevice)
	{
		BlockAccessor blockAccessor = new BlockAccessor(aBlockDevice, CompressionParam.BEST_COMPRESSION, true);

		if (mBlockPointer != null)
		{
			blockAccessor.freeBlock(mBlockPointer);
		}

		byte[] buffer = mMetadata.toByteArray();

		mBlockPointer = blockAccessor.writeBlock(buffer, 0, buffer.length, BlockType.APPLICATION_HEADER);

		aBlockDevice.getApplicationMetadata().put("root", mBlockPointer.marshal());
	}


	ArrayList<String> listCollections()
	{
		return new ArrayList<>(mMetadata.getDocument("collections").keySet());
	}


	Document getCollection(String aName)
	{
		return mMetadata.getDocument("collections").getDocument(aName);
	}


	void putCollection(String aName, Document aConfiguration)
	{
		mMetadata.getDocument("collections").put(aName, aConfiguration);
	}


	public synchronized void nextTransaction()
	{
		mTransactionId++;
	}


	public synchronized long getTransactionId()
	{
		return mTransactionId;
	}
}
