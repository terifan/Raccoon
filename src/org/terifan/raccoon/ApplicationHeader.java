package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.List;
import org.terifan.bundle.Document;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.raccoon.storage.BlockPointer;


public class ApplicationHeader
{
	private Document mMetadata;
	private BlockPointer mBlockPointer;


	public ApplicationHeader()
	{
		mMetadata = new Document().putBundle("tables", new Document());
	}


	public void readFromDevice(IManagedBlockDevice aBlockDevice)
	{
		mBlockPointer = new BlockPointer().unmarshal(aBlockDevice.getApplicationHeader().getBinary("pointer"));

		byte[] buffer = new BlockAccessor(aBlockDevice, CompressionParam.BEST_COMPRESSION).readBlock(mBlockPointer);

		mMetadata = Document.unmarshal(buffer);
	}


	public void writeToDevice(IManagedBlockDevice aBlockDevice)
	{
		BlockAccessor blockAccessor = new BlockAccessor(aBlockDevice, CompressionParam.BEST_COMPRESSION);

		if (mBlockPointer != null)
		{
			blockAccessor.freeBlock(mBlockPointer);
		}

		byte[] buffer = mMetadata.marshal();

		mBlockPointer = blockAccessor.writeBlock(buffer, 0, buffer.length, 0, BlockType.HEADER, 0);

		aBlockDevice.getApplicationHeader().putBinary("pointer", mBlockPointer.marshal());
	}


	ArrayList<String> list()
	{
		return new ArrayList<>(mMetadata.getBundle("tables").keySet());
	}


	Document get(String aTableName)
	{
		return mMetadata.getBundle("tables").getBundle(aTableName);
	}


	void put(String aTableName, Document aTableHeader)
	{
		mMetadata.getBundle("tables").putBundle(aTableName, aTableHeader);
	}
}
