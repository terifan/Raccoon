package org.terifan.raccoon;

import java.util.ArrayList;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.compressor.CompressorLevel;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.document.Document;


public class DatabaseRoot
{
	private final static String ROOT = "root";
	private final static String COLLECTIONS = "collections";

	private Document mMetadata;
	private BlockPointer mBlockPointer;


	public DatabaseRoot()
	{
		mMetadata = new Document()
			.put(COLLECTIONS, new Document());
	}


	public void readFromDevice(ManagedBlockDevice aBlockDevice)
	{
		mBlockPointer = new BlockPointer().unmarshal(aBlockDevice.getMetadata().getBinary(ROOT));

		byte[] buffer = new BlockAccessor(aBlockDevice).readBlock(mBlockPointer);

		mMetadata = new Document().fromByteArray(buffer);
	}


	public void writeToDevice(ManagedBlockDevice aBlockDevice)
	{
		BlockAccessor blockAccessor = new BlockAccessor(aBlockDevice);

		if (mBlockPointer != null)
		{
			blockAccessor.freeBlock(mBlockPointer);
		}

		byte[] buffer = mMetadata.toByteArray();

		mBlockPointer = blockAccessor.writeBlock(buffer, 0, buffer.length, BlockType.APPLICATION_HEADER, 0, CompressorLevel.DEFLATE_FAST);

		aBlockDevice.getMetadata().put(ROOT, mBlockPointer.marshal());
	}


	ArrayList<String> listCollections()
	{
		return new ArrayList<>(mMetadata.getDocument(COLLECTIONS).keySet());
	}


	Document getCollection(String aName)
	{
		return mMetadata.getDocument(COLLECTIONS).getDocument(aName);
	}


	void putCollection(String aName, Document aConfiguration)
	{
		mMetadata.getDocument(COLLECTIONS).put(aName, aConfiguration);
	}
}
