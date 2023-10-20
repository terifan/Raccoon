package org.terifan.raccoon;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobConsumer;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.ObjectId;


public class RaccoonDirectory
{
	private final RaccoonCollection mCollection;


	RaccoonDirectory(RaccoonCollection aCollection)
	{
		mCollection = aCollection;
	}


	public String getName()
	{
		return mCollection.getName();
	}


	public LobByteChannel open(ObjectId aId, LobOpenOption aLobOpenOption) throws IOException
	{
		LobByteChannel lob = tryOpen(aId, aLobOpenOption);

		if (lob == null)
		{
			throw new FileNotFoundException(aId.toString());
		}

		return lob;
	}


	public LobByteChannel tryOpen(ObjectId aId, LobOpenOption aLobOpenOption) throws IOException
	{
		Document entry = new Document().put("_id", aId);

		if (!mCollection.tryGet(entry) && aLobOpenOption == LobOpenOption.READ)
		{
			return null;
		}

		Runnable closeAction = () -> mCollection.save(entry);

		return new LobByteChannel(mCollection.getBlockAccessor(), entry, aLobOpenOption, closeAction);
	}


	public void delete(ObjectId aId) throws IOException
	{
		Document entry = new Document().put("_id", aId);

		if (!mCollection.tryGet(entry))
		{
			throw new FileNotFoundException(aId.toString());
		}

		new LobByteChannel(mCollection.getBlockAccessor(), entry, LobOpenOption.APPEND, null).delete();

		mCollection.delete(entry);
	}


	public void forEach(LobConsumer aConsumer) throws IOException
	{
		mCollection.listAll().forEach(e ->
		{
			aConsumer.accept(e.getObjectId("_id"), e.getDocument("metadata"));
		});
	}


	public long size()
	{
		return mCollection.size();
	}
}
