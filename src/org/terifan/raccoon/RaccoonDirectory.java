package org.terifan.raccoon;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobConsumer;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.document.Document;


public class RaccoonDirectory<K>
{
	private final RaccoonCollection mCollection;


	RaccoonDirectory(RaccoonCollection aCollection)
	{
		mCollection = aCollection;
	}


	RaccoonCollection getCollection()
	{
		return mCollection;
	}


	public LobByteChannel open(K aId, LobOpenOption aLobOpenOption) throws IOException
	{
		return open(aId, aLobOpenOption, null);
	}


	public LobByteChannel open(K aId, LobOpenOption aLobOpenOption, Document aOptions) throws IOException
	{
		LobByteChannel lob = tryOpen(aId, aLobOpenOption, aOptions);

		if (lob == null)
		{
			throw new LobNotFoundException(aId);
		}

		return lob;
	}


	public LobByteChannel tryOpen(K aId, LobOpenOption aLobOpenOption) throws IOException
	{
		return tryOpen(aId, aLobOpenOption, null);
	}


	public LobByteChannel tryOpen(K aId, LobOpenOption aLobOpenOption, Document aOptions) throws IOException
	{
		Document entry = new Document().put("_id", aId);

		if (!mCollection.tryGet(entry) && aLobOpenOption == LobOpenOption.READ)
		{
			return null;
		}

		return new LobByteChannel(mCollection.getBlockAccessor(), entry, aLobOpenOption, aOptions).setCloseAction(ch -> mCollection.save(entry));
	}


	public void delete(K aId) throws IOException
	{
		Document entry = new Document().put("_id", aId);

		if (!mCollection.tryGet(entry))
		{
			throw new FileNotFoundException(aId.toString());
		}

		LobByteChannel lob = tryOpen(aId, LobOpenOption.READ);
		if (lob != null)
		{
			lob.delete();
		}

		mCollection.delete(entry);
	}


	public void forEach(LobConsumer aConsumer) throws IOException
	{
//		mCollection.listAll().forEach(e -> aConsumer.accept(e.get("_id"), e.getDocument(LobByteChannel.IX_METADATA)));
		mCollection.listAll().forEach(e -> aConsumer.accept(e.get("_id"), e.getDocument("7")));
	}


	public List<K> list() throws IOException
	{
		return (List<K>)mCollection.listAll().stream().map(e->e.get("_id")).collect(Collectors.toList());
	}


	public long size()
	{
		return mCollection.size();
	}
}
