package resources.entities;

import org.terifan.raccoon.Blob;
import org.terifan.raccoon.annotations.Id;


public class _BlobEntity
{
	@Id public int _id;
	public Blob blob;


	public _BlobEntity()
	{
	}


	public _BlobEntity(int aId)
	{
		_id = aId;
	}
}
