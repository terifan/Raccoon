package org.terifan.raccoon;

import java.util.Objects;


class CommitLock
{
	private Exception mOwner;
	private Blob mBlob;


	public CommitLock(Exception aOwner)
	{
		mOwner = aOwner;
	}


	public void setBlob(Blob aBlob)
	{
		mBlob = aBlob;
	}


	@Override
	public int hashCode()
	{
		int hash = 3;
		hash = 19 * hash + Objects.hashCode(this.mOwner);
		hash = 19 * hash + Objects.hashCode(this.mBlob);
		return hash;
	}


	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
		{
			return true;
		}
		if (obj == null)
		{
			return false;
		}
		if (getClass() != obj.getClass())
		{
			return false;
		}
		final CommitLock other = (CommitLock)obj;
		if (!Objects.equals(this.mOwner, other.mOwner))
		{
			return false;
		}
		if (!Objects.equals(this.mBlob, other.mBlob))
		{
			return false;
		}
		return true;
	}
}
