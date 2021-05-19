package org.terifan.raccoon;

import java.util.Objects;


class CommitLock
{
	private Exception mOwner;
	private LobByteChannelImpl mBlob;


	public CommitLock()
	{
		mOwner = new Exception();
	}


	public void setBlob(LobByteChannelImpl aBlob)
	{
		mBlob = aBlob;
	}


	public String getOwner()
	{
		StringBuilder sb = new StringBuilder();

		try
		{
			boolean found = false;
			StackTraceElement prev = null;
			for (StackTraceElement el : mOwner.getStackTrace())
			{
				if (found || !el.getClassName().startsWith("org.terifan.raccoon."))
				{
					if (!found && prev != null)
					{
						sb.append(prev.getClassName() + "." + prev.getMethodName() + "(..)");
					}
					found = true;
					sb.append("\n\tat " + el.toString());
				}
				prev = el;
			}
		}
		catch (Throwable e)
		{
		}

		return sb.length() == 0 ? "unknown" : sb.toString();
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
