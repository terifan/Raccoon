package resources.entities;

import org.terifan.raccoon.Key;


public class _KeyValue1K
{
	@Key public String _name;
	public byte[] content;


	public _KeyValue1K()
	{
	}


	public _KeyValue1K(String aName)
	{
		_name = aName;
	}


	public _KeyValue1K(String aName, byte[] aContent)
	{
		_name = aName;
		content = aContent;
	}
}
