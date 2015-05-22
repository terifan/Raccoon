package org.terifan.raccoon;


class _Blob1
{
	@Key String name;
	byte[] content;


	public _Blob1()
	{
	}


	public _Blob1(String aName)
	{
		name = aName;
	}


	public _Blob1(String aName, byte[] aContent)
	{
		name = aName;
		content = aContent;
	}
}
