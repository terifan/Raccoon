package tests;

import org.terifan.bundle.Bundle;
import org.terifan.raccoon.Key;


public class _KeyValue1K1B
{
	@Key public String _name;
	public Bundle content;


	public _KeyValue1K1B()
	{
	}


	public _KeyValue1K1B(String aName)
	{
		_name = aName;
	}


	public _KeyValue1K1B(String aName, Bundle aContent)
	{
		_name = aName;
		content = aContent;
	}
}
