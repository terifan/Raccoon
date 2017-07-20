package resources.entities;

import org.terifan.raccoon.Key;


public abstract class _Base1K1C
{
	@Key public int _id;
	public int cls;
	public String name;


	public _Base1K1C()
	{
	}


	public _Base1K1C(int aCls, int aId, String aName)
	{
		this.cls = aCls;
		this._id = aId;
		this.name = aName;
	}
}
