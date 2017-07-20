package resources.entities;

import org.terifan.raccoon.Classifier;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;


public class _BaseInherit2_1K1C extends _Base1K1C
{
	public String special;


	public _BaseInherit2_1K1C()
	{
	}


	public _BaseInherit2_1K1C(int aId, String aName, String aSpecial)
	{
		this.cls = 2;
		this._id = aId;
		this.name = aName;
		this.special = aSpecial;
	}
}
