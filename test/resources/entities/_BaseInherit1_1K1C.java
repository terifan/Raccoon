package resources.entities;

import org.terifan.raccoon.Classifier;
import org.terifan.raccoon.Discriminator;
import org.terifan.raccoon.Key;


public class _BaseInherit1_1K1C extends _Base1K1C
{
	public double special;


	public _BaseInherit1_1K1C()
	{
	}


	public _BaseInherit1_1K1C(int aId, String aName, double aSpecial)
	{
		this.cls = 1;
		this._id = aId;
		this.name = aName;
		this.special = aSpecial;
	}
}
