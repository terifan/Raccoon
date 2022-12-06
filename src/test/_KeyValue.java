package test;

import org.terifan.raccoon.annotations.Column;
import org.terifan.raccoon.annotations.Entity;
import org.terifan.raccoon.annotations.Id;


@Entity(name = "KeyValue", implementation = "btree")
public class _KeyValue
{
	@Id(name = "id", index = 0) String mKey;
	@Column(name = "value") String mValue;


	public _KeyValue()
	{
	}


	public _KeyValue(String aKey)
	{
		mKey = aKey;
	}


	public _KeyValue(String aKey, String aValue)
	{
		mKey = aKey;
		mValue = aValue;
	}


	@Override
	public String toString()
	{
		return "[" + mKey + "=" + mValue + "]";
	}
}
