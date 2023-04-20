package org.terifan.raccoon.util;


public interface FormattedToString
{
	void toFormattedString(FormattedOutput aOutput);

	default String toFormattedString()
	{
		FormattedOutput output = new FormattedOutput();
		toFormattedString(output);
		return output.toString();
	}
}
