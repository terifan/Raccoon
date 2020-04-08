package org.terifan.raccoon.util;


public abstract class GraphTreeNode
{
	protected String mLabel;


	public GraphTreeNode()
	{
	}


	public GraphTreeNode(String aLabel)
	{
		mLabel = aLabel;
	}


	public String getLabel()
	{
		return mLabel;
	}
}