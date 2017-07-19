package org.terifan.raccoon;

import org.terifan.raccoon.TableInstance;
import org.terifan.raccoon.storage.BlockPointer;


public class ScanResult
{
	public int tables;
	public int records;
	public int indexBlocks;
	public int blobIndices;
	public int blobData;
	public int holes;

	public StringBuilder sb = new StringBuilder();


	public void enterNode(BlockPointer aBlockPointer)
	{
		sb.append("<table border=1><tr><td>"+aBlockPointer+"</td></tr><tr><td style='padding-left:40px;'>");
	}


	public void exitNode()
	{
		sb.append("</td></tr></table>");
	}


	public void enterLeaf(BlockPointer aBlockPointer, byte[] aBuffer)
	{
		sb.append("<table border=1><tr><td>"+aBlockPointer+"</td><td>");
	}


	public void entry()
	{
		sb.append("entry ");
	}


	public void exitLeaf()
	{
		sb.append("</td></tr></table>");
	}


	public void enterBlob()
	{
		sb.append("<table border=1><tr><td>blob</td></tr><tr><td>");
	}


	public void exitBlob()
	{
		sb.append("</td></tr></table>");
	}


	public void blobData()
	{
		sb.append("blob ");
	}


	public void enterTable(TableInstance aTable)
	{
		sb.append("<table border=1><tr><td>"+aTable.toString()+"</td></tr><tr><td>");
	}


	public void exitTable()
	{
		sb.append("</td></tr></table>");
	}


	@Override
	public String toString()
	{
		return "ScanResult{" + "tables=" + tables + ", records=" + records + ", indexBlocks=" + indexBlocks + ", blobIndices=" + blobIndices + ", blobData=" + blobData + ", holes=" + holes + ", sb=" + sb + '}';
	}
}
