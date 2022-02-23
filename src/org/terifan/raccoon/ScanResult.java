package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;


public class ScanResult
{
	protected int tables;
	protected int records;
	protected int blobs;
	protected int innerNodes;
	protected int leafNodes;
	protected int blobIndirectBlocks;
	protected int blobDataBlocks;
	protected long blobAllocatedSize;
	protected long blobPhysicalSize;
	protected long blobLogicalSize;
	protected int holes;

	protected StringBuilder log = new StringBuilder();


	public ScanResult()
	{
	}


	public int getTables()
	{
		return tables;
	}


	public int getRecords()
	{
		return records;
	}


	public int getBlobs()
	{
		return blobs;
	}


	public int getInnerNodes()
	{
		return innerNodes;
	}


	public int getLeafNodes()
	{
		return leafNodes;
	}


	public int getBlobIndirectBlocks()
	{
		return blobIndirectBlocks;
	}


	public int getBlobDataBlocks()
	{
		return blobDataBlocks;
	}


	public long getBlobAllocatedSize()
	{
		return blobAllocatedSize;
	}


	public long getBlobPhysicalSize()
	{
		return blobPhysicalSize;
	}


	public long getBlobLogicalSize()
	{
		return blobLogicalSize;
	}


	public int getHoles()
	{
		return holes;
	}


	protected void enterTable(TableInstance aTable)
	{
//		sb.append("<table border=1><tr><td>" + aTable.toString() + "</td></tr><tr><td>");
	}


	protected void exitTable()
	{
//		sb.append("</td></tr></table>");
	}


	protected void enterInnerNode(BlockPointer aBlockPointer)
	{
//		sb.append("<table border=1><tr><td>" + aBlockPointer + "</td></tr><tr><td style='padding-left:40px;'>");
	}


	protected void exitInnerNode()
	{
//		sb.append("</td></tr></table>");
	}


	protected void enterLeafNode(BlockPointer aBlockPointer, byte[] aBuffer)
	{
//		sb.append("<table border=1><tr><td>" + aBlockPointer + "</td><td>");
	}


	protected void exitLeafNode()
	{
//		sb.append("</td></tr></table>");
	}


	protected void enterBlob()
	{
//		sb.append("<table border=1><tr><td>" + aBlockPointer + "</td></tr><tr><td style='padding-left:40px;'>");
	}


	protected void exitBlob()
	{
//		sb.append("</td></tr></table>");
	}


	protected void record()
	{
//		sb.append("entry ");
	}


	protected void blobIndirect(BlockPointer aBlockPointer)
	{
//		sb.append("<table border=1><tr><td>" + aBlockPointer + "</td></tr><tr><td style='padding-left:40px;'>");
	}


	protected void blobData(BlockPointer aBlockPointer)
	{
//		sb.append("<table border=1><tr><td>" + aBlockPointer + "</td></tr><tr><td style='padding-left:40px;'>");
	}


	public String getDescription()
	{
		return log.toString();
	}


	@Override
	public String toString()
	{
		return "{" + "\"tables\":" + tables + ", \"records\":" + records + ", \"blobs\":" + blobs + ", \"innerNodes\":" + innerNodes + ", \"leafNodes\":" + leafNodes + ", \"blobIndirectBlocks\":" + blobIndirectBlocks + ", \"blobDataBlocks\":" + blobDataBlocks + ", \"blobAllocatedSize\":" + blobAllocatedSize + ", \"blobPhysicalSize\":" + blobPhysicalSize + ", \"blobLogicalSize\":" + blobLogicalSize + ", \"holes\":" + holes + '}';
	}
}
