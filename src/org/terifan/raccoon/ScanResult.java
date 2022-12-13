package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;


public class ScanResult
{
	public int tables;
	public int records;
	public int blobs;
	public int innerNodes;
	public int leafNodes;
	public int blobIndirectBlocks;
	public int blobDataBlocks;
	public long blobAllocatedSize;
	public long blobPhysicalSize;
	public long blobLogicalSize;
	public int holes;

	public StringBuilder log = new StringBuilder();


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


	public void enterTable(RaccoonCollection aTable)
	{
//		sb.append("<table border=1><tr><td>" + aTable.toString() + "</td></tr><tr><td>");
	}


	public void exitTable()
	{
//		sb.append("</td></tr></table>");
	}


	public void enterInnerNode(BlockPointer aBlockPointer)
	{
//		sb.append("<table border=1><tr><td>" + aBlockPointer + "</td></tr><tr><td style='padding-left:40px;'>");
	}


	public void exitInnerNode()
	{
//		sb.append("</td></tr></table>");
	}


	public void enterLeafNode(BlockPointer aBlockPointer, byte[] aBuffer)
	{
//		sb.append("<table border=1><tr><td>" + aBlockPointer + "</td><td>");
	}


	public void exitLeafNode()
	{
//		sb.append("</td></tr></table>");
	}


	public void enterBlob()
	{
//		sb.append("<table border=1><tr><td>" + aBlockPointer + "</td></tr><tr><td style='padding-left:40px;'>");
	}


	public void exitBlob()
	{
//		sb.append("</td></tr></table>");
	}


	public void record()
	{
//		sb.append("entry ");
	}


	public void blobIndirect(BlockPointer aBlockPointer)
	{
//		sb.append("<table border=1><tr><td>" + aBlockPointer + "</td></tr><tr><td style='padding-left:40px;'>");
	}


	public void blobData(BlockPointer aBlockPointer)
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
