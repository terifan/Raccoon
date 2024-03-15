package org.terifan.raccoon.btree;


public class _Scanner
{
//	public ScanResult scan(BTree aTree, ScanResult aScanResult)
//	{
//		aScanResult.tables++;
//		if (aTree.getRoot() instanceof BTreeInteriorNode v) scan(v, aScanResult);
//		if (aTree.getRoot() instanceof BTreeLeafNode v) scan(v, aScanResult);
//		return aScanResult;
//	}
//
//
//	protected void scan(BTreeInteriorNode aNode, ScanResult aScanResult)
//	{
//		int fillRatio = aNode.mMap.getUsedSpace() * 100 / aNode.mTree.getConfiguration().getNodeSize();
//		aScanResult.log.append("{" + (aNode.mBlockPointer == null ? "" : aNode.mBlockPointer.getBlockIndex0()) + ":" + fillRatio + "%" + "}");
//
//		boolean first = true;
//		aScanResult.log.append("'");
//		for (ArrayMapEntry entry : aNode)
//		{
//			if (!first)
//			{
//				aScanResult.log.append(":");
//			}
//			first = false;
//			String s = stringifyKey(entry.getKey());
//			aScanResult.log.append(s.isEmpty() ? "*" : s);
//		}
//		aScanResult.log.append("'");
//
////		if (aNode.mHighlight)
////		{
////			aScanResult.log.append("#a00#a00#fff");
////		}
////		else
//			if (aNode.size() == 1)
//		{
//			aScanResult.log.append("#000#ff0#000");
//		}
//		else if (fillRatio > 100)
//		{
////			aScanResult.log.append(aNode.mModified ? "#a00#a00#fff" : "#666#666#fff");
//			aScanResult.log.append("#666#666#fff");
//		}
//		else
//		{
////			aScanResult.log.append(aNode.mModified ? "#f00#f00#fff" : "#888#fff#000");
//			aScanResult.log.append("#888#fff#000");
//		}
//
//		first = true;
//		aScanResult.log.append("[");
//
//		for (int i = 0, sz = aNode.size(); i < sz; i++)
//		{
//			if (!first)
//			{
//				aScanResult.log.append(",");
//			}
//			first = false;
//
//			BTreeNode child = aNode.getNode(i);
//
//			if (child instanceof BTreeInteriorNode v)
//			{
//				scan(v, aScanResult);
//			}
//			else if (child instanceof BTreeLeafNode v)
//			{
//				scan(v, aScanResult);
//			}
//		}
//
//		aScanResult.log.append("]");
//	}
//
//
//	protected void scan(BTreeLeafNode aNode, ScanResult aScanResult)
//	{
//		int fillRatio = aNode.mMap.getUsedSpace() * 100 / aNode.mTree.getConfiguration().getLeafSize();
//
//		aScanResult.log.append("{" + (aNode.mBlockPointer == null ? "" : aNode.mBlockPointer.getBlockIndex0()) + ":" + fillRatio + "%" + "}");
//		aScanResult.log.append("[");
//
//		boolean first = true;
//
//		for (ArrayMapEntry entry : aNode.mMap)
//		{
//			if (!first)
//			{
//				aScanResult.log.append(",");
//			}
//			first = false;
//			if (entry.getType() == TYPE_EXTERNAL)
//			{
//				aScanResult.log.append("'" + stringifyKey(entry.getKey()) + "'");
//			}
//			else
//			{
//				aScanResult.log.append("'" + stringifyKey(entry.getKey()) + "'");
//			}
//		}
//
//		aScanResult.log.append("]");
//
////		if (aNode.mHighlight)
////		{
////			aScanResult.log.append("#a00#a00#fff");
////		}
////		else
//			if (fillRatio > 100)
//		{
////			aScanResult.log.append(aNode.mModified ? "#a00#a00#fff" : "#666#666#fff");
//			aScanResult.log.append("#666#666#fff");
//		}
//		else
//		{
////			aScanResult.log.append(aNode.mModified ? "#f00#f00#fff" : "#888#fff#000");
//			aScanResult.log.append("#888#fff#000");
//		}
//	}
//
//
//	protected String stringifyKey(ArrayMapKey aKey)
//	{
//		Object keyValue = aKey.get();
//
//		String value = "";
//
//		if (keyValue instanceof Array)
//		{
//			for (Object k : (Array)keyValue)
//			{
//				if (!value.isEmpty())
//				{
//					value += ",";
//				}
//				value += k.toString().replaceAll("[^\\w]*", "");
//			}
//		}
//		else
//		{
//			value += keyValue.toString().replaceAll("[^\\w]*", "");
//		}
//
//		return value;
//	}
}
