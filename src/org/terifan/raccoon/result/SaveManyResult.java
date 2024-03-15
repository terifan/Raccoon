package org.terifan.raccoon.result;

import java.util.ArrayList;
import org.terifan.raccoon.document.Document;


public class SaveManyResult
{
	public final ArrayList<Document> inserted = new ArrayList<>();
	public final ArrayList<Document> updated = new ArrayList<>();
}
