package org.terifan.raccoon.browser;

import java.awt.BorderLayout;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Entry;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.Schema;
import org.terifan.raccoon.util.Log;


public class Browser
{
	public static void main(String ... args)
	{
		try
		{
			JFrame frame = new JFrame();

			final Database database = Database.open(new File("d:/test.rdb"), OpenOption.OPEN);

			final DefaultTableModel tableContentmodel = new DefaultTableModel(new String[]{"key","value"}, 0);
			final DefaultTableModel tableFormatModel = new DefaultTableModel(new String[]{"name","type"}, 0);
			List<Schema> schemas = database.getSchemas();
			JList schemaList = new JList(schemas.toArray());
			JTable tableFormat = new JTable(tableFormatModel);
			JTable tableContent = new JTable(tableContentmodel);
			JScrollPane tableListScroll = new JScrollPane(schemaList);
			JScrollPane tableContentScroll = new JScrollPane(tableContent);
			JScrollPane tableFormatScroll = new JScrollPane(tableFormat);
			JTabbedPane tabbedPane = new JTabbedPane();
			tabbedPane.add("Format", tableFormatScroll);
			tabbedPane.add("Content", tableContentScroll);

			schemaList.addListSelectionListener((e)->{
				try
				{
					if (!e.getValueIsAdjusting())
					{
						tableFormatModel.setNumRows(0);
						tableContentmodel.setNumRows(0);

						Schema schema = schemas.get(schemaList.getSelectedIndex());

						frame.setTitle(schema.getName());

						for (String[] fields : schema.getFields())
						{
							tableFormatModel.addRow(fields);
						}

						for (Iterator<Entry> it = schema.iteratorRaw(); it.hasNext(); )
						{
							Entry entry = it.next();
							tableContentmodel.addRow(new Object[]{new String(entry.getKey()), new String(entry.getValue())});
						}

						tableContent.invalidate();
						tableContent.validate();
						tableContent.repaint();
					}
				}
				catch (Throwable ex)
				{
					ex.printStackTrace(Log.out);
				}
			});

			frame.add(tableListScroll, BorderLayout.WEST);
			frame.add(tabbedPane, BorderLayout.CENTER);
			frame.setSize(1024, 768);
			frame.setLocationRelativeTo(null);
			frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
			frame.addWindowListener(new WindowAdapter()
			{
				@Override
				public void windowClosing(WindowEvent aE)
				{
					try
					{
						database.close();
					}
					catch (Exception e)
					{
						e.printStackTrace(Log.out);
					}
				}
			});
			frame.setVisible(true);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
