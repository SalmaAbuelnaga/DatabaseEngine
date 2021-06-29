import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;
import java.text.DateFormat;
import java.text.ParseException;
//import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.w3c.dom.html.HTMLAppletElement;
public class DBApp implements DBAppInterface {
	static String csvUrl = "src/main/resources/metadata.csv";
	String Config = "src/main/resources/DBApp.config";
	String tablesUrl = "src/main/resources/Tables.ser";
	String gridUrl = "src/main/resources/Indices.ser";
	Vector<Table> tables;
	static // Vector tables1=new Vector<Table>();
	BufferedWriter writer;
	static CSVPrinter csvPrinter;
	boolean exists;
	public DBApp() {
		init();
	}
	public void init() {
		// read metadata files found in resources,
		// same with tables - should be found it data
		// deserialize the existing tables file;
		tables = new Vector<Table>();
		String u = csvUrl;
		File l = new File(u);
		if (l.length() == 0) {
			try {
				writer = Files.newBufferedWriter(Paths.get(csvUrl));
				csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Table Name", "Column Name",
						"Column Type", "ClusteringKey", "Indexed", "min", "max"));
				csvPrinter.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// csvPrinter.close();
		}
		// checkfile();
		String url = tablesUrl;
		File f = new File(url);
		// System.out.println(f.length());
		if (f.length() != 0) {
			// System.out.println("ggg");
			tables = tableDe();
			tableSer();
		} else {
			// System.out.println("gg");
			// tables = new Vector<Table>();
		}
	}
	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
		// TODO Auto-generated method stub
		String clust = "";
		String max = "";
		String min = "";
		String name = "";
		String type = "";
		ArrayList<String> tnames = new ArrayList();
		// Vector<Table>tables = tableDe();
		for (int i = 0; i < tables.size(); i++) {
			tnames.add(tables.get(i).getName());
		}
		if (tnames.contains(tableName)) {
			throw new DBAppException();
		} else {
			Set<String> keys = colNameType.keySet();
			for (String key : keys) {
				name = key;
				type = colNameType.get(key);
				if (name.contentEquals(clusteringKey)) {
					clust = "true";
				} else {
					clust = "false";
				}
				Set<String> keys1 = colNameMin.keySet();
				Set<String> keys2 = colNameMax.keySet();
				for (String key1 : keys1) {
					if (key1.contentEquals(name)) {
						min = colNameMin.get(key1);
					}
				}
				for (String key2 : keys2) {
					if (key2.contentEquals(name)) {
						max = colNameMax.get(key2);
					}
				}
				String p = csvUrl;
				BufferedWriter writer;
				try {
					writer = Files.newBufferedWriter(Paths.get(p), StandardOpenOption.APPEND,
							StandardOpenOption.CREATE);
					csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
					csvPrinter.printRecord(tableName, name, type, clust, "false", min, max);
					csvPrinter.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// csvPrinter.close();
			}
			Table t = new Table(tableName, clusteringKey);
			tables.add(t);
			// System.out.println(tables.size());
			// for(int i=0;i<tables.size();i++) {
			// System.out.println(tables.get(i).getName());
			// }
			//
			// t
			// t.setPrimary(null);
		}
		tableSer();
		// String[]array = {clusteringKey};
		// createIndex(tableName,array);
	}
	public Vector<Table> tableDe() {
		String url = tablesUrl;
		FileInputStream fileIn;
		Vector<Table> v = new Vector();
		try {
			fileIn = new FileInputStream(url);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			v = (Vector<Table>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return v;
	}
	public void tableSer() {
		String url = tablesUrl;
		FileOutputStream fileOut;
		try {
			fileOut = new FileOutputStream(url);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(tables);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@SuppressWarnings("unchecked")
	public Vector<grid> checkI() {
		Vector<grid> v = new Vector<grid>();
		File f = new File(gridUrl);
		if (!(f.exists())) {
			try {
				f.createNewFile();
				return v;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			FileInputStream fileIn;
			try {
				fileIn = new FileInputStream(gridUrl);
				ObjectInputStream in = new ObjectInputStream(fileIn);
				v = (Vector<grid>) in.readObject();
				in.close();
				fileIn.close();
			} catch (IOException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return v;
		}
		return null;
	}
	public void gridSer(Vector<grid> v) {
		String url = gridUrl;
		FileOutputStream fileOut;
		try {
			fileOut = new FileOutputStream(url);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(v);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		Table t = checkT(tableName);
		String k = t.getKey();
		ArrayList l = readFile();
		Vector<String> x = new Vector<String>();// vector of column names from metadata
		for (int i = 0; i < l.size(); i++) {
			String[] a = (String[]) l.get(i); // a =l
			if (a[0].compareTo(tableName) == 0) {
				x.add(a[1]);
			}
		}
		for (int i = 0; i < columnNames.length; i++) {// comparing each key
			if (!x.contains(columnNames[i])) {// key by user is non existent
				throw new DBAppException();
			}
		}
		// check if index already exists //create new serializable file
		Vector<String> co = new Vector<String>();
		Collections.addAll(co, columnNames);
		Collections.sort(co);// vector of sorted column names
		boolean f = false;
		Vector<grid> in = t.getGrids();
		for (int i = 0; i < in.size(); i++) {
			if (in.get(i).getColNames().equals(co)) {
				f = true;
				break;
			}
		}
		if (f) {
			throw new DBAppException();// grid already exists
		}
		grid g = new grid(tableName, columnNames);
		g.setFilepath(tableName);
		updateMeta(tableName, co);
		in.add(g);
		t.setGrids(in);
		// Vector<column> c = t.getC(co);
		if (co.contains(k) && co.size() == 1) {
			t.setPrimary(g);// primary index
		}
		if (t.getPages() != null && t.getPages().size() != 0) {
			pop(t.getPages(), g, k, t.getName());
			//populate(t,g);
		}
		// g.serialize(null);
		tableSer();
	}
	public void pop(Vector<page> v, grid g, String cl, String tableName) throws DBAppException {
		Vector<String> st = g.getColNames();
		for (int i = 0; i < v.size(); i++) {
			page p = v.get(i);
			Vector<Hashtable<String, Object>> h = deserial(p);
			for (int j = 0; j < h.size(); j++) {
				Hashtable<String, Object> ht = getHash(h.get(j), st);
				ArrayList<bucket> b = g.find(ht);
				if (!(ht.containsKey(cl))) {
					ht.put(cl, h.get(j).get(cl));
				}
				bRecord br = new bRecord(p, ht);
				for (int k = 0; k < b.size(); k++) {
					b.get(k).insert(br, g, tableName);
				}
			}
			if (p.getOverflow() != null && p.getOverflow().size() > 0) {
				Vector<page> c = p.getOverflow();
				for (int j = 0; j < c.size(); j++) {
					page pg = c.get(j);
					Vector<Hashtable<String, Object>> vc = deserial(p);
					for (int k = 0; k < vc.size(); k++) {
						Hashtable ht = getHash(vc.get(j), st);
						ArrayList<bucket> b = g.find(ht);
						if (!(ht.containsKey(cl))) {
							ht.put(cl, vc.get(j).get(cl));
						}
						bRecord br = new bRecord(p, ht);
						for (int z = 0; z < b.size(); z++) {
							b.get(z).insert(br, g, tableName);
						}
					}
				}
			}
		}
	}
	/*public void populate(Table t,grid g) throws DBAppException{
		Vector <page>v=t.getPages();
		Vector <Hashtable> recs=new Vector <Hashtable>();
		for(int i=0;i<v.size();i++) {
		recs=deserial(v.get(i));
		for(int j=0;j<recs.size();j++) {
		Hashtable 	h=recs.get(i);
	ArrayList<bucket> a =	g.find(h);
	bRecord bb=new bRecord((page)v.get(i),h);
	a.get(0).insert(bb,t.getKey(),t.getName(),g);
	}
		}
	}*/
	public static void updateMeta(String tableName, Vector<String> colnames) {
		ArrayList l = readFile();
		for (int i = 0; i < l.size(); i++) {
			String[] a = (String[]) l.get(i); // a =l
			if (a[0].compareTo(tableName) == 0 && colnames.contains(a[1])) {
				a[4] = "True";
			}
			l.set(i, a);
		}
		try {
			FileWriter fw = new FileWriter(csvUrl, false);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			writer = Files.newBufferedWriter(Paths.get(csvUrl));
			csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Table Name", "Column Name", "Column Type",
					"ClusteringKey", "Indexed", "min", "max"));
			csvPrinter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 1; i < l.size(); i++) {
			String[] a = (String[]) l.get(i); // a =l
			tableName = a[0];
			String name = a[1];
			String type = a[2];
			String clust = a[3];
			String index = a[4];
			String min = a[5];
			String max = a[6];
			BufferedWriter writer;
			try {
				writer = Files.newBufferedWriter(Paths.get(csvUrl), StandardOpenOption.APPEND,
						StandardOpenOption.CREATE);
				csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
				csvPrinter.printRecord(tableName, name, type, clust, index, min, max);
				csvPrinter.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public int bSearch(String clust, Object o, Vector v) throws DBAppException {// vector of pages
		Vector f = new Vector();
		for (int i = 0; i < v.size(); i++) {
			page p = (page) v.get(i);
			f.add(p.getFirst().get(clust));
			f.add(p.getLast().get(clust));
		}
		int x = Collections.binarySearch(f, o);
		return x;
	}
	public int bSearchP(String clust, Object o, Vector<Hashtable> v) throws DBAppException {// vector of pages
		Vector f = new Vector();
		for (int i = 0; i < v.size(); i++) {
			// page p = (page) v.get(i);
			Hashtable h = v.get(i);
			// f.add(p.getFirst().get(clust));
			f.add(h.get(clust));
		}
		int x = Collections.binarySearch(f, o);// binarySearch 3ala kol el primary keys fel page, w return index
		if (x < 0) {// hashtable not found
			x = (x + 1);// return to true, do I need to know if its found?? yes, keep the negative, if
						// its negative,
		} // not found start from that index if its bigger than , law (>=) --> same bas
			// law positive, habda2 mel index else if
		return x;
	}
	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		// TODO Auto-generated method stub
		//tables = tableDe();
		ArrayList l = readFile();
		String[] a = new String[7];
		//String clust = "";
		Object k = new Object();
		Table y = checkT(tableName);
		String clust =y.getKey();
		//Table y = new Table();
		boolean f = false;
		int max = this.maxData();
		ArrayList indexes = new ArrayList();
		colNameValue = checkCol(tableName, colNameValue);
		for (int i = 0; i < l.size(); i++) {
			a = (String[]) l.get(i); // a =l
			if (a[0].compareTo(tableName) == 0) {
				indexes.add(i); // indexes of records in csv file containing the tablename
			}
		}
		if (indexes.size() == 0)
			throw new DBAppException(); // table not found
		else {// x=indexes.get(0);
			for (int i = 0; i < indexes.size(); i++) {
				String[] x = (String[]) l.get((int) indexes.get(i)); // records in csv file containing the table name
				if (x[3].equalsIgnoreCase("true")) {
					clust = x[1];
				}
			} // getting pk
		}
		if (colNameValue == null || colNameValue.isEmpty()) {
			throw new DBAppException();
		}
		//ArrayList l = readFile();
		Set<String> st = colNameValue.keySet();
		Vector vx = new Vector();// vector of column names from metadata
		for (int i = 0; i < l.size(); i++) {
			String[] sl = (String[]) l.get(i); // a =l
			if (sl[0].compareTo(tableName) == 0) {
				vx.add(sl[1]);
			}
		}
		for (String key2 : st) {// comparing each key
			if (!vx.contains(key2)) {// key by user is non existent
				throw new DBAppException();
			}
		}
		if (!colNameValue.containsKey(clust))
			throw new DBAppException(); // no pk given
		else {
			for (int i = 0; i < indexes.size(); i++) {
				String[] x = (String[]) l.get((int) indexes.get(i)); // records in csv file containing the table name
				if (colNameValue.containsKey(x[1])) {
					if (x[2].toLowerCase().contains(colNameValue.get(x[1]).getClass().getSimpleName().toLowerCase())) { // checking
																														// data
																														// types
					} else
						throw new DBAppException();
				}
			}
		}
		for (int i = 0; i < tables.size(); i++) {
			if (tables.get(i).name.equals(tableName)) {
				y = tables.get(i);
				break;
			}
		}
		
	
		if (y.getPages().size() == 0) { // creating the first page for the table
			Vector serpage = new Vector();
			serpage.add(colNameValue);
			page p = new page(y.getName(), 0);
			y.getPages().add(p);
			p.setNoOfRe(1);
			p.setPrev(null);
			String x = pageName(y);
		//	System.out.println(x);
			p.setFilepath(x);
			p.setFirst(colNameValue);
			p.setLast(colNameValue);
			p.setNoOfRe(1);
			serialize(serpage, p);
			k = colNameValue.get(clust);
			if(y.getGrids()!=null&&y.getGrids().size()!=0) {
				
				Vector<grid> v=y.getGrids();
				for(int i=0;i<v.size();i++) {
					bRecord b=new bRecord(p,getHash(colNameValue,v.get(i).getColName()));
					ArrayList v1=((grid) v.get(i)).find(getHash(colNameValue,v.get(i).getColName()));
					((bucket)v1.get(0)).insert(b, ((grid) v.get(i)),y.getName());
	  }
	}
		} else {
			int in = bSearch(clust, colNameValue.get(clust), y.getPages());
			Binsert(in, y, colNameValue);
		}
		
		
		
		tableSer();
}
	/*public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		// TODO Auto-generated method stub
		//tables = tableDe();
		//tables = tableDe();
				ArrayList l = readFile();
				String[] a = new String[7];
				//String clust = "";
				Object k = new Object();
				Table y = checkT(tableName);
				String clust =y.getKey();
				//Table y = new Table();
				boolean f = false;
				int max = this.maxData();
				ArrayList indexes = new ArrayList();
				colNameValue = checkCol(tableName, colNameValue);
				for (int i = 0; i < l.size(); i++) {
					a = (String[]) l.get(i); // a =l
					if (a[0].compareTo(tableName) == 0) {
						indexes.add(i); // indexes of records in csv file containing the tablename
					}
				}
				if (indexes.size() == 0)
					throw new DBAppException(); // table not found
				else {// x=indexes.get(0);
					for (int i = 0; i < indexes.size(); i++) {
						String[] x = (String[]) l.get((int) indexes.get(i)); // records in csv file containing the table name
						if (x[3].equalsIgnoreCase("true")) {
							clust = x[1];
						}
					} // getting pk
				}
				if (colNameValue == null || colNameValue.isEmpty()) {
					throw new DBAppException();
				}
				//ArrayList l = readFile();
				Set<String> st = colNameValue.keySet();
				Vector vx = new Vector();// vector of column names from metadata
				for (int i = 0; i < l.size(); i++) {
					String[] sl = (String[]) l.get(i); // a =l
					if (sl[0].compareTo(tableName) == 0) {
						vx.add(sl[1]);
					}
				}
				for (String key2 : st) {// comparing each key
					if (!vx.contains(key2)) {// key by user is non existent
						throw new DBAppException();
					}
				}
				if (!colNameValue.containsKey(clust))
					throw new DBAppException(); // no pk given
				else {
					for (int i = 0; i < indexes.size(); i++) {
						String[] x = (String[]) l.get((int) indexes.get(i)); // records in csv file containing the table name
						if (colNameValue.containsKey(x[1])) {
							if (x[2].toLowerCase().contains(colNameValue.get(x[1]).getClass().getSimpleName().toLowerCase())) { // checking
																																// data
																																// types
							} else
								throw new DBAppException();
						}
					}
				}
				for (int i = 0; i < tables.size(); i++) {
					if (tables.get(i).name.equals(tableName)) {
						y = tables.get(i);
						break;
					}
				}page p2=null;
		if(y.getGrids()!=null&&y.getGrids().size()!=0) {
			bRecord b=new bRecord(p2,colNameValue);
			Vector v=y.getGrids();
			for(int i=0;i<v.size();i++) {
				ArrayList v1=((grid) v.get(i)).find(colNameValue);
				((bucket)v1.get(0)).insert(b,(grid)v.get(i),y.getName());
			}if( y.getPages()!=null && y.getPages().size()!=0) {
			for(int i=0;i<v.size();i++) {
				if(((grid)v.get(i)).isPrimary(clust)) {
					grid g=(grid) v.get(i);
					p2=g.locatePage(b, clust, y, maxData());
					if(p2.getFilepath()==null ||p2.getFilepath().length()==0) {
						p2.setFilepath(pageName(y));
						p2.setFirst(colNameValue);
						p2.setLast(colNameValue);
						p2.setNoOfRe(p2.getNoOfRe()+1);
						Vector<Hashtable> v0=new Vector<Hashtable>();
						v0.add(colNameValue);
						serialize(v0,p2);
					}
					else {	Vector<Hashtable> v0=deserial(p2);
					v0.add(colNameValue);
					BSort(clust,v0);
					p2.setFirst(colNameValue);
					p2.setLast(colNameValue);
					p2.setNoOfRe(p2.getNoOfRe()+1);
					serialize(v0,p2);
						}
				/*	ArrayList v1=((grid) v.get(i)).find(colNameValue);
				p2 =((bucket)v1.get(0)).locatePage(b, clust,y,maxData());
				if(p2.getFilepath()==null ||p2.getFilepath().length()==0) {
					p2.setFilepath(pageName(y));
					p2.setFirst(colNameValue);
					p2.setLast(colNameValue);
					p2.setNoOfRe(p2.getNoOfRe()+1);
					Vector<Hashtable> v0=new Vector<Hashtable>();
					v0.add(colNameValue);
					serialize(v0,p2);
				}
				else {	Vector<Hashtable> v0=deserial(p2);
				v0.add(colNameValue);
				BSort(clust,v0);
				p2.setFirst(colNameValue);
				p2.setLast(colNameValue);
				p2.setNoOfRe(p2.getNoOfRe()+1);
				serialize(v0,p2);
					}*/
				//insert into the page
				/*break;}
				}
		}
		}
		if (y.getPages().size() == 0) { // creating the first page for the table
		Vector serpage = new Vector();
		serpage.add(colNameValue);
		page p = new page(y.getName(), 0);
		y.getPages().add(p);
		p.setNoOfRe(1);
		p.setPrev(null);
		String x = pageName(y);
		// System.out.println(x);
		p.setFilepath(x);
		p.setFirst(colNameValue);
		p.setLast(colNameValue);
		p.setNoOfRe(1);
		serialize(serpage, p);
		k = colNameValue.get(clust);
		} else {
		int in = bSearch(clust, colNameValue.get(clust), y.getPages());
		Binsert(in, y, colNameValue);
		}
		/*
		* for(int i=0;i<tables.size();i++) { for(int
		* j=0;i<tables.get(i).getPages().size();i++) { ObjectOutputStream out =
		* tables.get(i).getPages().get(j); tables1.add(p); } }
		*/
		//tableSer();
		//}*/
	
	 
	
	public int maxData() {
		Properties prop = new Properties();
		String fileName = Config;
		InputStream is;
		try {
			is = new FileInputStream(fileName);
			prop.load(is);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
	}
	public Vector<Hashtable> BSort(String pk, Vector<Hashtable> my_arr) {
		boolean swapped = true;
		int start = 0;
		int end = my_arr.size();
		// System.out.println(my_arr.get(end - 1).get(pk));
		while (swapped == true) {
			swapped = false;
			for (int i = start; i < end - 1; ++i) {
				Hashtable h1 = my_arr.get(i);
				Hashtable h2 = my_arr.get(i + 1);
				if (bigger(h1.get(pk), h2.get(pk))) {
					// Object temp = h1.get(pk);
					Hashtable temp = new Hashtable();
					Set<String> keys = h1.keySet();
					for (String key : keys) {
						temp.put(key, h1.get(key));
					}
					keys = h2.keySet();
					for (String key : keys) {
						h1.put(key, h2.get(key));
					}
					keys = temp.keySet();
					for (String key : keys) {
						h2.put(key, temp.get(key));
					}
					swapped = true;
				}
			}
			if (swapped == false)
				break;
			swapped = false;
			end = end - 1;
			for (int i = end - 1; i >= start; i--) {
				Hashtable h1 = my_arr.get(i);
				Hashtable h2 = my_arr.get(i + 1);
				if (bigger(h1.get(pk), h2.get(pk))) {
					Hashtable temp = new Hashtable();
					Set<String> keys = h1.keySet();
					for (String key : keys) {
						temp.put(key, h1.get(key));
					}
					keys = h2.keySet();
					for (String key : keys) {
						h1.put(key, h2.get(key));
					}
					keys = temp.keySet();
					for (String key : keys) {
						h2.put(key, temp.get(key));
					}
					swapped = true;
				}
			}
			start = start + 1;
		}
		return (my_arr);
	}
	public void Binsert(int in, Table t, Hashtable<String, Object> colNameValue) throws DBAppException {
		// tables = tableDe();
		page re = null;
		if (in >= 0)
			throw new DBAppException();// object already found, should throw DBAppException
		else {
			in = (in + 1) * -1;// index I'm supposed to insert in(page number)
		}
		int pagen;
		if (in % 2 == 1 || in == 0) {// if index is odd, value is within the range of 1 page, that page is half the
										// index
			// need to do search inside this page to make sure no duplicate clustering key
			pagen = in / 2;
			Vector v = t.getPages();
			String k = t.getKey();
			Object o;
			page p = (page) v.get(pagen);// System.out.println(p.getOverflow().size());
			// System.out.println(1<p.getOverflow().size());
			String file;
			Vector v1 = deserial(p);
			// System.out.println(p.getNoOfRe());
			// System.out.println(v1.size());
			int bs = binarySearch(v1, colNameValue.get(k), k);
			if (bs >= 0) {// value already exists
				serialize(v1, p);
				throw new DBAppException();
			} else {
				serialize(v1, p);
				if (p.getNoOfRe() < maxData()) {
					// p.setNoOfRe(p.getNoOfRe()+1);
					v1.add(colNameValue);
					v1 = BSort(k, v1);
					p.setFirst((Hashtable) v1.get(0));
					p.setLast((Hashtable) v1.get(v1.size() - 1));
					p.setNoOfRe(p.getNoOfRe() + 1);
					serialize(v1, p);
					re=p;
				}
				// deserialize page then add it
				// insert then sort
				else if (p.getOverflow() != null && !p.getOverflow().isEmpty()) {
					// check if overflow already exists
					boolean flag = false;
					Vector vv = deserial(p);
					Hashtable h = p.getLast();
					vv.remove(vv.size() - 1);
					vv.add(colNameValue);
					re=p;
					vv = BSort(k, vv);
					p.setFirst((Hashtable) vv.get(0));
					p.setLast((Hashtable) vv.get(vv.size() - 1));
					serialize(vv, p);
					for (int i = 0; i < p.getOverflow().size(); i++) {
						page ps = (page) p.getOverflow().get(i);// page 2, checking if it has space, insert, else
																// shifting
						if (ps.getNoOfRe() >= maxData()) {
							vv = deserial(ps);
							Hashtable h1 = (Hashtable) vv.get(vv.size() - 1);
							vv.removeElementAt(vv.size() - 1);
							vv.add(h);
							BSort(k, vv);
							ps.setFirst((Hashtable) vv.get(0));
							ps.setLast((Hashtable) vv.get(vv.size() - 1));
							serialize(vv, ps);
							h = h1;
						} else {
							flag = true;
							vv = deserial(ps);
							vv.add(h);
							vv = BSort(k, vv);
							ps.setFirst((Hashtable) vv.get(0));
							ps.setLast((Hashtable) vv.get(vv.size() - 1));
							ps.setNoOfRe(ps.getNoOfRe() + 1);
							serialize(vv, ps);
							break;
						}
					}
					if (!flag) {
						page pl = new page(t.getName(), p.getOverflow().size());
						Vector vpl = new Vector();
						vpl.add(h);
						p.getOverflow().add(pl);
						String f = pageName(t);
						pl.setFilepath(f);
						pl.setFirst(h);
						pl.setLast(h);
						pl.setNoOfRe(1);
						serialize(vpl, pl);
					}
				} else {// overflow is empty, check space in next page else, overflow
						// System.out.println("here");
					v1 = deserial(p); // elements in page p
					Hashtable ht = (Hashtable) v1.get(v1.size() - 1);// last element to be moved to next page
					v1.remove(ht);// removed last element
					v1.add(colNameValue); // inserted the new value
					re =p;
					v1 = BSort(k, v1);
					p.setFirst((Hashtable) v1.get(0));
					p.setLast((Hashtable) v1.get(v1.size() - 1));
					serialize(v1, p);
					/////
					if (pagen == v.size() - 1) {// last page, doesn't have an overflow
						page z = new page(t.getName(), v.size());
						Vector v2 = new Vector();
						v2.add(ht);
						z.setFirst(ht);
						z.setLast(ht);
						z.setNoOfRe(1);
						z.setFilepath(pageName(t));
						v.add(z);
						serialize(v2, z);
					} else {
						page z = (page) v.get(pagen + 1);
						if (z.getNoOfRe() < maxData()) {
							Vector v2 = deserial(z); // elements in page z
							v2.add(ht); // inserted the new value
							v2 = BSort(k, v2);
							z.setFirst((Hashtable) v2.get(0));
							z.setLast((Hashtable) v2.get(v2.size() - 1));
							z.setNoOfRe(z.getNoOfRe() + 1);
							serialize(v2, z);
						} else {// create overflow
							page op = new page(t.getName(), p.getPageno());
							Vector opv = new Vector();
							opv.add(ht);
							p.getOverflow().add(op);
							op.setNoOfRe(1);
							op.setFilepath(pageName(t));
							op.setFirst(ht);
							op.setLast(ht);
							serialize(opv, op);
						}
					}
				}
			}
		} // odd ends here
		else // if its even, the value is between 2 pages, check previous page first, then
				// the next for space
		// value cannot be a duplicate can insert right away
		{
			// check for last and first page
			pagen = (in - 1) / 2;
			Vector v = t.getPages();
			String k = t.getKey();
			Object o;
			page pg = (page) v.get(pagen);
			// System.out.println(pg.getOverflow()!=null);
			// System.out.println(pg.getNoOfRe());
			if (pg.getNoOfRe() < maxData()) {
				Vector vec = deserial(pg);
				// System.out.println(vec.size());
				vec.add(colNameValue);
				re=pg;
				vec = BSort(k, vec);
				pg.setFirst((Hashtable) vec.get(0));
				pg.setLast((Hashtable) vec.get(vec.size() - 1));
				pg.setNoOfRe(pg.getNoOfRe() + 1);
				serialize(vec, pg);
			} // add the case that the page was the last one -> create a new page not overflow
			else if (pg.getOverflow() != null && !pg.getOverflow().isEmpty()) {
				Hashtable h = colNameValue;
				boolean flag = false;
				// int bs = bSearch(k,colNameValue.get(k),pg.getOverflow());
				// didn't check if value exists???
				int ms = bSearch(k, colNameValue.get(k), pg.getOverflow());
				if (ms >= 0) {
					throw new DBAppException();
				} else {
					ms = (ms + 1) * -1;
					if (ms % 2 == 0 && ms != 0) {// between 2 overflow pages
						int m1 = ms / 2;
						page pa = (page) pg.getOverflow().get(m1 - 1);
						page pn = (page) pg.getOverflow().get(m1);
						Hashtable ht = colNameValue;
						if (pa.getNoOfRe() < maxData()) {
							Vector va = deserial(pa);
							va.add(colNameValue);
							re=pa;
							va = BSort(k, va);
							pa.setFirst((Hashtable) va.get(0));
							pa.setLast((Hashtable) va.get(va.size() - 1));
							pa.setNoOfRe(pa.getNoOfRe() + 1);
							serialize(va, pa);
						} else if (pn.getNoOfRe() < maxData()) {
							Vector va = deserial(pn);
							va.add(colNameValue);
							re=pa;
							va = BSort(k, va);
							pn.setFirst((Hashtable) va.get(0));
							pn.setLast((Hashtable) va.get(va.size() - 1));
							pn.setNoOfRe(pn.getNoOfRe() + 1);
							serialize(va, pn);
						} else {// shifting no space in both pages
								// page 1 already inserted user's value, hashtable is the last hashtable (i will
								// shift it to next page)
							boolean g = false;
							for (int i = m1; i < pg.getOverflow().size(); i++) {
								page ps = (page) pg.getOverflow().get(i);// page 2, checking if it has space, insert,
																			// else shifting
								if (ps.getNoOfRe() >= maxData()) {
									Vector vv = deserial(ps);
									Hashtable h1 = (Hashtable) vv.get(vv.size() - 1);
									vv.removeElementAt(vv.size() - 1);
									vv.add(ht);
									BSort(k, vv);
									ps.setFirst((Hashtable) vv.get(0));
									ps.setLast((Hashtable) vv.get(vv.size() - 1));
									serialize(vv, ps);
									ht = h1;
								} else {
									g = true;
									Vector vv = deserial(ps);
									vv.add(ht);
									BSort(k, vv);
									ps.setFirst((Hashtable) vv.get(0));
									ps.setLast((Hashtable) vv.get(vv.size() - 1));
									ps.setNoOfRe(ps.getNoOfRe() + 1);
									serialize(vv, ps);
									break;
								}
							}
							if (!g) {// last page already handled
								page pl = new page(t.getName(), pg.getOverflow().size());
								Vector vpl = new Vector();
								vpl.add(ht);
								pg.getOverflow().add(pl);
								String f = pageName(t);
								pl.setFilepath(f);
								pl.setFirst(ht);
								pl.setLast(ht);
								pl.setNoOfRe(1);
								serialize(vpl, pl);
							}
						}
					} else {// odd, within a page
						int m1 = ms / 2;
						page pm = (page) pg.getOverflow().get(m1);
						Vector v1 = deserial(pm);
						int bs = binarySearch(v1, colNameValue.get(k), k);
						if (bs >= 0) {// value already exists
							serialize(v1, pm);
							throw new DBAppException();
						} else {
							serialize(v1, pm);
							if (pm.getNoOfRe() < maxData()) {
								Vector va = deserial(pm);
								va.add(colNameValue);
								re=pm;
								va = BSort(k, va);
								pm.setFirst((Hashtable) va.get(0));
								pm.setLast((Hashtable) va.get(va.size() - 1));
								pm.setNoOfRe(pm.getNoOfRe() + 1);
								serialize(va, pm);
							} else {
								Hashtable ht = colNameValue;
								boolean g = false;
								for (int i = m1; i < pg.getOverflow().size(); i++) {
									page ps = (page) pg.getOverflow().get(i);// page 2, checking if it has space,
																				// insert, else shifting
									if (ps.getNoOfRe() >= maxData()) {
										Vector vv = deserial(ps);
										Hashtable h1 = (Hashtable) vv.get(vv.size() - 1);
										vv.removeElementAt(vv.size() - 1);
										vv.add(ht);
										BSort(k, vv);
										ps.setFirst((Hashtable) vv.get(0));
										ps.setLast((Hashtable) vv.get(vv.size() - 1));
										serialize(vv, ps);
										ht = h1;
									} else {
										g = true;
										Vector vv = deserial(ps);
										vv.add(ht);
										BSort(k, vv);
										ps.setFirst((Hashtable) vv.get(0));
										ps.setLast((Hashtable) vv.get(vv.size() - 1));
										ps.setNoOfRe(ps.getNoOfRe() + 1);
										serialize(vv, ps);
										break;
									}
								}
								if (!g) {
									page pl = new page(t.getName(), pg.getOverflow().size());
									Vector vpl = new Vector();
									vpl.add(ht);
									pg.getOverflow().add(pl);
									String f = pageName(t);
									pl.setFilepath(f);
									pl.setFirst(ht);
									pl.setLast(ht);
									pl.setNoOfRe(1);
									serialize(vpl, pl);
								}
							}
						}
					}
				}
			} else {// check the next page or create overflow
				if (pagen == v.size() - 1) {
					page p = new page(t.getName(), pg.getPageno() + 1);
					p.setFilepath(pageName(t));
					Vector pr = new Vector();
					v.add(p);
					re=p;
					pr.add(colNameValue);
					p.setFirst(colNameValue);
					p.setLast(colNameValue);
					p.setNoOfRe(1);
					serialize(pr, p);
				} else {
					pagen = pagen + 1;
					page p = (page) v.get(pagen);
					if (p.getNoOfRe() < maxData()) {// check the next page for space
						Vector vx = deserial(p);
						vx.add(colNameValue);
						re=p;
						vx = BSort(k, vx);
						p.setFirst((Hashtable) vx.get(0));
						p.setLast((Hashtable) vx.get(vx.size() - 1));
						serialize(vx, p);
						p.setNoOfRe(p.getNoOfRe() + 1);
					} else {// create overflow for
						page of = new page(t.getName(), pg.getPageno());
						Vector opv = new Vector();
						opv.add(colNameValue);
						re=of;
						pg.getOverflow().add(of);
						of.setFirst((Hashtable) colNameValue);
						of.setLast((Hashtable) colNameValue);
						of.setNoOfRe(1);
						of.setFilepath(pageName(t));
						serialize(opv, of);
					}
				}
			}
		}
		if(t.getGrids()!=null&&t.getGrids().size()!=0) {
			
			Vector<grid> v=t.getGrids();
			for(int i=0;i<v.size();i++) {
				bRecord b=new bRecord(re,getHash(colNameValue,v.get(i).getColName()));
				ArrayList v1=((grid) v.get(i)).find(getHash(colNameValue,v.get(i).getColName()));
				((bucket)v1.get(0)).insert(b, ((grid) v.get(i)),t.getName());
  }
}
		tableSer();
	}
	public String pageName(Table t) {
		t.setCounter(t.getCounter() + 1);
		String x = "src/main/resources/data/" + t.getName() + t.getCounter() + ".ser";
		return x;
	}
	public Vector deserial(page p1) {
		String url = p1.getFilepath();
		FileInputStream fileIn;
		Vector v = new Vector();
		try {
			fileIn = new FileInputStream(url);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			v = (Vector) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return v;
	}
	public void serialize(Vector v, page p1) {
		String url = p1.getFilepath();
		FileOutputStream fileOut;
		try {
			fileOut = new FileOutputStream(url);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(v);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public int checkfile() {
		String url = tablesUrl;
		File f = new File(url);
		if (!f.exists()) {
			try {
				f.createNewFile();
				tables = new Vector<Table>();
				return -1;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (f.length() > 0) {
			tables = tableDe();
			return 0;
		} else {
			tables = new Vector<Table>();
			return -1;
		}
	}
	public Table checkT(String t) throws DBAppException {
		int cf = checkfile();
		int x = -1;
		boolean fl = false;
		if (cf == -1) {
			throw new DBAppException();
		}
		for (int i = 0; i < tables.size(); i++) {
			if (tables.get(i).getName().toLowerCase().equals(t.toLowerCase())) {
				fl = true;
				x = i;
			}
		}
		tableSer();
		if (fl) {
			return tables.get(x);
		} else {
			throw new DBAppException();
		}
	}
	public boolean bigger(Object x, Object y) {
		if (x instanceof String) {
			if (((String) x).compareTo((String) (y)) > 0) {
				return true;
			} else {
				return false;
			}
		} else if (x instanceof Integer) {
			if ((Integer) x > (Integer) y) {
				return true;
			} else {
				return false;
			}
		}
		if (x instanceof Double) {
			if ((Double) x > (Double) y) {
				return true;
			}
		} else if (x instanceof Date) {
			// x instanceof java.util.Date
			if (((java.sql.Date) x).compareTo((java.sql.Date) (y)) > 0) {
				return true;
			} else
				return false;
		}
		return false;
	}
	public Date convtodate(Object o) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String d = sdf.format((Date) o);
		Date date = (Date) o;
		try {
			date = (Date) sdf.parse(d);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return date;
	}
	public int binarySearch(Vector<Hashtable> arr, Object key, String cl) {
		Hashtable h1 = (Hashtable) arr.get(0);
		Hashtable h2 = (Hashtable) arr.get(arr.size() - 1);
		int first = 0;
		int last = (arr.size() - 1);
		Object f = h1.get(cl);
		Object l = h2.get(cl);
		int mid = (0 + (arr.size() - 1)) / 2;
		while (first <= last) {
			if (bigger(key, arr.get(mid).get(cl))) {
				first = mid + 1;
			} else if (arr.get(mid).get(cl).equals(key)) {
				break;
			} else {
				last = mid - 1;
			}
			mid = (first + last) / 2;
		}
		if (first > last) {
			return -1;
		}
		return mid;
	}
	public page DelUp(Vector<page> t, Object k, String cl) throws DBAppException {
		int x = bSearch(cl, k, t);
		if (x >= 0) {
			page p = t.get(x / 2);
			return p;
		} else {
			int y = (x + 1) * -1;
			if (y % 2 == 1) {
				page p = t.get(y / 2);
				Vector<Hashtable> v = deserial(p);
				int z = binarySearch(v, k, cl);
				if (z < 0) {
					serialize(v, p);
					throw new DBAppException();
				} else {
					serialize(v, p);
					return p;
				}
			} else {
				if (y == 0 || y == (t.size() * 2)) {
					throw new DBAppException();
				} else {
					y = (y - 1) / 2;
					page p1 = t.get(y);
					if (p1.getOverflow() == null || p1.getOverflow().size() == 0) {
						throw new DBAppException();
					} else {
						int z = bSearch(cl, k, p1.getOverflow());
						if (z >= 0) {
							return (page) p1.getOverflow().get((z / 2));
						} else {
							int e = (z + 1) * -1;
							if (e % 2 == 0) {
								throw new DBAppException();
							} else {
								page o = (page) p1.getOverflow().get(e / 2);
								Vector vo = deserial(o);
								int l = binarySearch(vo, k, cl);
								if (l < 0) {
									serialize(vo, o);
									throw new DBAppException();
								} else {
									serialize(vo, o);
									return o;
								}
							}
						}
					}
				}
			}
		}
	}
	public page Select(Vector<page> t, Object k, String cl) throws DBAppException {// return page with location
		int x = bSearch(cl, k, t);
		if (x >= 0) {// found on borders of that
			page p = t.get(x / 2);
			return p;
		} else {
			int y = (x + 1) * -1;
			if (y % 2 == 1) {
				page p = t.get(y / 2);
				return p;
			} else {
				if (y == 0) {
					page p = t.get(0);
					return p;
				} else if (y == (t.size() * 2)) {
					page p = t.get(t.size() - 1);
					return p;
				} else {
					y = (y - 1) / 2;
					page p1 = t.get(y);
					if (p1.getOverflow() == null || p1.getOverflow().size() == 0) {
						return p1;
					} else {
						int z = bSearch(cl, k, p1.getOverflow());
						if (z >= 0) {
							return (page) p1.getOverflow().get((z / 2));
						} else {
							int e = (z + 1) * -1;
							if (e % 2 == 0) {
								page p = (page) p1.getOverflow().get(((e - 1) / 2));
								return p;
							} else {
								page o = (page) p1.getOverflow().get(e / 2);
								return o;
							}
						}
					}
				}
			}
		}
	}
	public Hashtable checkCol(String tableName, Hashtable<String, Object> h) throws DBAppException {
		if (h == null || h.isEmpty()) {
			throw new DBAppException();
		}
		ArrayList l = readFile();
		Set<String> st = h.keySet();
		Vector x = new Vector();// vector of column names from metadata
		for (int i = 0; i < l.size(); i++) {
			String[] a = (String[]) l.get(i); // a =l
			if (a[0].compareTo(tableName) == 0) {
				x.add(a[1]);
			}
		}
		for (String key2 : st) {// comparing each key
			if (!x.contains(key2)) {// key by user is non existent
				throw new DBAppException();
			}
		}
		String[] z;
		String[] a = new String[7];
		Object o = new Object();
		// String k = "";
		ArrayList indexes = new ArrayList();
		for (int i = 0; i < l.size(); i++) {
			a = (String[]) l.get(i); // a =l
			if (a[0].compareTo(tableName) == 0) {
				indexes.add(i);
			}
		}
		for (int i = 0; i < indexes.size(); i++) {
			String[] y = (String[]) l.get((int) indexes.get(i)); // records in csv file containing the table name
			if (h.containsKey(y[1])) {
				if (!(y[2].toLowerCase().contains(h.get(y[1]).getClass().getSimpleName().toLowerCase()))) { // checking
					throw new DBAppException();
				}
				String type = "";
				type = y[2].toLowerCase();
				// con(type, h.get(y[1]));
				if (y[2].toLowerCase().contains("date")) {// sql date kda kda
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					String d = sdf.format((h.get(y[1])));
					Date c = Date.valueOf(d);
					h.replace(y[1], c);
				}
				if (bigger(convertSt(y[5], type), h.get(y[1])) || bigger(h.get(y[1]), convertSt(y[6], type)))
					throw new DBAppException();
			}
		}
		return h;
	}
	public int binarySearchU(Vector<bRecord> arr, Object key, String cl) {
		bRecord h1 = arr.get(0);
		bRecord h2 = arr.get(arr.size() - 1);
		int first = 0;
		int last = (arr.size() - 1);
		Object f = h1.getRecord().get(cl);
		Object l = h2.getRecord().get(cl);
		int mid = (0 + (arr.size() - 1)) / 2;
		while (first <= last) {
			if (bigger(key, arr.get(mid).getRecord().get(cl))) {
				first = mid + 1;
			} else if (arr.get(mid).getRecord().get(cl).equals(key)) {
				break;
			} else {
				last = mid - 1;
			}
			mid = (first + last) / 2;
		}
		if (first > last) {
			return -1;
		}
		return mid;
	}
	public bucket DelUpU(Vector<bucket> t, Object k, String cl) throws DBAppException {
		int x = bSearchU(cl, k, t);
		if (x >= 0) {
			bucket p = t.get(x / 2);
			return p;
		} else {
			int y = (x + 1) * -1;
			if (y % 2 == 1) {
				bucket p = t.get(y / 2);
				Vector<bRecord> v = p.deserialize();
				int z = binarySearchU(v, k, cl);
				if (z < 0) {
					// serialize(v, p);
					throw new DBAppException();
				} else {
					// serialize(v, p);
					return p;
				}
			} else {
				if (y == 0 || y == (t.size() * 2)) {
					throw new DBAppException();
				} else {
					y = (y - 1) / 2;
					bucket p1 = t.get(y);
					if (p1.getOverflow() == null || p1.getOverflow().size() == 0) {
						throw new DBAppException();
					} else {
						int z = bSearchU(cl, k, p1.getOverflow());
						if (z >= 0) {
							return p1.getOverflow().get((z / 2));
						} else {
							int e = (z + 1) * -1;
							if (e % 2 == 0) {
								throw new DBAppException();
							} else {
								bucket o = p1.getOverflow().get(e / 2);
								Vector<bRecord> vo = o.deserialize();
								int l = binarySearchU(vo, k, cl);
								if (l < 0) {
									// serialize(vo, o);
									throw new DBAppException();
								} else {
									// serialize(vo, o);
									return o;
								}
							}
						}
					}
				}
			}
		}
	}
	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {
		Table t = checkT(tableName);
		if (t.getPages().size() == 0) {
			throw new DBAppException();
		}
		Vector v = t.getPages();
		String cl = t.getKey();
		ArrayList al = readFile();
		columnNameValue = checkCol(tableName, columnNameValue);
		String[] x;
		String type = "";
		boolean indexed = false;
		for (int i = 0; i < al.size(); i++) {// converting to correct type
			x = (String[]) al.get(i);
			if (x[3].equalsIgnoreCase("true") && x[0].equals(t.getName())) {
				type = x[2];
				if (x[4].equalsIgnoreCase("true"))
					indexed = true;
				break;
			}
		}
		int l;
		int ind;
		Set<String> st = columnNameValue.keySet();
		if (st.contains(cl)) {
			throw new DBAppException();
		}
		if (!indexed) {
			page p = DelUp(v, convertSt(clusteringKeyValue, type), cl);
			Vector vo = deserial(p);
			int m = binarySearch(vo, convertSt(clusteringKeyValue, type), cl);
			Hashtable h = (Hashtable) vo.get(m);
			for (String key2 : st) {// comparing each key
				if (h.containsKey(key2))
					h.replace(key2, columnNameValue.get(key2));
				else
					h.put(key2, columnNameValue.get(key2));
			}
			p.setFirst((Hashtable) vo.get(0));
			p.setLast((Hashtable) vo.get(vo.size() - 1));
			serialize(vo, p);
			tableSer();
		} else {
			grid g = t.getPrimary();
			boolean f = false;
			Hashtable<String, Object> h = new Hashtable<String, Object>();
			Hashtable<String, Object> h2 = new Hashtable<String, Object>();
			h.put(cl, convertSt(clusteringKeyValue, type));
			ArrayList<bucket> b = g.find(h);// check the serialization and deserialization of list of buckets
			if (b.size() == 0)
				throw new DBAppException();
			for(int i=0;i<b.size();i++) {
				page p=null;
				bucket c = b.get(i);
				Vector<bRecord> r =c.deserial();
				for(int j=0;j<r.size();i++) {
					Object val = r.get(j).getRecord().get(cl);
					if(val.equals( convertSt(clusteringKeyValue, type))) {
						p = r.get(j).getPage();
						Vector<Hashtable> ht = deserial(p);
						for(int k=0;k<ht.size();k++) {
							if (ht.get(k).get(cl).equals(val)) {
								h = ht.get(k);
								for (String key : st) {
									ht.get(k).replace(key, columnNameValue.get(key));
								}
								h2 = ht.get(k);
								f=true;
								updateGrids(h, h2, t, st,p);
								break;
							}
						}
						serialize(ht,p);
						if(f)
							break;
					}
				}
				if(f)
					break;
			}
			/*Vector<bucket> z = new Vector<bucket>();
			z.addAll(b);
			// z.addAll(b.get(0).getOverflow());//looping through bucket and its overflow
			bucket d = g.find()
			if (d.isEmpty())
				throw new DBAppException();
			Vector<bRecord> c = d.deserialize();
			int u = binarySearchU(c, convertSt(clusteringKeyValue, type), cl);
			page p = c.get(u).getPage();
			Vector<Hashtable> ht = deserial(p);
			int m = binarySearch(ht, convertSt(clusteringKeyValue, type), cl);
			if (m < 0)
				throw new DBAppException();
			h = ht.get(m);
			for (String key2 : st) {// updating the hashtable
				ht.get(m).replace(key2, columnNameValue.get(key2));
			}
			h2 = ht.get(m);
			serialize(ht, p);// done with updating the values in the pages,next grids
			// f = true;
			updateGrids(h, h2, t, st,p);
			// break;
			// binary search in brecord to get i
			// should return only 1 bucket with its overflow
			  for (int j = 0; j < b.size(); j++) { 
				  page p;
				  if(b.get(i).getRecord().get(cl).equals(convertSt(clusteringKeyValue, type)))
			  {// record found won't // be repeated again // can stop 
					  p = b.get(i).getPage(); 
			  Vector<Hashtable> vc = deserial(p);
			  int m =binarySearch(vc, convertSt(clusteringKeyValue, type), cl); 
			  h = vc.get(m);//
			  old hashtable values for (String key2 : st) {// updating the hashtable
			  vc.get(m).replace(key2, columnNameValue.get(key2)); } h2 = vc.get(m);
			  serialize(vc, p); f = true; 
			  updateGrids(h, h2, t); break; 
			  }
				  } 
			  if (f) 
				  break; }
			 */
			tableSer();
		}
	}
	public int bSearchU(String clust, Object o, Vector<bucket> v) throws DBAppException {// vector of pages
		Vector f = new Vector();
		for (int i = 0; i < v.size(); i++) {
			// page p = (page) v.get(i);
			bucket b = v.get(i);
			f.add(b.getFirst().getRecord().get(clust));
			f.add(b.getLast().getRecord().get(clust));
		}
		int x = Collections.binarySearch(f, o);
		return x;
	}
	public void updateGrids(Hashtable<String, Object> old, Hashtable<String, Object> n, Table t, Set<String> st,page p) throws DBAppException {
		Vector<Hashtable<String, Object>> c = new Vector<Hashtable<String, Object>>();
		c.add(old);
		Vector<grid> s = getcols(t.getGrids(), st);
		updateGridDel(c, t.getKey(), s);// removes all occurences of record from table, choose only the grids
		updateGridinsert(n,t.getKey(),s,p,t.getName());			// that contain changed columns
		// insert hash n into all grids
	}
	public Vector<grid> getcols(Vector<grid> g, Set<String> st) {
		Vector<grid> r = new Vector<grid>();
		for (int i = 0; i < g.size(); i++) {
			grid z = g.get(i);
			Vector<String> s = z.getColNames();
			for (String key2 : st) {// updating the hashtable
				if (s.contains(key2)) {
					r.add(z);
					break;
				}
			}
		}
		return r;
	}
	public grid getPriIndex(Table t) {
		Hashtable<Integer, grid> c = new Hashtable<Integer, grid>();
		int min = 0;
		for (int i = 0; i < t.getGrids().size(); i++) {
			grid g = t.getGrids().get(i);
			if (g.getColNames().get(i).contains(t.getKey())) {
				int z = g.getColNames().size();
				if (z < min)
					min = z;
				c.put(z, g);
			}
		}
		if (c.containsKey(1)) {
			return c.get(1);
		} else
			return c.get(min);
	}
	public Object convertSt(String st, String type) {
		// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		type = type.toLowerCase();
		if (type.contains("Integer".toLowerCase()))// changed from equals java.util.lang to contains each
			return Integer.parseInt(st);
		else if (type.contains("Double".toLowerCase()))
			return Double.parseDouble(st);
		else if (type.contains("Date".toLowerCase())) {
			// if(type.contains("java.sql.date")) {
			return Date.valueOf(st);
		}
		return st;
	}
	@Override
	public void deleteFromTableH(String tableName, Hashtable<String, Object> columnNameValue, Table t)
			throws DBAppException {
		// Table t = checkT(tableName);
		// if (t.getPages() == null || t.getPages().size() == 0) {
		// throw new DBAppException();
		// } // else
		// columnNameValue = checkCol(tableName, columnNameValue);
		String k = t.getKey();
		Vector v = t.getPages();
		int s = columnNameValue.size();
		Set<String> st = columnNameValue.keySet();
		ArrayList al = readFile();
		String[] a = new String[7];
		Object o = new Object();
		boolean f = true;
		int max = this.maxData();
		boolean flag;
		int l;
		if (st.contains(k))// law dal clustering key use binary search
		{
			page p = DelUp(v, columnNameValue.get(k), k);
			Vector<Hashtable> r = deserial(p);
			int m = binarySearch(r, columnNameValue.get(k), k);
			Hashtable ht = r.get(m);
			for (String key2 : st) {
				String sg = key2;
				if (!columnNameValue.get(key2).equals(ht.get(key2))) {
					f = false;
					break;
				}
			}
			if (f) {
				r.removeElementAt(m);
				if (r == null || r.size() == 0) {// law page is empty after removing
					l = v.indexOf(p);
					v.remove(p);
					String g = p.getFilepath();
					try {
						Files.deleteIfExists(Paths.get(g));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (p.getOverflow() != null && p.getOverflow().size() > 0) {
						page x = (page) p.getOverflow().get(0);
						Vector sof = p.getOverflow();
						sof.remove(x);
						x.setOverflow(sof);
						v.insertElementAt(x, l);
					}
				} else {
					p.setFirst((Hashtable) r.get(0));
					p.setLast((Hashtable) (r.get(r.size() - 1)));
					p.setNoOfRe(p.getNoOfRe() - 1);
					serialize(r, p);
				}
			} else {
				serialize(r, p);
				throw new DBAppException();
			}
		} else // da msh clustering, use linear, delete every instance of that name
		{
			// deserialize all starting from first page
			flag = false;//
			boolean ff = true;
			Vector<page> pg = new Vector();
			for (int i = 0; i < v.size(); i++)// for loop on the whole table, every page
			{
				page p = (page) v.get(i);
				Vector v2 = deserial(p); // vector of hash tables in that page
				if (p.getOverflow() != null && !p.getOverflow().isEmpty()) {
					for (int ii = 0; ii < p.getOverflow().size(); ii++)// for loop on the whole table, every page
					{
						boolean f1 = true;
						page pp = (page) p.getOverflow().get(ii);
						Vector vo = deserial(pp); // vector of hash tables in that page
						for (int j = 0; j < vo.size(); j++) {// for loop every hashtable in 1 page
							boolean fo = true;
							Hashtable h = (Hashtable) vo.get(j);
							for (String key2 : st) {// comparing each key
								String sg = key2;
								if (!columnNameValue.get(key2).equals(h.get(key2))) {
									fo = false;// records are matching
									break;
								}
							}
							if (fo) {
								vo.remove(j);
								flag = true;// check law element found and removed
								if (vo == null || vo.isEmpty()) {
									p.getOverflow().removeElementAt(ii);
									String g = pp.getFilepath();
									try {
										Files.deleteIfExists(Paths.get(g));
									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
									f1 = false;// so I don't serialize a deleted page
									ii--;
									break;
									// f = true;
								} else {
									pp.setNoOfRe(pp.getNoOfRe() - 1);
									pp.setFirst((Hashtable) vo.get(0));
									pp.setLast((Hashtable) vo.get(vo.size() - 1));
									j--;
								}
							}
						}
						if (f1)
							serialize(vo, pp);
					}
				}
				for (int j = 0; j < v2.size(); j++) {// for loop every hashtable in 1 page
					boolean fl = true;
					Hashtable h = (Hashtable) v2.get(j);
					for (String key2 : st) {// comparing each key
						String sg = key2;
						if (!columnNameValue.get(key2).equals(h.get(key2))) {
							fl = false;
							break;
						}
					}
					if (fl) {
						v2.remove(j);
						flag = true;
						if (v2 == null || v2.isEmpty()) {
							v.remove(p);
							String g = p.getFilepath();
							try {
								Files.deleteIfExists(Paths.get(g));
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							if (p.getOverflow() != null && p.getOverflow().size() > 0) {
								page x = (page) p.getOverflow().get(0);
								Vector sof = p.getOverflow();
								sof.remove(x);
								x.setOverflow(sof);
								v.insertElementAt(x, i);
								i++;
							}
							i--;
							ff = false;
							break;// can find more than 1 row, stop looping on empty page
						} else {
							p.setNoOfRe(p.getNoOfRe() - 1);
							// serialize(v2, p);
							p.setFirst((Hashtable) v2.get(0));
							p.setLast((Hashtable) v2.get(v2.size() - 1));
							j--;
						}
					}
				}
				if (ff)
					serialize(v2, p);
			}
			if (!flag) {
				throw new DBAppException();
			}
		}
		tableSer();
	}
	public grid getI(Table t, Hashtable<String, Object> columnNameValue) {
		Vector<grid> g = t.getGrids();
		Set<String> st = columnNameValue.keySet();
		int c = 0;
		int ind = -1;
		for (int i = 0; i < g.size(); i++) {
			int x = 0;
			grid r = g.get(i);
			Vector<String> s = r.getColNames();
			for (int j = 0; j < s.size(); i++) {
				if (st.contains(s.get(i))) {
					x++;
				}
			}
			if (x > c) {
				ind = i;// grid index in vector
				c = x;
			}
		}
		if (ind > -1 && c > 1) {
			return g.get(ind);
		} else
			return null;
	}
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		Table t = checkT(tableName);
		if (t.getPages() == null || t.getPages().size() == 0) {
			throw new DBAppException();
		} // else
		columnNameValue = checkCol(tableName, columnNameValue);
		String k = t.getKey();
		// Vector v = t.getPages();
		// int s = columnNameValue.size();
		Set<String> st = columnNameValue.keySet();
		// ArrayList al = readFile();
		// String[] a = new String[7];
		// Object o = new Object();
		// boolean f = true;
		// int max = this.maxData();
		// boolean flag;
		// int l;
		grid g = getI(t, columnNameValue);
		if (g != null) {
			boolean f;
			if (st.contains(k)) {
				f = true;
			}
			Vector<String> colN = g.getColNames();// names of index column names,
			Vector<Hashtable> keys = new Vector<Hashtable>();// deleted hashtables
			Vector<Hashtable<String, Object>> pgs = new Vector<Hashtable<String, Object>>();
			Hashtable a = getHash(columnNameValue, colN);
			ArrayList<bucket> gb = g.find(a);// indices of buckets from grid bucket array
			// bucket[]bc = g.getBuckets();//array of buckets
			for (int i = 0; i < gb.size(); i++) {// list of all buckets
				bucket buc = gb.get(i);
				if (buc.isEmpty())
					break;
				Vector<bRecord> br = buc.deserialize();
				for (int j = 0; j < br.size(); j++) {// looping in records in the bucket
					if (comp(columnNameValue, br.get(j).getRecord())) {
						// once deleted from page, remove from index
						Hashtable<String, Object> h = delete(br.get(j).getRecord().get(k), br.get(j).getPage(), k, t);
						keys.add(h);
						//buc.delete(br.get(j), k);
						br.remove(j);
						j--;
					}
				}
				// once done with one bucket, finish the overflow
				if (buc.getOverflow() != null && buc.getOverflow().size() > 0) {
					for (int z = 0; z < buc.getOverflow().size(); z++) {// list of all buckets
						bucket buc1 = buc.getOverflow().get(z);
						Vector<bRecord> br1 = buc1.deserialize();
						for (int j = 0; j < br1.size(); j++) {// looping in records in the bucket
							if (comp(columnNameValue, br1.get(j).getRecord())) {
								delete(br1.get(j).getRecord().get(k), br1.get(j).getPage(), k, t);// once deleted from
																									// page, remove from
								//buc1.delete(br1.get(j), k);															// index
								br1.remove(j);
								j--;
							}
						}
						if (br1.size() == 0) {
							buc.getOverflow().remove(z);// removed from vector of overflows
							String str = buc1.getFilepath();
							try {
								Files.deleteIfExists(Paths.get(str));// removed the actual file
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							z--;
						} else
							buc1.serialize(br1);
					}
				}
				if (br.size() == 0) {// law el main bucket fedi
					if (buc.getOverflow() != null && buc.getOverflow().size() > 0) {
						gb.set(i, buc.getOverflow().firstElement());// replaced the main with 1st overflow
						bucket bc2 = buc.getOverflow().firstElement();
						buc.getOverflow().remove(0);
						bc2.setOverflow(buc.getOverflow());
					} else {
						buc.setFilepath(null);
						gb.set(i, buc);
					}
					// removed from vector of overflows
					String str = buc.getFilepath();
					try {
						Files.deleteIfExists(Paths.get(str));// removed the actual file
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					i--;
				} else
					buc.serialize(br);
			}
			tableSer();
		} else
			deleteFromTableH(tableName, columnNameValue, t);
	}
	public void removeFromB(Hashtable<String, Object> c, bucket x, int i, ArrayList<bucket> gb) {// removes a record
																									// from a bucket
		if (!(x.isEmpty())) {
			Vector<bRecord> b = x.deserialize();
			for (int j = 0; j < b.size(); j++) {// looping in records in the bucket
				if (comp(c, b.get(j).getRecord())) {
					// delete(br1.get(j).getRecord().get(k),br1.get(j).getPage(),k,t);//once deleted
					// from page, remove from index
					b.remove(j);
					j--;
				}
			}
			if (x.getOverflow() != null && x.getOverflow().size() > 0) {
				for (int z = 0; z < x.getOverflow().size(); z++) {// list of all buckets
					bucket buc1 = x.getOverflow().get(z);
					Vector<bRecord> br1 = buc1.deserialize();
					for (int j = 0; j < br1.size(); j++) {// looping in records in the bucket
						if (comp(c, br1.get(j).getRecord())) {
							br1.remove(j);
							j--;
						}
					}
					if (br1.size() == 0) {
						x.getOverflow().remove(z);// removed from vector of overflows
						String str = buc1.getFilepath();
						try {
							Files.deleteIfExists(Paths.get(str));// removed the actual file
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						z--;
					} else
						buc1.serialize(br1);
				}
			}
			if (b.size() == 0) {// law el main bucket fedi
				if (x.getOverflow() != null && x.getOverflow().size() > 0) {
					gb.set(i, x.getOverflow().firstElement());// replaced the main with 1st overflow
					bucket bc2 = x.getOverflow().firstElement();
					x.getOverflow().remove(0);
					bc2.setOverflow(x.getOverflow());
				} else {
					x.setFilepath(null);
					gb.set(i, x);
				}
				// removed from vector of overflows
				String str = x.getFilepath();
				try {
					Files.deleteIfExists(Paths.get(str));// removed the actual file
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	public Hashtable<String, Object> getHash(Hashtable<String, Object> k, Vector<String> s) {// gets part of the
																								// hashtable that
																								// contains only those
																								// keys
		Hashtable<String, Object> r = new Hashtable<String, Object>();
		for (int i = 0; i < s.size(); i++) {
			r.put(s.get(i), k.get(s.get(i)));
		}
		/*
		 * Set<String> st = k.keySet(); for (String ke : st) { if (s.contains(ke))
		 * r.put(ke, k.get(ke)); }
		 */
		return r;
	}
	public void updateGridDel(Vector<Hashtable<String, Object>> rec, String k, Vector<grid> g) {// removes the records
																								// deleted
		// from all grids
		// Vector<grid> g = t.getGrids();
		for (int i = 0; i < g.size(); i++) {
			// hashof el values bta3t el columns el mawgoda fel grid, put in hashtable, call
			// find, remove from buckets
			grid gr = g.get(i);
			Vector<String> c = gr.getColNames();
			for (int j = 0; j < rec.size(); j++) {
				Hashtable<String, Object> x = getHash(rec.get(j), c);
				ArrayList<bucket> b = gr.find(x);
				for (int z = 0; z < b.size(); z++) {
					removeFromB(rec.get(j), b.get(z), z, b);// z is the index of the bucket in the grid
				}
				// Hashtable<String,Object> xc = rec.get(i);
			}
		}
	}
	public void updateGridinsert(Hashtable<String, Object> rec, String k, Vector<grid> g,page p,String tablename) throws DBAppException {// removes the
																									// records
		for (int i = 0; i < g.size(); i++) {
			grid gr = g.get(i);
			Vector<String> c = gr.getColNames();
			//for (int j = 0; j < rec.size(); j++) {
				Hashtable<String, Object> x = getHash(rec, c);
				ArrayList<bucket> b = gr.find(x);
				for (int z = 0; z < b.size(); z++) {
					if (!x.containsKey(k)) {
						x.put(k, rec.get(k));
					}
					bRecord r = new bRecord(p,x);
					b.get(i).insert(r, gr, tablename);// z is the index of the bucket in the grid
				//}
// Hashtable<String,Object> xc = rec.get(i);
			}
		}
	}
	public Hashtable<String, Object> delete(Object cl, page p, String k, Table t) throws DBAppException {// deletes the
																											// record
																											// from page
		Vector<Hashtable> c = deserial(p);
		int m = binarySearch(c, cl, k);// place of record in page
		if (m < 0)
			throw new DBAppException();
		Hashtable ht = c.get(m);
		if (p.getNoOfRe() > 1) {
			c.remove(m);
			p.setNoOfRe(p.getNoOfRe() - 1);
			p.setFirst(c.firstElement());
			p.setLast(c.lastElement());
			serialize(c, p);
		} else {
			if (p.getOverflow() == null || p.getOverflow().size() == 0) {
				page d = checkO(p, t.getPages());
				if (d == null)// main page,not an overflow
					t.getPages().remove(p);
				else
					d.getOverflow().remove(p);
			} else {
				int i = t.getPages().indexOf(p);
				page x = (page) p.getOverflow().get(0);
				Vector sof = p.getOverflow();
				sof.remove(x);
				x.setOverflow(sof);
				t.getPages().insertElementAt(x, i);
			}
		}
		return ht;
	}
	public boolean comp(Hashtable h, Hashtable y) {// has to have common, since I used the index
		boolean t = true;
		Set<String> keys = h.keySet();
		for (String key : keys) {
			if (y.containsKey(key)) {
				if (!(y.get(key).equals(h.get(key))))
					t = false;
			}
		}
		return t;
	}
	public void checkSel(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		int s = sqlTerms.length;
		int o = arrayOperators.length;
		if (o != s - 1) {
			throw new DBAppException();
		}
		String r = sqlTerms[0].getTableName().toLowerCase();
		for (int i = 0; i < s; i++) {// check all the table names entered by user are the same
			if (!(sqlTerms[i].getTableName().toLowerCase().equals(r)))
				throw new DBAppException();
		}
		Table t = checkT(r);
		ArrayList l = readFile();
		Vector x = new Vector();// vector of column names from metadata
		Hashtable<String, String> ht = new Hashtable<String, String>();
		Vector ind = new Vector();
		for (int i = 0; i < l.size(); i++) {// created a hashtable of:keys are column names, values are the type
			String[] a = (String[]) l.get(i); // a =l
			if (a[0].compareTo(r) == 0) {// law table name matches the user's input save col and type
				// x.add(a[1].toLowerCase());
				ht.put(a[1].toLowerCase(), a[2]);
				// ind.add(i);
			}
		}
		for (int i = 0; i < s; i++) {
			String col = sqlTerms[i].getColumnName().toLowerCase();
			if (!(ht.containsKey(col))) {// check col name by user exists
				throw new DBAppException();
			} else {
				String ty = ht.get(col);
				if (!(ty.contains(sqlTerms[i].getValue().getClass().getSimpleName()))) {// check the type by user
																						// matches the column type
					throw new DBAppException();
				}
				switch (sqlTerms[i].getOperator()) {
				case "=":
					break;
				case ">":
					break;
				case "<":
					break;
				case "!=":
					break;
				case ">=":
					break;
				case "<=":
					break;
				default:
					throw new DBAppException();
				}
			}
		}
		for (int i = 0; i < o; i++) {
			switch (arrayOperators[i]) {
			case "OR":
				break;
			case "AND":
				break;
			case "XOR":
				break;
			default:
				throw new DBAppException();
			}
		}
	}
	public SQLTerm[] xor(SQLTerm s1, SQLTerm s2) {
		// SQLTerm a= new SQLTerm(s1.getTableName(), s1.getColumnName(),
		// s1.getOperator());//NOT S1
		// SQLTerm b;
		String nop1 = negate(s1.getOperator());
		String nop2 = negate(s2.getOperator());
		SQLTerm r1 = new SQLTerm(s1.getTableName(), s1.getColumnName(), nop1, s1.getValue());// not a
		SQLTerm r2 = new SQLTerm(s2.getTableName(), s2.getColumnName(), nop2, s2.getValue());// not B
		SQLTerm[] sqlTerms = new SQLTerm[4];
		sqlTerms[0] = s1;// a
		sqlTerms[1] = r2;// not b
		// or
		sqlTerms[2] = r1;// not a
		sqlTerms[3] = s2; // b
		return sqlTerms;
	}
	public Vector<Hashtable> xor(SQLTerm s1, SQLTerm s2, Vector<page> v, String cl) throws DBAppException {
		SQLTerm[] s = xor(s1, s2);
		Vector<Hashtable> r1 = and(s[0], s[1], v, cl);// AB'
		Vector<Hashtable> r2 = and(s[2], s[3], v, cl);// A'B
		Vector<Hashtable> r3 = unionV(r1, r2);
		return r3;
	}
	public String negate(String op) {
		String nop = "";
		switch (op) {
		case "=": {
			nop = "!=";
			break;
		}
		case ">": {
			nop = "<=";
			break;
		}
		case "<": {
			nop = ">=";
			break;
		}
		case "!=": {
			nop = "=";
			break;
		}
		case ">=": {
			nop = "<";
			break;
		}
		case "<=": {
			nop = ">";
			break;
		}
		default:
			nop = "";
		}
		return nop;
	}
	public page checkO(page p, Vector<page> t) {
		if (!(t.contains(p))) {
			// law hia overflow return main page
			for (int i = 0; i < t.size(); i++) {
				if (((page) t.get(i)).getOverflow().contains(p)) {
					return (page) t.get(i);
				}
			}
			//
		}
		return null;
	}
	public Vector<Hashtable> linearS(String colName, Vector<page> p, Object val, String op) {
		Vector<Hashtable> x = new Vector<Hashtable>();
		for (int i = 0; i < p.size(); i++) {
			page g = p.get(i);
			Vector<Hashtable> r = deserial(g);
			for (int j = 0; j < r.size(); j++) {
				Hashtable h = r.get(j);
				Object o = h.get(colName);
				switch (op) {
				case "=":
					if (o.equals(val))
						x.add(h);
					break;
				case ">":
					if (bigger(o, val))
						x.add(h);
					break;
				case "<":
					if (bigger(val, o))
						x.add(h);
					break;
				case "!=":
					if (!(o.equals(val)))// .equals works with integers and doubles?
						x.add(h);
					break;
				case ">=":
					if (bigger(o, val) || o.equals(val))
						x.add(h);
					break;
				case "<=":
					if (bigger(val, o) || o.equals(val))
						x.add(h);
					break;
				default:
					// flag =false;
				}
			}
			serialize(r, g);
			if (g.getOverflow() != null && g.getOverflow().size() > 0) {
				for (int z = 0; z < g.getOverflow().size(); z++) {
					page pg = (page) g.getOverflow().get(z);
					Vector<Hashtable> rz = deserial(pg);
					for (int j = 0; j < rz.size(); j++) {
						Hashtable h = rz.get(j);
						Object o = h.get(colName);
						switch (op) {
						case "=":
							if (o.equals(val))
								x.add(h);
							break;
						case ">":
							if (bigger(o, val))
								x.add(h);
							break;
						case "<":
							if (bigger(val, o))
								x.add(h);
							break;
						case "!=":
							if (!(o.equals(val)))// .equals works with integers and doubles?
								x.add(h);
							break;
						case ">=":
							if (bigger(o, val) || o.equals(val))
								x.add(h);
							break;
						case "<=":
							if (bigger(val, o) || o.equals(val))
								x.add(h);
							break;
						default:
						}
					}
					serialize(rz, pg);
				}
			}
		}
		return x;
	}
	public Vector<Hashtable> linearS2(String colName, Vector<page> p, Object val, String op, Object eq, String cl) {
		Vector<Hashtable> x = new Vector<Hashtable>();
		for (int i = 0; i < p.size(); i++) {
			page g = p.get(i);
			Vector<Hashtable> r = deserial(g);
			for (int j = 0; j < r.size(); j++) {
				Hashtable h = r.get(j);
				Object o = h.get(colName);
				if (!(h.get(cl).equals(eq))) {
					switch (op) {
					case "=":
						if (o.equals(val))
							x.add(h);
						break;
					case ">":
						if (bigger(o, val))
							x.add(h);
						break;
					case "<":
						if (bigger(val, o))
							x.add(h);
						break;
					case "!=":
						if (!(o.equals(val)))// .equals works with integers and doubles?
							x.add(h);
						break;
					case ">=":
						if (bigger(o, val) || o.equals(val))
							x.add(h);
						break;
					case "<=":
						if (bigger(val, o) || o.equals(val))
							x.add(h);
						break;
					default:
						// flag =false;
					}
				}
			}
			serialize(r, g);
			if (g.getOverflow() != null && g.getOverflow().size() > 0) {
				for (int z = 0; z < g.getOverflow().size(); z++) {
					page pg = (page) g.getOverflow().get(z);
					Vector<Hashtable> rz = deserial(pg);
					for (int j = 0; j < rz.size(); j++) {
						Hashtable h = rz.get(j);
						Object o = h.get(colName);
						if (!(h.get(cl).equals(eq))) {
							switch (op) {
							case "=":
								if (o.equals(val))
									x.add(h);
								break;
							case ">":
								if (bigger(o, val))
									x.add(h);
								break;
							case "<":
								if (bigger(val, o))
									x.add(h);
								break;
							case "!=":
								if (!(o.equals(val)))// .equals works with integers and doubles?
									x.add(h);
								break;
							case ">=":
								if (bigger(o, val) || o.equals(val))
									x.add(h);
								break;
							case "<=":
								if (bigger(val, o) || o.equals(val))
									x.add(h);
								break;
							default:
							}
						}
					}
					serialize(rz, pg);
				}
			}
		}
		return x;
	}
	public void performQ(SQLTerm s1, SQLTerm s2, String arrayOperators, String cl, Table t) throws DBAppException {
		Vector<Hashtable> result;
		boolean flag;
		int ind = -1;
		if (s1.getColumnName().equals(cl)) {
			ind = 0;
		} else if (s2.getColumnName().equals(cl)) {
			ind = 1;
		}
		switch (arrayOperators) {
		case "AND":
			if (ind >= 0) {
				if (ind == 0) {
					Vector<Hashtable> h = new Vector<Hashtable>();
					switch (s1.getOperator()) {
					case "=":
						page p = DelUp(t.getPages(), s1.getValue(), t.getKey());
						Vector v = deserial(p);
						int c = binarySearch(v, s1.getValue(), t.getKey());
						Hashtable ht = (Hashtable) v.get(c);
						Object o = ht.get(s2.getColumnName());
						switch (s2.getOperator()) {
						case "=":
							if (o.equals(s2.getValue())) {
								flag = true;// should return the hashtable
							}
							break;
						case ">":
							if (bigger(o, s2.getValue())) {
								flag = true;
							}
							break;
						case "<":
							if (bigger(s2.getValue(), o)) {
								flag = true;
							}
							break;
						case "!=":
							if (!(o.equals(s2.getValue())))// .equals works with integers and doubles?
								flag = true;
							break;
						case ">=":
							if (bigger(o, s2.getValue()) || o.equals(s2.getValue()))
								flag = true;
							break;
						case "<=":
							if (bigger(s2.getValue(), o) || o.equals(s2.getValue()))
								flag = true;
							break;
						default:
							// flag =false;
						}
						break;
					case ">":
						page p1 = Select(t.getPages(), s1.getValue(), t.getKey());// page that has the index
						page po = checkO(p1, t.getPages());
						Vector<page> pg = new Vector<page>();
						if (po != null) {// da overflow get the main page=po
							int in = po.getOverflow().indexOf(p1);
							int in1 = t.getPages().indexOf(po) + 1;
							for (int i = in; i < po.getOverflow().size(); i++) {
								pg.add((page) po.getOverflow().get(i));
							}
							for (int i = in1; i < t.getPages().size(); i++) {
								pg.add(t.getPages().get(i));
							}
						} else {// not an overflow page
							int tg = (t.getPages().indexOf(p1));
							for (int i = tg; i < t.getPages().size(); i++) {
								pg.add(t.getPages().get(i));
							}
						}
						h = linearS(s2.getColumnName(), pg, s2.getValue(), s2.getOperator());
						break;
					case "<":// check case does it get the equal index??
						Vector<page> pgl = new Vector<page>();
						page p2 = Select(t.getPages(), s1.getValue(), t.getKey());
						page op = checkO(p2, t.getPages());
						if (op != null) {// da overflow get the main page=po
							int in = op.getOverflow().indexOf(p2);
							int in1 = t.getPages().indexOf(op) + 1;
							for (int i = in; i < op.getOverflow().size(); i++) {
								pgl.add((page) op.getOverflow().get(i));
							}
							for (int i = in1; i < t.getPages().size(); i++) {
								pgl.add(t.getPages().get(i));
							}
						} else {// not an overflow page
							int tg = (t.getPages().indexOf(p2));
							for (int i = tg; i < t.getPages().size(); i++) {
								pgl.add(t.getPages().get(i));
							}
						}
						h = linearS(s2.getColumnName(), pgl, s2.getValue(), s2.getOperator());
						break;
					case "!=":
						h = linearS2(s2.getColumnName(), t.getPages(), s2.getValue(), s2.getOperator(), s1.getValue(),
								t.getKey());
						break;
					case ">=":
						break;
					case "<=":
						break;
					}
				}
			}
			break;
		case "OR":
			break;
		case "XOR":// xor between 2 sql terms not results yet.
			SQLTerm[] s = xor(s1, s2);
			Vector<Hashtable> r1 = and(s[0], s[1], t.getPages(), t.getKey());// AB'
			Vector<Hashtable> r2 = and(s[2], s[3], t.getPages(), t.getKey());// A'B
			Vector<Hashtable> r3 = unionV(r1, r2);// r1+r2;
			break;
		}
	}
	public Vector<Hashtable> unionV(Vector<Hashtable> r1, Vector<Hashtable> r2) {
		Vector<Hashtable> r = new Vector<Hashtable>();
		r.addAll(r1);
		for (int i = 0; i < r2.size(); i++) {
			Hashtable h = r2.get(i);
			if (!(r.contains(h))) {
				r.add(h);
			}
		}
		return r;
	}
	public boolean performOp(Object value, String op, Object hashVal) {
		boolean flag = false;
		switch (op) {
		case "=":
			if (hashVal.equals(value)) {
				flag = true;// should return the hashtable
			}
			break;
		case ">":
			if (bigger(hashVal, value)) {
				flag = true;
			}
			break;
		case "<":
			if (bigger(value, hashVal)) {
				flag = true;
			}
			break;
		case "!=":
			if (!(hashVal.equals(value)))// .equals works with integers and doubles?
				flag = true;
			break;
		case ">=":
			if (bigger(hashVal, value) || hashVal.equals(value))
				flag = true;
			break;
		case "<=":
			if (bigger(value, hashVal) || hashVal.equals(value))
				flag = true;
			break;
		}
		return flag;
	}
	public Vector<Hashtable> getP(String op, Object val, String cl, Vector<page> z) throws DBAppException {
		Vector<Hashtable> p = new Vector<Hashtable>();
		page pg = Select(z, val, cl);
		page o = checkO(pg, z);// pg now overflow page, o is the main page
		boolean f2 = false;
		page of = null;
		int in2 = -1;
		Vector<Hashtable> ht = deserial(pg);
		serialize(ht, pg);
		if (o != null) {
			f2 = true;// overflow page
			of = pg;// of is the overflow page
			pg = o;// pg is the main page
			in2 = pg.getOverflow().indexOf(of);
			ht = deserial(of);
			serialize(ht, of);
		}
		int in = z.indexOf(pg);
		int x = bSearchP(cl, val, ht);// binarysearch in page to get location-negative for correct val
		boolean f = true;// different for greater than/equal
		if (x < 0) {
			f = false;// not found, same for >= and >
		}
		switch (op) {
		case "=":// only this value compare
			if (f) {
				p.add(ht.get(x));
			}
			break;
		case ">":
			in = z.indexOf(pg);
			int x3 = x + 1;
			if (!f) {
				x3 = x * -1;// for loop <x --> <x3
			}
			for (int i = x3; i < ht.size(); i++) {
				p.add(ht.get(i));
			}
			if (f2) {// law overflow page
				for (int i = in2 + 1; i < pg.getOverflow().size(); i++) {
					page l2 = (page) pg.getOverflow().get(i);
					Vector<Hashtable> h2 = deserial(l2);
					p.addAll(h2);
					serialize(h2, l2);
				}
			} // law msh overflow check law 3andaha overflow
			for (int i = in + 1; i < z.size(); i++) {
				page l = z.get(i);
				Vector<Hashtable> h = deserial(l);
				p.addAll(h);
				serialize(h, l);
				if (l.getOverflow() != null && l.getOverflow().size() > 0) {
					for (int j = 0; j < l.getOverflow().size(); j++) {
						page l2 = (page) l.getOverflow().get(j);
						Vector<Hashtable> h2 = deserial(l2);
						p.addAll(h2);
						serialize(h2, l2);
					}
				}
			}
			break;
		case "<":
			int x2 = x;
			if (!f) {
				x2 = x * -1;// for loop <x --> <x2
			}
			for (int i = 0; i < in; i++) {// add everything before page
				page l = z.get(i);
				Vector<Hashtable> h = deserial(l);
				p.addAll(h);
				serialize(h, l);
				if (l.getOverflow() != null && l.getOverflow().size() > 0) {
					for (int j = 0; j < l.getOverflow().size(); j++) {
						page l2 = (page) l.getOverflow().get(j);
						Vector<Hashtable> h2 = deserial(l2);
						p.addAll(h2);
						serialize(h2, l2);
					}
				}
			}
			Vector<Hashtable> h3 = deserial(pg);
			if (f2)// ;law hia overflow
			{
				// get main page
				p.addAll(h3);// get main page, then all pages till overflow in2
				serialize(h3, pg);
				for (int i = 0; i < in2; i++) {// for loop in overflow till correct page then skip the element
					page current = (page) pg.getOverflow().get(i);
					Vector<Hashtable> h2 = deserial(current);
					p.addAll(h2);
					serialize(h2, current);
				}
				for (int i = 0; i < x2; i++) {
					p.add(ht.get(i));
				}
			} else {// msh overflow, stopped fel page el abliha
				for (int i = 0; i < x2; i++) {
					p.add(h3.get(i));
				}
				serialize(h3, pg);
			}
			break;
		case "!=": {// law not found use all, law found loop on all and skip entry
			if (!f) {// f false, object not found
				for (int i = 0; i < z.size(); i++) {
					page l = z.get(i);
					Vector<Hashtable> h = deserial(l);
					p.addAll(h);
					serialize(h, l);
					if (l.getOverflow() != null && l.getOverflow().size() > 0) {
						for (int j = 0; j < l.getOverflow().size(); j++) {
							page l2 = (page) l.getOverflow().get(j);
							Vector<Hashtable> h2 = deserial(l2);
							p.addAll(h2);
							serialize(h2, l2);
						}
					}
				}
			} else {// missing if this page is an overflow page
				in = z.indexOf(pg);// index of main page
				for (int i = 0; i < in; i++) {
					page l = z.get(i);
					Vector<Hashtable> h = deserial(l);
					p.addAll(h);
					serialize(h, l);
					if (l.getOverflow() != null && l.getOverflow().size() > 0) {
						for (int j = 0; j < l.getOverflow().size(); j++) {
							page l2 = (page) l.getOverflow().get(j);
							Vector<Hashtable> h2 = deserial(l2);
							p.addAll(h2);
							serialize(h2, l2);
						}
					}
				}
				if (!f2) {// not an overflow page
					for (int i = 0; i < x; i++) {
						p.add(ht.get(i));
					}
					for (int i = x + 1; i < ht.size(); i++) {
						p.add(ht.get(i));
					}
					if (pg.getOverflow() != null && pg.getOverflow().size() > 0) {
						for (int j = 0; j < pg.getOverflow().size(); j++) {
							page l2 = (page) pg.getOverflow().get(j);
							Vector<Hashtable> h2 = deserial(l2);
							p.addAll(h2);
							serialize(h2, l2);
						}
					}
				} else {// overflow page, get main page till overflow in2-1 skip in2
					h3 = deserial(pg);
					p.addAll(h3);
					// all hashtables in this page, now go to overflow
					for (int i = 0; i < in2; i++) {// for loop in overflow till correct page then skip the element
						page current = (page) pg.getOverflow().get(i);
						Vector<Hashtable> h2 = deserial(current);
						p.addAll(h2);
						serialize(h2, current);
					}
					for (int i = 0; i < x; i++) {
						p.add(ht.get(i));
					}
					for (int i = x + 1; i < ht.size(); i++) {
						p.add(ht.get(i));
					}
					for (int i = in2 + 1; i < pg.getOverflow().size(); i++) {// ba2eet overflow pages
						page l = (page) pg.getOverflow().get(i);
						Vector<Hashtable> h = deserial(l);
						p.addAll(h);
						serialize(h, l);
					}
				}
				for (int i = in + 1; i < z.size(); i++) {
					page l = z.get(i);
					Vector<Hashtable> h = deserial(l);
					p.addAll(h);
					serialize(h, l);
					if (l.getOverflow() != null && l.getOverflow().size() > 0) {
						for (int j = 0; j < l.getOverflow().size(); j++) {
							page l2 = (page) l.getOverflow().get(j);
							Vector<Hashtable> h2 = deserial(l2);
							p.addAll(h2);
							serialize(h2, l2);
						}
					}
				}
			}
			break;
		}
		case ">=":
			in = z.indexOf(pg);
			x2 = x;
			if (!f) {
				x2 = x * -1;// for loop <x --> <x2
			}
			for (int i = x2; i < ht.size(); i++) {
				p.add(ht.get(i));
			}
			if (f2) {// law overflow page
				for (int i = in2 + 1; i < pg.getOverflow().size(); i++) {
					page l2 = (page) pg.getOverflow().get(i);
					Vector<Hashtable> h2 = deserial(l2);
					p.addAll(h2);
					serialize(h2, l2);
				}
			}
			for (int i = in + 1; i < z.size(); i++) {
				page l = z.get(i);
				Vector<Hashtable> h = deserial(l);
				p.addAll(h);
				serialize(h, l);
				if (l.getOverflow() != null && l.getOverflow().size() > 0) {
					for (int j = 0; j < l.getOverflow().size(); j++) {
						page l2 = (page) l.getOverflow().get(j);
						Vector<Hashtable> h2 = deserial(l2);
						p.addAll(h2);
						serialize(h2, l2);
					}
				}
			}
			break;
		case "<=":// all values less than or equal to primary key already found, law msh found get
			x2 = x;
			if (!f) {
				x2 = x * -1;// for loop <x --> <x2
			} else {
				x2 = x + 1;// for loop<x+1 --> <x2
			}
			in = z.indexOf(pg);
			for (int i = 0; i < in; i++) {
				page l = z.get(i);
				Vector<Hashtable> h = deserial(l);
				p.addAll(h);
				serialize(h, l);
				if (l.getOverflow() != null && l.getOverflow().size() > 0) {
					for (int j = 0; j < l.getOverflow().size(); j++) {
						page l2 = (page) l.getOverflow().get(j);
						Vector<Hashtable> h2 = deserial(l2);
						p.addAll(h2);
						serialize(h2, l2);
					}
				}
			}
			h3 = deserial(pg);
			if (f2)// ;law hia overflow
			{
				// get main page
				p.addAll(h3);// get main page, then all pages till overflow in2
				serialize(h3, pg);
				for (int i = 0; i < in2; i++) {// for loop in overflow till correct page then skip the element
					page current = (page) pg.getOverflow().get(i);
					Vector<Hashtable> h2 = deserial(current);
					p.addAll(h2);
					serialize(h2, current);
				}
				for (int i = 0; i < x2; i++) {
					p.add(ht.get(i));
				}
			} else {// msh overflow, stopped fel page el abliha
				for (int i = 0; i < x2; i++) {
					p.add(h3.get(i));
				}
				serialize(h3, pg);
			}
			break;
		default:
			throw new DBAppException();
		}
		return p;
	}
	public Vector<Hashtable> and(SQLTerm s1, SQLTerm s2, Vector<page> v, String cl) throws DBAppException {
		Vector<Hashtable> r = new Vector<Hashtable>();
		SQLTerm prim = null;
		SQLTerm sec = null;
		if (s1.getColumnName().equals(cl) && s2.getColumnName().equals(cl)) {
			if (s1.getOperator().equals("=")) {
				prim = s1;
				sec = s2;
			} else if (s2.getOperator().equals("=")) {
				prim = s2;
				sec = s1;
			}
		} else if (s1.getColumnName().equals(cl)) {
			prim = s1;
			sec = s2;
		} else if (s2.getColumnName().equals(cl)) {
			prim = s2;
			sec = s1;
		}
		if (prim == null) {// mafish clustering key , msh da code el linear search?
			// linear search 3ala el pages
			prim = s1;
			sec = s2;
			for (int i = 0; i < v.size(); i++) {
				page p = (page) v.get(i);
				Vector<Hashtable> v1 = deserial(p);
				for (int k = 0; k < v1.size(); k++) {
					Hashtable h = v1.get(k);
					if ((performOp(prim.getValue(), prim.getOperator(), h.get(prim.getColumnName())))
							&& (performOp(sec.getValue(), sec.getOperator(), h.get(sec.getColumnName())))) {
						r.add(h);
					}
				}
				serialize(v1, p);
				if (p.getOverflow() != null && p.getOverflow().size() > 0) {
					for (int k = 0; k < p.getOverflow().size(); k++) {
						page p2 = (page) p.getOverflow().get(k);
						Vector<Hashtable> v2 = deserial(p2);
						for (int j = 0; j < v2.size(); j++) {
							Hashtable h = v2.get(j);
							if ((performOp(prim.getValue(), prim.getOperator(), h.get(prim.getColumnName())))
									&& (performOp(sec.getValue(), sec.getOperator(), h.get(sec.getColumnName())))) {
								r.add(h);
							}
						}
						serialize(v2, p2);
					}
				}
			}
			return r;// result set of all pages of an AND condition
		} else {
			Vector<Hashtable> t = getP(prim.getOperator(), prim.getValue(), cl, v);// all pages from specified location
			// vector has all pages with the specified condition of pk, check the next
			// condition
			// code el linear search tani
			for (int i = 0; i < t.size(); i++) {
				Hashtable h = t.get(i);
				if ((performOp(sec.getValue(), sec.getOperator(), h.get(sec.getColumnName())))) {
					r.add(h);
				}
			}
			return r;
		}
	}
	public Vector<Hashtable> and(Vector<Hashtable> h, SQLTerm s2) {
		Vector<Hashtable> r = new Vector<Hashtable>();
		for (int i = 0; i < h.size(); i++) {
			Hashtable ht = h.get(i);
			if (performOp(s2.getValue(), s2.getOperator(), ht.get(s2.getColumnName()))) {
				r.add(ht);
			}
		}
		return r;
	}
	public Vector<Hashtable> OR(SQLTerm s1, SQLTerm s2, Vector<page> v, String cl) throws DBAppException {
		Vector<Hashtable> h = new Vector<Hashtable>();
		if (s1.getColumnName().equals(cl)) {
			h = getP(s1.getOperator(), s1.getValue(), cl, v);
		} else {
			h = linearS(s1.getColumnName(), v, s1.getValue(), s1.getOperator());
		}
		Vector<Hashtable> h2 = new Vector<Hashtable>();
		if (s2.getColumnName().equals(cl)) {
			h2 = getP(s1.getOperator(), s1.getValue(), cl, v);
		} else {
			h2 = linearS(s1.getColumnName(), v, s1.getValue(), s1.getOperator());
		}
		Vector<Hashtable> r = unionV(h, h2);
		return r;
	}
	public Vector<Hashtable> OR(SQLTerm s1, Vector<Hashtable> h2, Vector<page> v, String cl) throws DBAppException {
		Vector<Hashtable> h = new Vector<Hashtable>();
		if (s1.getColumnName().equals(cl)) {
			h = getP(s1.getOperator(), s1.getValue(), cl, v);
		} else {
			h = linearS(s1.getColumnName(), v, s1.getValue(), s1.getOperator());
		}
		Vector<Hashtable> r = unionV(h, h2);
		return r;
	}
	public Vector<Hashtable> and(Vector<Hashtable> l, Vector<Hashtable> h) {// for ANDing 2 result sets
		Vector<Hashtable> r = new Vector<Hashtable>();
		for (int i = 0; i < l.size(); i++) {
			if (h.contains(l.get(i)))
				r.add(l.get(i));
		}
		return r;
	}
	public Vector<Hashtable> op(Object b, Object c, String cl, Vector<page> v, String o) throws DBAppException {
		Vector<Hashtable> r = new Vector<Hashtable>();
		String x = b.getClass().getSimpleName();// vector of hashtables wla sql
		String x2 = c.getClass().getSimpleName();
		if (x.equals("Vector") && x2.equals("Vector")) {
			Vector a = (Vector) b;
			Vector d = (Vector) c;
			switch (o) {
			case "AND":
				r = and(a, d);
				break;
			case "OR":
				r = unionV(a, d);
				break;
			default:
				r = xor(a, d, v);
			}
		} else if (x.equals("Vector") && x2.equals("SQLTerm")) {
			Vector<Hashtable> a = (Vector) b;
			SQLTerm d = (SQLTerm) c;
			switch (o) {
			case "AND":
				r = and(a, d);
				break;
			case "OR":
				r = OR(d, a, v, cl);
				break;
			default:
				r = xor(a, d, v, cl);
			}
		} else if (x.equals("SQLTerm") && x2.equals("Vector")) {
			Vector<Hashtable> a = (Vector) c;
			SQLTerm d = (SQLTerm) b;
			switch (o) {
			case "AND":
				r = and(a, d);
				break;
			case "OR":
				r = OR(d, a, v, cl);
				break;
			default:
				r = xor(a, d, v, cl);
			}
		} else {
			SQLTerm a = (SQLTerm) b;
			SQLTerm d = (SQLTerm) c;
			switch (o) {
			case "AND":
				r = and(a, d, v, cl);
				break;
			case "OR":
				r = OR(a, d, v, cl);
				break;
			default:
				r = xor(a, d, v, cl);
				break;
			}
		}
		return r;
	}
	private Vector<Hashtable> xor(Vector<Hashtable> a, SQLTerm d, Vector<page> v, String cl) throws DBAppException {
		Vector<Hashtable> r = new Vector<Hashtable>();
		Vector<Hashtable> h = new Vector<Hashtable>();
		if (d.getColumnName().equals(cl)) {
			h = getP(d.getOperator(), d.getValue(), cl, v);
		} else {
			h = linearS(d.getColumnName(), v, d.getValue(), d.getOperator());
		}
		r = xor(a, h, v);
		return r;
	}
	private Vector<Hashtable> xor(Vector<Hashtable> a, Vector<Hashtable> d, Vector<page> v) {
		Vector<Hashtable> r = new Vector<Hashtable>();
		Vector<Hashtable> na = negate(a, v);// not a
		Vector<Hashtable> nd = negate(d, v);// not d
		r = unionV(and(na, d), and(nd, a));
		return r;
	}
	public Vector<Hashtable> allElements(Vector<page> v) {
		Vector<Hashtable> h = new Vector<Hashtable>();
		for (int i = 0; i < v.size(); i++) {
			page p = v.get(i);
			Vector<Hashtable> r = deserial(p);
			h.addAll(r);
			serialize(r, p);
			if (p.getOverflow() != null && p.getOverflow().size() > 0) {
				for (int j = 0; j < p.getOverflow().size(); j++) {
					page l2 = (page) p.getOverflow().get(j);
					Vector<Hashtable> h2 = deserial(l2);
					h.addAll(h2);
					serialize(h2, l2);
				}
			}
		}
		return h;
	}
	public Vector<Hashtable> negate(Vector<Hashtable> h, Vector<page> p) {
		Vector<Hashtable> x = allElements(p);
		x.removeAll(h);
		return x;
	}
	public grid getG(Vector<String> col, Table t) {
		Vector<grid> d = t.getGrids();
		for (int i = 0; i < d.size(); i++) {
			Vector<String> c = d.get(i).getColNames();
			if (checkE(c, col)) {
				return d.get(i);
			}
		}
		return null;
	}
	public boolean checkE(Vector<String> c, Vector<String> d) {
		if (c.size() != d.size())
			return false;
		for (int i = 0; i < c.size(); i++) {
			if (!d.contains(c.get(i))) {
				return false;
			}
		}
		return true;
	}
	public Vector<String> getC(SQLTerm[] sqlTerms) {
		Vector<String> r = new Vector<String>();
		for (int i = 0; i < sqlTerms.length; i++) {
			String x = sqlTerms[i].getColumnName();
			if (!r.contains(x))
				r.add(x);
		}
		return r;
	}
	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		checkSel(sqlTerms, arrayOperators);
		// if there's no index
		Table t = checkT(sqlTerms[0].getTableName());
		String k = t.getKey();
		int index = -1;
		for (int i = 0; i < sqlTerms.length; i++) {
			String col = sqlTerms[i].getColumnName();
			if (col.equals(k)) {
				index = i;
				break;
			}
		}
		/*
		 * if(arrayOperators[0].equals("AND")) if(index>=0) {
		 * if(sqlTerms[index].getOperator().equals("=")) { //int
		 * bSearch(k,sqlTerms[index].getValue(),t.getPages()); page
		 * p=DelUp(t.getPages(),sqlTerms[index].getValue(),k); } }
		 */
		Vector<String> str = getC(sqlTerms);
		grid g = getG(str, t);//can be partial, recheck
		Vector<SQLTerm> sq = new Vector<SQLTerm>();
		sq.addAll(Arrays.asList(sqlTerms));
		Vector<Object> s = new Vector<Object>();
		s.addAll(Arrays.asList(sqlTerms));
		Vector<String> o = new Vector<String>();
		o.addAll(Arrays.asList(arrayOperators));
		if (g == null) {
			if (o.size() == 0 && s.size() == 1) {
				Vector<Hashtable> z = new Vector<Hashtable>();
				if (sq.get(0).getColumnName().equalsIgnoreCase(k)) {
					z = getP(sq.get(0).getOperator(), sq.get(0).getValue(), k, t.getPages());
				} else {
					z = linearS(sq.get(0).getColumnName(), t.getPages(), sq.get(0).getValue(), sq.get(0).getOperator());
				}
				Iterator i = z.iterator();
				tableSer();
				return i;
			} else {
				while (o.size() != 0) {
					while (o.contains("AND")) {
						int ind = o.indexOf("AND");
						Vector<Hashtable> h = new Vector<Hashtable>();
						h = op(s.get(ind), s.get(ind + 1), k, t.getPages(), "AND");
						o.remove(ind);
						s.remove(ind + 1);
						s.remove(ind);
						s.add(ind, h);
					}
					while (o.contains("OR")) {
						int ind = o.indexOf("OR");
						Vector<Hashtable> h = new Vector<Hashtable>();
						h = op(s.get(ind), s.get(ind + 1), k, t.getPages(), "OR");
						o.remove(ind);
						s.remove(ind + 1);
						s.remove(ind);
						s.add(ind, h);
					}
					while (o.contains("XOR")) {
						int ind = o.indexOf("XOR");
						Vector<Hashtable> h = new Vector<Hashtable>();
						h = op(s.get(ind), s.get(ind + 1), k, t.getPages(), "XOR");
						o.remove(ind);
						s.remove(ind + 1);
						s.remove(ind);
						s.add(ind, h);
					}
				}
				Vector v = (Vector) s.get(0);
				Iterator i = v.iterator();
				// else
				tableSer();
				return i;
			}
		} else {
			// got the grid now use it to find comb of indices
			ArrayList<bucket> b = g.select(sq, o);
			if (b==null)
				throw new DBAppException();
			Vector<Hashtable> h = new Vector<Hashtable>();
			for (int i = 0; i < b.size(); i++) {
				h.addAll(checkV(sqlTerms, b.get(i), k));
			}
			Iterator i = h.iterator();
			tableSer();
			return i;
			/*Vector<ArrayList<bucket>> v = new Vector<ArrayList<bucket>>();
			for (int i = 0; i < sq.size(); i++) {
				Hashtable h = new Hashtable();
				h.put(sq.get(i).getColumnName(), sq.get(i).getValue());
				g.find(h);// indices of that value buckets
				v.add(g.find(h));
			}
			for (int i = 0; i < o.size(); i++) {
				switch (o.get(i)) {
				case "AND":
					ArrayList<bucket> a = getCommB(v.get(i), v.get(i + 1));
					v.add(i, a);
					v.remove(i + 2);
					v.remove(i + 1);
					o.remove(i);
					i--;
					break;
				case "OR":
					ArrayList b = getU(v.get(i), v.get(i + 1));
					v.add(i, b);
					v.remove(i + 2);
					v.remove(i + 1);
					o.remove(i);
					i--;
					break;
				case "XOR":
					ArrayList c = removec(v.get(i), v.get(i + 1));
					v.add(i, c);
					v.remove(i + 2);
					v.remove(i + 1);
					o.remove(i);
					i--;
					break;
				default:
					throw new DBAppException();
				}
				if (i == 0)
					break;
			}
			*/
			//if (v.size() != 1)
			//	throw new DBAppException();
		//	ArrayList<bucket> b = v.get(0);// the result set of buckets to check for values
		}
	}
	public Vector<Hashtable> checkV(SQLTerm[] sqlTerms, bucket c, String cl) {
		Vector<Hashtable> r = new Vector<Hashtable>();
		Vector<bRecord> b = c.deserialize();
		for (int i = 0; i < b.size(); i++) {
			Hashtable h = b.get(i).getRecord();
			if (checkK(h, sqlTerms))
				r.add(getH(b.get(i), cl));
		}
		return r;
	}
	public Hashtable getH(bRecord b, String cl) {
		page p = b.getPage();// page
		Object val = b.getRecord().get(cl);// clusteringkey
		Vector<Hashtable> h = deserial(p);
		int m = binarySearch(h, val, cl);// place of record in page
		Hashtable ht = h.get(m);
		return ht;
	}
	public boolean checkK(Hashtable h, SQLTerm[] sqlTerms) {
		boolean f = false;
		for (int i = 0; i < sqlTerms.length; i++) {
			String colName = sqlTerms[i].getColumnName();
			String op = sqlTerms[i].getOperator();
			Object c = sqlTerms[i].getValue();
			Object val = h.get(colName);
			f = performOp(c, op, val);
		}
		return f;
	}
	public ArrayList<bucket> getU(ArrayList<bucket> a, ArrayList<bucket> b) {
		a.addAll(b);
		Set<bucket> set = new HashSet<>(a);
		a.clear();
		a.addAll(set);
		return a;
	}
	public ArrayList<bucket> removec(ArrayList<bucket> a, ArrayList<bucket> b) {
		for (int i = 0; i < a.size(); i++) {
			if (b.contains(a.get(i))) {
				b.remove(a.get(i));
				a.remove(i); // removed all the common from a, add the ones in b
			}
		}
		return getCommB(a, b);
	}
	public ArrayList<bucket> getCommB(ArrayList<bucket> a, ArrayList<bucket> b) {
		a.retainAll(b);
		return a;
	}
	public static ArrayList readFile() {
		String currentLine = "";
		ArrayList line = new ArrayList();
		FileReader fileReader;
		try {
			fileReader = new FileReader(csvUrl);
			BufferedReader br = new BufferedReader(fileReader);
			while ((currentLine = br.readLine()) != null) {
				String[] line1 = currentLine.split(",");
				line.add(line1);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return line;
	}
	public static void print(Vector<bRecord> r) {
		for (int i = 0; i < r.size(); i++) {
			bRecord c = r.get(i);
			System.out.print(c.getRecord());
			// System.out.println(c.getPage().getPageno());
		}
	}
	// Parsing the currentLine String
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		
	}
}
