import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;
public class grid implements java.io.Serializable {
	/**
		 * 
		 */
	private static final long serialVersionUID = 1L;
	// [1[a[78,9],b[78,9] ,c[78,9]],2[a,b,c],3[a,b,c]]
	String filepath;
	int bc = 0;
	Vector cells = new Vector();
	int n = 0;
	column[] columns;
	Double[][] col;
	bucket[] buckets;
	HashMap<String, Integer> indx = new HashMap<String, Integer>();
	Vector<String> colNames;
	public grid(String TableName, String[] columnNames) {
		colNames = new Vector<String>();
		Collections.addAll(colNames, columnNames);
		n = columnNames.length;
		columns = new column[n];
		col = new Double[n][10];
		buckets = new bucket[(int) Math.pow(10, n)];
		for (int i = 0; i < n; i++) {
			columns[i] = new column(TableName, columnNames[i]);
			col[i] = columns[i].getRanges();
		}
		formGrid(col);
		// cells=formGrid(col);
		for (int i = 0; i < buckets.length; i++) {
			bucket b = new bucket(false);
			buckets[i] = b;
		}
	}
	public static Double ascii(String name) {
		// String to check it's value
		int nameLenght = name.length(); // length of the string used for the loop
		int ascii = 0;
		for (int i = 0; i < nameLenght; i++) { // while counting characters if less than the length add one
			char character = name.charAt(i); // start on the first character
			ascii = ascii + (int) character; // convert the first character
			// print the character and it's value in ascii
		}
		return (double) ascii;
	}
	public void formGrid(Double[][] col) {
		int n = (int) Math.pow(10, col.length); // number of cells, 3 is the number of divisions
		Object[][] ind = new Object[n][col.length];// here we will store the cells
		for (int i = 0; i < col.length; i++) {
			int index = 0;
			int r = (int) Math.pow(10, i);
			for (int m = 0; m < r; m++) {
				// int k;
				for (int j = 0; j < col[i].length; j++) {
					int rep = (int) Math.pow(10, col.length - (i + 1));
					// System.out.println(col[i][j]);
					for (int k = 0; k < rep; k++) {
						ind[index][i] = col[i][j];
						index++;
						// System.out.println(index);
					}
					// index++;}
				}
			}
		}
		for (int i = 0; i < ind.length; i++) {
			indx.put(Arrays.toString(ind[i]), i);
		}
	}
	public boolean bigger(Object x, Object y) {
		if (x instanceof String) {
			String z=(String) y;
		if (((String) x).toLowerCase().compareTo(z.toLowerCase())>0) {
		return true;
		} else {
		return false;
		}
		}
		else if (x instanceof Integer ) {
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
		if (((Date) x).compareTo((Date) (y)) > 0) {
		return true;
		} else
		return false;
		}
		return false;
		}
		/////////////////
	public page  checkO(page p, Vector<page>t) {
		if(!(t.contains(p))) {
			//law hia overflow return main page
			for(int i=0;i<t.size();i++) {
				if (((page)t.get(i)).getOverflow().contains(p)) {
					return (page)t.get(i);
				}
			}
			//
		}
		return null;}
	public page locatePage(bRecord b,String clust,Table table,int maxi) {
		page prev=this.searchprevKey(b ,clust).getPage();
		page next=this.searchnxtKey(b ,clust).getPage();
		if(prev.equals(next)){
			return prev;}
		else if(bigger(b.getRecord().get(clust),prev.getFirst())&&bigger(prev.getLast(),b.getRecord().get(clust))) {
			return prev;
		}
		else if(bigger(b.getRecord().get(clust),next.getFirst())&&bigger(next.getLast(),b.getRecord().get(clust))) {
			return next;
	}
		else {page o=checkO(prev,table.getPages());
		if(o==null) {
			if(prev.noOfRe<maxi) {
				return prev;
			}
			else if(next.getNoOfRe()<maxi) {
				return next;
			}
			else{page p=new page(table.getName(),0);prev.getOverflow().add(p);
			return p;
			}
			}
		else {if(prev.noOfRe<maxi) {
			return prev;
		}
		else if(next.getNoOfRe()<maxi) {
			return next;
		}
		else {	int x=o.getOverflow().indexOf(prev)+1;
		page p=new page(table.getName(),0);
		o.getOverflow().add(x+1, p);
			return p;}
		}
	}}
		public bRecord searchprevKey(bRecord val,String key) {
		Hashtable h=new Hashtable();
		h.put(key,val);
		ArrayList<bucket> a=	find(h);
		Vector <bRecord> v=new Vector <bRecord>();
		for(int i=0;i<a.size();i++) {
			if(a.get(i).filepath!=null&&a.get(i).filepath.length()!=0) {
				v.addAll(a.get(i).allelements());}}
	   int x= v.indexOf(val);
	   bRecord prev= v.get(x-1);
	   Object max=prev.getRecord().get(key);
	   for(int i=0;i<v.size();i++) {
		   bRecord brec=v.get(i);
	   if(bigger(val.getRecord().get(key),brec.getRecord().get(key))&&bigger(brec.getRecord().get(key),max)){
		   max=brec.getRecord().get(key);
		   prev=brec;
	   }
		}return prev;
	 }
		public bRecord searchnxtKey(bRecord val,String key) {
			Hashtable h=new Hashtable();
			h.put(key,val);
			ArrayList<bucket> a=	find(h);
			Vector <bRecord> v=new Vector <bRecord>();
			for(int i=0;i<a.size();i++) {
				if(a.get(i).filepath!=null&&a.get(i).filepath.length()!=0) {
			v.addAll(a.get(i).allelements());}}
		   int x= v.indexOf(val);
		   bRecord nxt= v.get(x+1);
		   Object min=nxt.getRecord().get(key);
		   for(int i=0;i<v.size();i++) {
			   bRecord brec=v.get(i);
		   if(bigger(brec.getRecord().get(key),val.getRecord().get(key))&&bigger(min,brec.getRecord().get(key))){
			   min=brec.getRecord().get(key);
			  nxt=brec;
		   }
			} return nxt;
		 }
		public double convertD(Date d) {
			//return date()
			double a = d.getYear();
			double b = d.getMonth();
			double c = d.getDay();
			double z = a*365+b*30+c;
			//double f = ascii(z);
			return z;
			
		}
	public ArrayList<bucket> find(Hashtable h) {
		// Hashtable<String,Double> ht = new Hashtable<String,Double>();
		Hashtable<String, Object> o = new Hashtable<String, Object>();
		o.putAll(h);
		Double a[] = new Double[n];
		Set<String> keys = o.keySet();
		for (String key : keys) {
			Object x = o.get(key);
			if (x instanceof String) {
				String x1 = checknum((String)x,key);//converts 641 to 0641 then to ascii
				o.replace(key, ascii((String) x1));
			} // date too
			if (x instanceof Integer) {
				o.replace(key, (double) (int) x);
			}
			if(x instanceof Date) {
				o.replace(key, convertD((Date)x));
			}
		}
		for (int i = 0; i < n; i++) {
			String name = columns[i].getName();
			column c = columns[i];
			for (String key : keys) {
				String keyr = key;
				String hkey = o.get(keyr) + "";
				if (key.contentEquals(c.getName())) {
					int j = c.getJumps(); 
					int x = (int) ((Double) o.get(name) - columns[i].getMin()) / j;
					if (x==10) {
						x=9;
					}
					a[i] = columns[i].getRanges()[x];
					break;
				}
			}
		}
		int k = 0;
		int mino = -1;
		ArrayList c1 = new ArrayList();
		for (int i = 0; i < col.length; i++) {
			if (a[i] == null) {
				a[i] = col[i][0];
				k++;
				mino = (int) Math.pow(10, col.length - (i + 1));
			}
			else {
			}
		}
		int x = indx.get(Arrays.toString(a));
		c1.add(buckets[x]);
		k = (int) Math.pow(10, k);
		for (int i = 0; i < k - 1; i++) {
			x = x + mino;
			c1.add(buckets[x]);
		}
		return c1;
	}
	public int diffdate(Double min,Date user,Hashtable<String,Double>h) {//it gets the start range of the date according to date month or year (will use it in find)
		int diff =0;
		if(h.containsKey("year")) {
		diff= (int) ((user.getYear()-min)+1);
		}
		if(h.containsKey("month")) {
		diff= (int) ((user.getMonth()-min)+1);
		}
		if(h.containsKey("day")) {
		diff=(int) (user.getDay()-min)+1;
		}
		return diff;
		}
	// x= (key)/jump then (jump*x) if >max then place in max
	// public bucket
	public static void main(String[] args) throws IOException {
		// System.out.println( asciiToSentence(65+66));
		// grid g=new grid();
		Object[][] col = { { 1, 2, 3 }, { "a", "b", "c" } };
		// g.formGrid(col);
		final int[] dimension = new int[3];
		Arrays.fill(dimension, 10);
		// String name = "admin";//int ascii = name.toCharArray()[0];
		Double[] a = new Double[3];
		/// int x=1/2;
		// a[0]=(double) x;
		System.out.println(Arrays.toString(a));
		String mino = "aa";
		String maxo = "cc";
		Double[] ranges = new Double[10];
		Double min = (double) 1.5;
		Double max = (double) 10.8;
		int x = (int) (((max - min) + 1) / 10);
		Double y = ((max - min) + 1);
		if (y % 10 != 0) {
			x = x + 1;
		}
		System.out.println(x);
		int i = 0;
		double m = min;
		// ranges[0]=m;
		while (m <= max) {
			ranges[i] = m;
			m = m + x;
			i++;
		}
		System.out.println(Arrays.toString(ranges) + ranges[(int) ((10.9 - min) / x)]);
	}
	public int getBc() {
		return bc;
	}
	public void setBc(int bc) {
		this.bc = bc;
	}
	public String getFilepath() {
		return filepath;
	}
	public void setFilepath(String tablename) {
		Vector<String> s = getColName();
		String x = "";
		for (int i = 0; i < s.size(); i++) {
			x += s.get(i);
		}
		filepath = "src/main/resources/" + tablename + x + ".ser";
		// this.filepath = filepath;
	}
	public Vector<String> getColName() {
		Vector<String> s = new Vector<String>();
		for (int i = 0; i < columns.length; i++) {
			s.add(columns[i].getName());
		}
		if (s.size() == 0) {
			return null;
		}
		// Collections.sort(s);
		return s;
	}
	public Vector<Double> getPer(column c) {
		Double[] ranges = c.ranges;
		int jump = c.jumps;
		Vector<Double> per = new Vector<Double>();
		Double curr = 0.0;
		for (int i = 0; i < ranges.length; i++) {
			curr = ranges[i];
			int k = 0;
			for (int j = 0; j < jump; j++) {
				per.add(curr + k);
				if (k == 0) {
					k++;
				}
			}
		}
		return per;
	}
	public Double[][] rangesarr(column[] co) { // to returm array of ranges according to the array of columns el dakhly
		String[] y = new String[10];
		Double[][] rd = new Double[co.length][];
		for (int i = 0; i < co.length; i++) {
			rd[i] = co[i].ranges;
		}
		for (int i = 0; i < rd.length; i++) {
			// indx.put(Arrays.toString(ind[i]), i);
			String z = Arrays.toString(rd[i]);
			y[i] = z;
		}
		return rd;
	}
	public Double[] getVal(column c, String operator, Object val) {// returns the correct ranges(not all)-matching the
																	// operator
		Vector<Double> e = new Vector<Double>();
		int j = c.getJumps();
		Hashtable<String, Object> h = new Hashtable<String, Object>();
		h.put(c.getName(), val);
		if (val instanceof String) {
			h.replace(c.getName(), ascii((String) val));
		} // date too
		else if (val instanceof Integer) {
			h.replace(c.getName(), (double) (int) val);
		}
		// elsif date
		int x = (int) ((Double) h.get(c.getName()) - c.getMin()) / j;//
		Double[] ranges = c.getRanges();
		double r = ranges[x];
		switch (operator) {
		case "=":
			e.add(r);
			break;
		case ">":
		case ">=":
			for (int i = x; i < ranges.length; i++) {
				e.add(ranges[i]);
			}
			break;
		case "<":
		case "<=":
			for (int i = 0; i < x + 1; i++) {
				e.add(ranges[i]);
			}
			break;
		case "!=":
			for (int i = 0; i < ranges.length; i++) {
				e.add(ranges[i]);
			}
			break;
		default:
			break;
		}
		Double[] array = e.toArray(new Double[e.size()]);
		return array;
	}
	public ArrayList<bucket> select(Vector<SQLTerm> cols, Vector<String> op) {
		// Double[][][]r =new Double[cols.size()][columns.length][10];
		String[][] r = new String[cols.size()][];
		for (int i = 0; i < cols.size(); i++) {
			SQLTerm t = cols.get(i);
			String name = t.getColumnName();
			String opr = t.getOperator();
			Object v = t.getValue();
			column c = null;
			for (int j = 0; j < columns.length; j++) {
				if (columns[j].getName().equals(name)) {
					c = columns[j];
					break;
				}
			}
			r[i] = r(c, columns, opr, v);
		}
		String[] s = performO(op, r);// return buckets
		if(s==null||s.length==0) {
			return null;
		}
		ArrayList<bucket> b = new ArrayList<bucket>();
		for (int i = 0; i < s.length; i++) {
			int j = indx.get(s[i]);
			b.add(buckets[j]);
		}
		return b;
	}
	public String[] performO(Vector<String> op, String[][] r) {
		Vector<String[]> s = new Vector<String[]>();
		Collections.addAll(s, r);
		while (op != null && op.size() != 0) {
			while (op.contains("AND")) {
				int ind = op.indexOf("AND");
				// Vector<Hashtable> h = new Vector<Hashtable>();
				// h = op(s.get(ind), s.get(ind + 1), k, t.getPages(), "AND");
				String[] v = and(r[ind], r[ind + 1]);
				op.remove(ind);
				s.remove(ind + 1);
				s.remove(ind);
				s.add(ind, v);
			}
			while (op.contains("OR")) {
				int ind = op.indexOf("OR");
				String[] v = or(r[ind], r[ind + 1]);
				op.remove(ind);
				s.remove(ind + 1);
				s.remove(ind);
				s.add(ind, v);
			}
			while (op.contains("XOR")) {
				int ind = op.indexOf("XOR");
				String[] v = xor(r[ind], r[ind + 1]);
				op.remove(ind);
				s.remove(ind + 1);
				s.remove(ind);
				s.add(ind, v);
			}
		}
		if (s.size() != 1) {
			return null;
		}
		return s.get(0);
	}
	public String[] and(String[] a, String[] b) {
		Vector<String> r = new Vector<String>();
		Vector<String> c = new Vector<String>();
		Vector<String> d = new Vector<String>();
		Collections.addAll(c, a);
		Collections.addAll(d, b);
		for (int i = 0; i < c.size(); i++) {
			if (d.contains(c.get(i)))
				r.add(c.get(i));
		}
		String[] array = r.toArray(new String[r.size()]);
		return array;
	}
	public String[] or(String[] a, String[] b) {
		// Vector<String>r = new Vector<String>();
		Vector<String> c = new Vector<String>();
		Vector<String> d = new Vector<String>();
		Collections.addAll(c, a);
		Collections.addAll(d, b);
		for (int i = 0; i < d.size(); i++) {
			if (!c.contains(d.get(i)))
				c.add(d.get(i));
		}
		String[] array = c.toArray(new String[c.size()]);
		return array;
	}
	public String[] xor(String[] a, String[] b) {
		Vector<String> rc = new Vector<String>();// contains all the common fields of a and b
		String[] r = and(a, b);
		Collections.addAll(rc, r);
		Vector<String> c = new Vector<String>();
		Vector<String> d = new Vector<String>();
		Collections.addAll(c, a);
		Collections.addAll(d, b);
		c.removeAll(rc);
		d.removeAll(rc);
		c.addAll(d);
		String[] array = c.toArray(new String[c.size()]);
		return array;
	}
	public String[] r(column c, column[] co, String operator, Object val) {
		Double[][] r = rangesarr(co);
		int x = -1;
		for (int i = 0; i < co.length; i++) {
			if (c.equals(co[i])) {
				x = i;
				break;
			}
		}
		r[x] = getVal(c, operator, val);
		String[] p = perm(r, co.length);
		return p;
	}
	public static String[] perm(Double[][] r2, int nuofcol) // hena badkhlaha array of ranges (array containing ranges of
	{
		int []a = new int[nuofcol];//array of multiplication of sizes, {6,3,1};last is always 1
		for(int i=0;i<r2.length;i++) {
			int l=1;
			for(int j=i+1;j<r2.length;j++) {
				l*=r2[j].length;
			}
			a[i]=l;
		}
		int []b = new int[nuofcol];//array of multiplication of sizes, {6,3,1};last is always 1
		for(int i=0;i<r2.length;i++) {
			int l=1;
			if(i!=0) {
				for(int j=0;j<i;j++) {
					l*=r2[j].length;
				}
			}
			b[i]=l;
		}
		boolean f=false;
		int x = 1;
		for(int i=0;i<r2.length;i++) {
			x*=r2[i].length;
		}
		String []z = new String[x];
		Object[][] ind = new Object[x][r2.length];// here we will store the cells
	for (int i = 0; i < r2.length; i++) {
		int index = 0;
		int r = b[i];
		for (int m = 0; m < r; m++) {
			for (int j = 0; j < r2[i].length; j++) {
				int rep = a[i];
				for (int k = 0; k < rep; k++) {
					if (index==x) {
						f=true;
						break;
						}
					ind[index][i] = r2[i][j];
					index++;
				}
				if(f) {
					f=false;
					break;
				}
			}
		}
	}
	for (int i = 0; i < ind.length; i++) {
		z[i]=Arrays.toString(ind[i]);//each key
	}
	return z;
	}
	public void serialize(bucket[] buckets) {
		Vector<bucket> c = new Vector<bucket>();
		Collections.addAll(c, buckets);
		FileOutputStream fileOut;
		try {
			fileOut = new FileOutputStream(filepath);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(c);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public String checknum(String s,String colName) {
		int j=0;
		 for(int i=0;i<columns.length;i++) {
			if( columns[i].getName().equals(colName)){
				j=((String)columns[i].minimum()).length();
			}
		 }
		try {
		       Integer.parseInt(s);
		       if(s.length()!=j) {
		    	 while(s.length()<j) {
		    		 s="0"+s;
		    	 } }
		       }
		    catch (NumberFormatException nfe) {
	}     return s;
	}
	public boolean isPrimary(String clust) {
		for (int i = 0; i < columns.length; i++) {
			if (this.columns[i].getName().equals(clust)) {
				return true;
			}
		}
		return false;
	}
	public Vector<bucket> deserialize() {
		FileInputStream fileIn;
		Vector v = new Vector();
		try {
			fileIn = new FileInputStream(filepath);
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
	public Vector<String> getColNames() {
		return colNames;
	}
	public void setColNames(Vector<String> colNames) {
		this.colNames = colNames;
	}
}
