import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class bucket implements java.io.Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String Config = "src/main/resources/DBApp.config";
	String index;
	String filepath;
	int max;
	Vector<bucket> overflow;
	int records;
	boolean isOverflow;
	bucket parent;
	bRecord first;
	bRecord last;

	public bucket() {

	}

	public bucket(boolean o) {
		// filepath = bucketName(key,tableName, index);
		index = "";
		isOverflow = o;
		if(!o)
			overflow = new Vector<bucket>();
		records = 0;
		max = maxData();
		filepath = null;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}
	public void setFilepath(String path) {
		filepath = path;
	}
	public String getFilepath() {
		return filepath;
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
	
	public void setFilepath(grid g, String tableName) {
		Vector<String> r = g.getColName();
		for (int i = 0; i < r.size(); i++) {
			index += r.get(i);
		}
		this.filepath = bucketName(g.getBc(), tableName);
		g.setBc(g.getBc() + 1);
		File myObj = new File(filepath);
	    try {
			myObj.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Vector<bucket> getOverflow() {
		return overflow;
	}

	public void setOverflow(Vector<bucket> overflow) {
		for (int i = 0; i < overflow.size(); i++) {
			overflow.get(i).setParent(this);
			overflow.get(i).setOverflow(true);
		}
		this.overflow = overflow;
		isOverflow = false;
	}

	public int getRecords() {
		return records;
	}

	public void setRecords(int records) {
		this.records = records;
	}

	public String bucketName(int c, String tableName) {// tablenameKey,indexnumber or name
		String x = "src/main/resources/data/" + tableName + index + c + ".ser";
		return x;
	}

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

		return Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));

	}

	public void serialize(Vector<bRecord> b) {
		FileOutputStream fileOut;
		try {
			fileOut = new FileOutputStream(filepath);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(b);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void insert(bRecord b, grid g, String tableName) {
		if (records < max) {
			
			Vector<bRecord> v = new Vector<bRecord>();
			if (isEmpty()) {
				setFilepath(g, tableName);
			} else {
				v = deserialize();
			}
			v.add(b);

			serialize(v);
			records++;
		} else {
			if (!isOverflow) {
				if (this.overflow.size() == 0 || this.overflow == null) {
					bucket bu = new bucket(true);
					bu.insert(b, g, tableName);
					this.overflow.add(bu);
					bu.parent = this;

				} else {
					this.getOverflow().lastElement().insert(b, g, tableName);
				}
			} else {
				bucket bc = new bucket(true);
				bc.insert(b, g, tableName);
				parent.overflow.add(bc);

			}
		}
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
				// System.out.println(h1.get(pk));
				// System.out.println("mmmm");
				// System.out.println(h2.get(pk));
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
					// h1=h2;
					// h2=temp;
					keys = temp.keySet();
					for (String key : keys) {
						h2.put(key, temp.get(key));
					}
					// h1.replace(pk, h1.get(pk),h2.get(pk));

					// h2.replace(pk, h2.get(pk),temp);

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
					// h1=h2;
					// h2=temp;
					/*
					 * Object temp = h1.get(pk); h1.replace(pk, h1.get(pk),h2.get(pk));
					 * h2.replace(pk, h2.get(pk),temp);
					 */
					swapped = true;
				}
			}
			start = start + 1;
		}

		return (my_arr);
	}

	public Vector<bRecord> allelements() {
		if(isEmpty()) {
			System.out.println("empty");
			return new Vector<bRecord>();
		}
		File f = new File(filepath);
		// System.out.println(f.length());
		if (f.length() == 0) {
			
			return new Vector<bRecord>();
		}
		Vector<bRecord> v = deserial();
		if (getOverflow()==null||this.getOverflow().isEmpty()) {
		} else {
			for (int i = 0; i < this.getOverflow().size(); i++) {
				v.addAll(this.getOverflow().get(i).deserial());

			}
		}
		return v;
	}

	public Vector<bRecord> bucketSort(String pk) {
		Vector<bRecord> sorted = new Vector<bRecord>();

		Vector<bRecord> v = allelements();
		Vector<Hashtable> vrec = new Vector<Hashtable>();
		for (int i = 0; i < v.size(); i++) {
			vrec.add((v.get(i).getRecord()));
		}
		Vector vrecsort = BSort(pk, vrec);
		for (int i = 0; i < vrecsort.size(); i++) {
			for (int j = 0; j < v.size(); j++) {
				if (vrecsort.get(i) == v.get(j).getRecord()) {
					sorted.add(i, v.get(j));
					break;
				}
			}
		}
		return sorted;
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

	public void delete(bRecord b, String pk) {
		Vector<bRecord> v = this.allelements();
		v.remove(b);
		v = bucketSort(pk);
		Vector<bucket> buckets = new Vector<bucket>();
		buckets.add(this);

		if (v.size() <= (max * (this.getOverflow().size()))) {

			this.getOverflow().remove(this.getOverflow().size() - 1);
			// delete file path---------------------
		}
		if (this.getOverflow() != null && this.getOverflow().size() != 0) {
			buckets.addAll(getOverflow());
		}

		int i = 0;
		while (i < v.size()) {

			for (int j = 0; j < buckets.size(); i++) {
				buckets.get(j).setRecords(0);
				Vector<bRecord> vb = buckets.get(j).deserial();
				while (vb.size() < max) {
					buckets.get(j).setRecords(buckets.get(j).getRecords() + 1);
					vb.add(v.get(i));
					serialize(vb);
					i++;
				}

			}
		}
	}

	/*public void insert(bRecord b, String pk, String tablename,grid g) throws DBAppException {
		Vector<bRecord> v = new Vector<bRecord>();
		if(isEmpty()) {
			setFilepath(g, tablename);
		}
		else
		{
			v = this.allelements();
		}
		for (int i = 0; i < v.size(); i++) {
			if (bigger(v.get(i).getRecord().get(pk), b.getRecord().get(pk)) == false
					&& (bigger(b.getRecord().get(pk), v.get(i).getRecord().get(pk))) == false) {
			//if (bigger(v.get(i).getKey(pk), b.getKey(pk)) == false
			//		&& (bigger(b.getKey(pk), v.get(i).getKey(pk))) == false) {
				throw new DBAppException();
			}

		}
		v.add(b);
		v = bucketSort(pk);
		serialize(v);
		//
		if (v.size() > (max * (1 + this.getOverflow().size()))) {
			bucket buck = new bucket(true);
			this.getOverflow().add(buck);
			buck.setFilepath(g, tablename);

		}
		Vector<bucket> buckets = new Vector<bucket>();
		buckets.add(this);
		if (this.getOverflow() != null && this.getOverflow().size() != 0) {
			buckets.addAll(getOverflow());
		}

		int i = 0;
		while (i < v.size()) {
			for (int j = 0; j < buckets.size(); i++) {
				Vector<bRecord> vb = buckets.get(j).deserial();
				while (vb.size() < max) {
					vb.add(v.get(i));

					i++;
				}
				serialize(vb);
				buckets.get(j).setFirst(vb.firstElement());
				buckets.get(j).setLast(vb.lastElement());

			}
		}
	}*/

	public page getNxtElpage(bRecord b, String clust) {// after insertion in bucket

		Vector<bucket> v = new Vector();
		v.add(this);
		if (this.getOverflow() != null && this.getOverflow().size() != 0) {
			v.addAll(this.getOverflow());
		}

		for (int i = 0; i < v.size(); i++) {

			if (bigger(v.get(i).getLast().getRecord().get(clust), b.getRecord().get(clust))
					&& bigger(b.getRecord().get(clust), v.get(i).first.getRecord().get(clust))) {
				b = v.get(i).deserial().get(v.indexOf(b) + 1);
				return b.getPage();
			} else if (v.get(i).getFirst().getRecord().get(clust).equals(b.getRecord().get(clust))) {
				if (!v.get(i).getFirst().equals(v.get(i).getLast())) {
					b = v.get(i).deserial().get(v.indexOf(b) + 1);
					return b.getPage();
				}
			}
			if (v.get(i).getLast().getRecord().get(clust).equals(b.getRecord().get(clust))) {
				if (!v.get(i).equals(v.lastElement())) {
					b = v.get(i + 1).getFirst();
					return b.getPage();
				}

			}

		}
		return null;
	}

	public page getprevElpage(bRecord b, String clust) {// after insertion in bucket

		Vector<bucket> v = new Vector();
		v.add(this);
		if (this.getOverflow() != null && this.getOverflow().size() != 0) {
			v.addAll(this.getOverflow());
		}

		for (int i = 0; i < v.size(); i++) {

			if (bigger(v.get(i).getLast().getRecord().get(clust), b.getRecord().get(clust))
					&& bigger(b.getRecord().get(clust), v.get(i).first.getRecord().get(clust))) {
				b = v.get(i).deserial().get(v.indexOf(b) - 1);
				return b.getPage();
			}

			else if (v.get(i).getLast().getRecord().get(clust).equals(b.getRecord().get(clust))) {
				if (!v.get(i).equals(v.firstElement())) {
					b = v.get(i - 1).getFirst();
					return b.getPage();
				}

			}

		}
		return null;
	}

	public page locatePage(bRecord b, String clust, Table table, int maxi) {
		page prev = this.getprevElpage(b, clust);
		page next = this.getNxtElpage(b, clust);
		if (prev.equals(next)) {
			return prev;
		}

		else if (bigger(b.getRecord().get(clust), prev.getFirst()) && bigger(prev.getLast(), b.getRecord().get(clust))) {
			return prev;
		} else if (bigger(b.getRecord().get(clust), next.getFirst()) && bigger(next.getLast(), b.getRecord().get(clust))) {
			return next;
		} else {
			page o = checkO(prev, table.getPages());
			if (o == null) {
				if (prev.noOfRe < maxi) {
					return prev;
				} else if (next.getNoOfRe() < maxi) {
					return next;
				} else {
					page p = new page(table.getName(), 0);
					prev.getOverflow().add(p);
					return p;
				}
			}

			else {
				if (prev.noOfRe < maxi) {
					return prev;
				} else if (next.getNoOfRe() < maxi) {
					return next;
				} else {
					int x = o.getOverflow().indexOf(prev) + 1;
					page p = new page(table.getName(), 0);
					o.getOverflow().add(x + 1, p);
					return p;
				}

			}
		}
	}

	public boolean isOverflow() {
		return isOverflow;
	}

	public void setOverflow(boolean isOverflow) {
		this.isOverflow = isOverflow;
	}

	public bucket getParent() {
		return parent;
	}

	public void setParent(bucket parent) {
		this.parent = parent;
	}

	public Vector<bRecord> deserialize() {
		if(isEmpty()) {
			return new Vector<bRecord>();
		}
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
	public Vector<bRecord> deserial() {
		
		String url = filepath;
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
	public boolean isEmpty() {
		if (filepath == null || filepath.length() == 0) {
			return true;
		}
		return false;

	}
	public String getConfig() {
		return Config;
	}
	public void setConfig(String config) {
		Config = config;
	}
	
	public int getMax() {
		return max;
	}
	public void setMax(int max) {
		this.max = max;
	}
	public bRecord getFirst() {
		return first;
	}

	public void setFirst(bRecord first) {
		this.first = first;
	}

	public bRecord getLast() {
		return last;
	}

	public void setLast(bRecord last) {
		this.last = last;
	}

}
