import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Set;
public class column implements java.io.Serializable {
	/**
	* 
	*/
	private static final long serialVersionUID = 1L;
	static String csvUrl = "src/main/resources/metadata.csv";
	public static String getCsvUrl() {
		return csvUrl;
	}
	public static void setCsvUrl(String csvUrl) {
		column.csvUrl = csvUrl;
	}
	public double getMin() {
		return min;
	}
	public void setMin(double min) {
		this.min = min;
	}
	public double getMax() {
		return max;
	}
	public void setMax(double max) {
		this.max = max;
	}
	public Double[] getRanges() {
		return ranges;
	}
	public void setRanges(Double[] ranges) {
		this.ranges = ranges;
	}
	public int getJumps() {
		return jumps;
	}
	public void setJumps(int jumps) {
		this.jumps = jumps;
	}
	static String table;
 String name;
	Double min;
	Double max;
	Double[] ranges = new Double[10];
	int jumps;
	public column(String tablename, String n) {
		name = n;
		table = tablename;
		Object mino = minimum();
		Object maxo = maximum();
		if (mino instanceof String) {
			min = (double) ascii((String) mino);
			max = (double) ascii((String) maxo);
		} else if(mino instanceof Integer){
			min = (double) (int)mino;
			max = (double) (int)maxo;
		}
		else if(mino instanceof Date){
			min = convertD((Date)mino);
			max = convertD((Date)maxo);
		}
		else
		{
			min =(double) mino;
			max =(double)maxo;
		}
		jumps = getnumofValues();
		showMins();
	}
	@SuppressWarnings("deprecation")
	/*public static Hashtable<String,Double> compareT(Date f, Date d) {
		Double diff=0.0;
		Boolean month =false;
		Boolean day = false;
		Boolean year=false;
		Hashtable h = new Hashtable<String,Double>();
		if(d.getYear()==f.getYear()) {
			if(d.getMonth()>f.getMonth()) {
				diff = (double) (d.getMonth()-f.getMonth()) ;
				month=true;
		//range will be with month
		}
			else if (f.getMonth()>d.getMonth()) {
				diff = (double) (f.getMonth()-d.getMonth()) ;
				month=true;
		}
			else { // month and year are equal check days
				if(d.getDay()>f.getDay()) {
					diff = (double) (d.getDay()-f.getDay()) ;
					day=true;
		//range will be with days
		}
				else if (f.getDay()>d.getDay()) {
					diff = (double) (f.getDay()-d.getDay()) ;
					day=true;
		}
		}
		}
		//d.getYear()-f.getYear()<8 ||f.getYear()-d.getYear()<8 
		else if (y(d.getYear(),f.getYear())<8) { //check month alatool
		  if(y(d.getYear(),f.getYear() )<= 1 ) {
			  diff=12.0;
			  month=true;
		      }
		else { //more than 1 year difference
			
			if(d.getYear()>f.getYear()){
				int dyear = d.getYear()-f.getYear();
				int yearM = dyear*12;
				int sm =12-f.getMonth();
				int lg = d.getMonth();
				month=true;
				diff = (double)(yearM-(sm+lg));
			}
			else if(f.getYear()>d.getYear()){
				int dyear = f.getYear()-d.getYear();
				int yearM = dyear*12;
				int sm =12-d.getMonth();
				int lg = f.getMonth();
				month=true;
				diff = (double)(yearM-(sm+lg));
			}
			else if(d.getMonth()>f.getMonth()) {
				
				diff = (double) (d.getMonth()-f.getMonth()) ;
				month=true;
		//range will be with month
			}
			else if (f.getMonth()>d.getMonth()) {
				diff = (double) (f.getMonth()-d.getMonth()) ;
					month=true;
			}
			else { // month and year are equal check days
				if(d.getDay()>f.getDay()) {
					diff = (double) (d.getDay()-f.getDay()) ;
					day=true;
		//range will be with days
			}
				else if (f.getDay()>d.getDay()) {
					diff = (double) (f.getDay()-d.getDay()) ;
					day=true;
				}
				}
			}
		}
		else {//hena diff years > 8 fa divide ranges according to year
		if(d.getYear()>f.getYear()) {
		diff = (double) (d.getYear()-f.getYear()) ;
		year=true;
		//range will be with month
		}
		else if (f.getYear()>d.getYear()) {
		diff = (double) (f.getYear()-d.getYear()) ;
		year=true;
		}
		}
		if(day) {
			h.put("day", diff);
		}
		if(month) {
			h.put("month", diff);
		}
		if(year) {
			h.put("year", diff);
		}
		return h;
		}
		public static int y(int x,int y) {
			if(y>x)
				return y-x;
			else
				return x-y;
			
		}
		public String g(Date a, Date b) {
			Hashtable<String,Double> c = compareT(a,b);
			Set<String> st = c.keySet();
			String[]d =new String[1];
			st.toArray(d);
			
			int x = (int) (((max - min) + 1) / 10);
			Double y = ((max - min) + 1);
			if (y % 10 != 0) {
				x = x + 1;
			}
			return null;
		}
		public void getminmax(Hashtable<String,Double>h) {
		Date mn= (Date)minimum();
		Date mx=(Date)maximum();
		if(h.containsKey("year")) {
		  min=(double) mn.getYear();
		  max=(double) mx.getYear();
		}
		if(h.containsKey("month")) {
		   min=(double) mn.getMonth();
		max=(double) mx.getMonth();
		}
		if(h.containsKey("year")) {
		min=(double) mn.getDay();
		max=(double) mx.getDay();
		}
		}*/
	public static Double ascii(String name) {
		// String to check it's value
		int nameLenght = name.length(); // length of the string used for the loop
		Double ascii = (double) 0;
		for (int i = 0; i < nameLenght; i++) { // while counting characters if less than the length add one
			char character = name.charAt(i); // start on the first character
			ascii = ascii + (int) character; // convert the first character
			// print the character and it's value in ascii
		}
		return ascii;
	}
	public Object minimum() {
		Hashtable<String, String> h = getType();
		return convertSt(h.get("Min"), h.get("Type"));
	}
	public Object maximum() {
		Hashtable<String, String> h = getType();
		return convertSt(h.get("Max"), h.get("Type"));
	}
	public static double compare(Object min, Object value) {
		if (!min.getClass().equals(value)) {
			// throw new DBAppException();
		}
		if (min instanceof String) {
			return ((String) value).compareTo((String) min);
		} else if (min instanceof Date) {
			return ((Date) value).compareTo((Date) min);
		} else if (min instanceof Double) {
			return ((Double) value - (Double) min);
		} else {
			return (int) value - (int) min;
		}
	}
	
	public void showMins() {
		int i = 0;
		double m = min;
		// ranges[0]=m;
		int x = getnumofValues();
		while (m <= max) {
			ranges[i] = m;
			m = m + x;
			i++;
		}
	}
	public int getnumofValues() {
		int x = (int) (((max - min) + 1) / 10);
		Double y = ((max - min) + 1);
		if (y % 10 != 0) {
			x = x + 1;
		}
		//jump=x;
		return x;
	}
	public Object convertSt(String st, String type) {
		type = type.toLowerCase();
		if (type.contains("Integer".toLowerCase()))// changed from equals java.util.lang to contains each
			return Integer.parseInt(st);
		else if (type.contains("Double".toLowerCase()))
			return Double.parseDouble(st);
		else if (type.contains("Date".toLowerCase())) {
			return Date.valueOf(st);
		}
		return st;
	}
	public ArrayList readFile() {
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
	public Hashtable<String, String> getType() {
		Hashtable<String, String> h = new Hashtable<String, String>();
		ArrayList a = readFile();
		for (int i = 0; i < a.size(); i++) {
			String[] b = (String[]) a.get(i); // a =l
			if (b[0].compareTo(table) == 0 && b[1].equalsIgnoreCase(name)) {
				h.put("Type", b[2]);
				h.put("Min", b[5]);
				h.put("Max", b[6]);
			}
		}
		return h;
	}
	//public double CompT(date a1, date a2) {
		
	//}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		column.table = table;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
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
	public static void main(String[]args) {
		
		Date d = new Date(2020 , 06, 23);
		double a = d.getYear();
		double b = d.getMonth();
		double c = d.getDay();
		Date dl = new Date(2018, 12, 05);
		double a1 = dl.getYear();
		double b1 = dl.getMonth();
		double c1 = dl.getDay();
		double z = a*365+b*30+c;
		double zx = a1*365+b1*30+c1;
		double r = z-zx;
		System.out.println(r);
		double ad = (a-a1)*365+(b-b1)*30+c-c1;
		//System.out.println(a-a1);
		//System.out.println(b-b1);
		//System.out.println(c-c1);
		//System.out.println(ad);
		/*String r = d.toString();
		if (b.length()<2)
			b= '0'+b;
		if (c.length()<2)
			c= '0'+c;
		String z = a+""+b+""+c;
		z+=".0";
		//System.out.println(z);
		double f = ascii(z);
		System.out.println(f);
		/*double q = Double.parseDouble(z);
		/*double q = ascii(z);
		System.out.println(q);
		Date dl = new Date(2018, 12, 05);
		String e = dl.getMonth()+"";
		String f = dl.getDay()+"";
		if (e.length()<2)
			e= '0'+e;
		if (f.length()<2)
			f= '0'+f;
		String y = dl.getYear()+""+e+""+f;
		System.out.println(y);
		double q1 = ascii(y);
		System.out.println(q1);
		Date dlf = new Date(2019, 1, 6);
		String ef = dlf.getMonth()+"";
		String ff = dlf.getDay()+"";
		if (ef.length()<2)
			ef= '0'+ef;
		if (ff.length()<2)
			ff= '0'+f;
		String yf = dl.getYear()+""+ef+""+ff;
		System.out.println(yf);
		double q1f = ascii(yf);
		System.out.println(q1f);
		System.out.println((double)d.getDate());
		System.out.println(dl.getDate());
		System.out.println(dlf.getDate());
		/*
		
		//System.out.println(a);
		//System.out.println(b);
		//System.out.println(c);
		
		System.out.println(z);
		//int q =Integer.parseInt(z);
		double q = Double.parseDouble(z);
		System.out.println(q);
		//double dnum = q;  
		
		System.out.println(q);*/
		
	}
	
}