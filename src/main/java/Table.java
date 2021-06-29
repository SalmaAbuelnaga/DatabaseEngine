import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

public class Table implements java.io.Serializable{

String name;
int noOfRec;
String key;
static String csvUrl = "src/main/resources/metadata.csv";
Vector<page>pages;//=new Vector<page>();
int counter;
Vector<column>col;
String indexPath;
Vector<grid>grids;
grid primary;
public Vector<grid> getGrids() {
	return grids;
}
public void setGrids(Vector<grid> grids) {
	this.grids = grids;
}
public Table() {
	super();
	// TODO Auto-generated constructor stub
}
public Vector<page> getPages() {
	return pages;
}
public void setPages(Vector<page> pages) {
	this.pages = pages;
}
public void setKey(String key) {
	this.key = key;
}
public Table(String name, String key) {
	super();
	this.name = name;
	this.key = key;
	counter =-1;
	pages= new Vector<page>();
	grids = new Vector<grid>();
//	setCol();
	
}
public Vector<column> getCol() {
	return col;
}
public void setCol(Vector<column> col) {
	this.col = col;
}
public String getName() {
	return name;
}
public void setName(String name) {
	this.name = name;
}

public int getNoOfRec() {
	return noOfRec;
}
public void setNoOfRec(int noOfRec) {
	this.noOfRec = noOfRec;
}
public String getKey() {
	return key;
}
public int getCounter() {
	return counter;
}
public void setCounter(int counter) {
	this.counter = counter;
}

public void setCol() {
	ArrayList a =readFile();
	col =new Vector<column>();
	for (int i = 0; i < a.size(); i++) {
		String[] sl = (String[]) a.get(i); // a =l
		if (sl[0].compareTo(name) == 0) {
			column c =new column(name,sl[1]);
			col.add(c);
		}
	}
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
	public Vector<column> getC(Vector<String>c){
		Vector<column> x = new Vector<column>();
		for(int i=0;i<col.size();i++) {
			String na=col.get(i).getName();
			if (c.contains(na)){
				x.add(col.get(i));
			}
		}
		return x;
	}
	public grid getPrimary() {
		return primary;
	}
	public void setPrimary(grid primary) {
		this.primary = primary;
	}
	
}
