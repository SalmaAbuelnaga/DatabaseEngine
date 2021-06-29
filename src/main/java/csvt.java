import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
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

public class csvt {
	static String csvUrl = "/Users/salmaamr/Desktop/GUC/Semester 6/DB II/DB Test Run/DB2Project/src/main/resources/metadata.csv";
	String Config = "/DB Test Run/DB2Project/src/main/resources/DBApp.config";
	String tablesUrl = "/DB Test Run/DB2Project/src/main/resources/Tables.ser";
	Vector<Table> tables = new Vector<Table>();
static //Vector tables1=new Vector<Table>();
	BufferedWriter writer;
	static CSVPrinter csvPrinter;
	boolean exists;
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
	public static void updateMeta(String tableName, Vector<String>colnames) {
		ArrayList l = readFile();
		for(int i=0; i<l.size();i++) {
			String[] a = (String[]) l.get(i); // a =l
			if (a[0].compareTo(tableName) == 0&& colnames.contains(a[1])) {
				a[4]="True";
			}
			l.set(i, a);
		}
		try {
			FileWriter fw = new FileWriter(csvUrl,false);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			writer = Files.newBufferedWriter(Paths.get(csvUrl));
			csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Table Name", "Column Name",
					"Column Type", "ClusteringKey", "Indexed", "min", "max"));
			csvPrinter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(int i=1; i<l.size();i++) {
			String[] a = (String[]) l.get(i); // a =l
			tableName = a[0];
			String name = a[1];
			String type = a[2];
			String clust = a[3];
			String index = a[4];
			String min=a[5];
			String max=a[6];
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
	public static void main(String[]args) {
		/*try {
			writer = Files.newBufferedWriter(Paths.get(csvUrl));
			csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Table Name", "Column Name",
					"Column Type", "ClusteringKey", "Indexed", "min", "max"));
			csvPrinter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*
		try {
			FileWriter fw = new FileWriter(csvUrl,false);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/
		
		/*String tableName = "students";
		String name = "age";
		String type = "int";
		String clust = "false";
		String min= "0";
		String max = "100";
		String p = csvUrl;
		BufferedWriter writer;
		try {
		/*	writer = Files.newBufferedWriter(Paths.get(p), StandardOpenOption.APPEND,
					StandardOpenOption.CREATE);
			csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
			csvPrinter.printRecord(tableName, name, type, clust, "false", min, max);
			csvPrinter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		String tableName = "courses";
		Vector<String> str= new Vector<String>();
		str.add("id");
		//str.add("inst");
		updateMeta(tableName, str);
	}
}
