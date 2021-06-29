import java.beans.Transient;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class page implements Serializable {
String table;
int pageno;
Hashtable first;
 Hashtable last;
int noOfRe;
page prev;
page next;
Vector <page>overflow;
public String getTable() {
	return table;
}
public void setTable(String table) {
	this.table = table;
}
public int getPageno() {
	return pageno;
}
public void setPageno(int pageno) {
	this.pageno = pageno;
}
public int getNoOfRe() {
	return noOfRe;
}
public void setNoOfRe(int noOfRe) {
	this.noOfRe = noOfRe;
}

public page getPrev() {
	return prev;
}
public void setPrev(page prev) {
	this.prev = prev;
}
public Hashtable getFirst() {
	return first;
}
public void setFirst(Hashtable first) {
	this.first = first;
}
public Hashtable getLast() {
	return last;
}
public void setLast(Hashtable last) {
	this.last = last;
}
String filepath;
public String getFilepath() {
	return filepath;
}
public void setFilepath(String filepath) {
	this.filepath = filepath;
}
public page (String t,int pageNumber) {
	table=t;
	pageno=pageNumber;
	overflow = new Vector();
	
	
}
public page getNext() {
	return next;
}
public void setNext(page next) {
	this.next = next;
}
public Vector getOverflow() {
	return overflow;
}
public void setOverflow(Vector overflow) {
	this.overflow = overflow;
}

}
