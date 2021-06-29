import java.util.Hashtable;

public class bRecord implements java.io.Serializable{
	page page;
	Hashtable record;

	public bRecord(page p, Hashtable r) {
		page = p;
		record = r;
	}

	public page getPage() {
		return page;
	}

	public void setPage(page page) {
		this.page = page;
	}

	public Hashtable getRecord() {
		return record;
	}

	public void setRecord(Hashtable record) {
		this.record = record;
	}

}