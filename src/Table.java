import java.util.*;

public class Table {
	
	//Creating a ArrayList to save the primary key of the table
	private ArrayList<String> pKey = new ArrayList<String>();
	
	//Creating a ArrayList to save the non-primary attributes of the table
	private ArrayList<String> attributes = new ArrayList<String>();
		
	//Creating a String to hold the table name
	private String tableName;
	
	//Creating a ArrayList to contain all the dependencies RHS in the table
	private ArrayList<List<String>> depLHS = new ArrayList<List<String>>();
	
	//Creating a ArrayList to contain all the dependencies LHS in the table
	private ArrayList<String> depRHS = new ArrayList<String>();
	
	//Creating a LHS ArrayList to contain new aggregated dependencies
	private ArrayList<List<String>> depLHS_temp3 = new ArrayList<List<String>>();
	
	//Creating a RHS ArrayList to contain new aggregated dependencies
	private ArrayList<Set<String>> depRHS_temp3 = new ArrayList<Set<String>>();
	
	public ArrayList<Set<String>> getDepRHS_temp3() {
		return depRHS_temp3;
	}

	public void setDepRHS_temp3(ArrayList<Set<String>> depRHS_temp3) {
		this.depRHS_temp3 = depRHS_temp3;
	}

	public ArrayList<List<String>> getDepLHS_temp3() {
		return depLHS_temp3;
	}

	public void setDepLHS_temp3(ArrayList<List<String>> depLHS_temp3) {
		this.depLHS_temp3 = depLHS_temp3;
	}

	
	public ArrayList<List<String>> getDepLHS() {
		return depLHS;
	}

	public void setDepLHS(ArrayList<List<String>> depLHS) {
		this.depLHS = depLHS;
	}

	public ArrayList<String> getDepRHS() {
		return depRHS;
	}

	public void setDepRHS(ArrayList<String> depRHS) {
		this.depRHS = depRHS;
	}

	public ArrayList<String> getpKey() {
		return pKey;
	}

	public void setpKey(ArrayList<String> pKey) {
		this.pKey = pKey;
	}

	public ArrayList<String> getAttributes() {
		return attributes;
	}

	public void setAttributes(ArrayList<String> attributes) {
		this.attributes = attributes;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}	
}
