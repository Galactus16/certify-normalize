import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class CertifyNormals {
	private static Statement st = null;
	static LinkedHashSet<List<String>> powerSet = new LinkedHashSet<List<String>>();
	static LinkedHashSet<List<String>> attributeSet = new LinkedHashSet<List<String>>();
	
	
	public static void main(String args[]) {

		//Create files
		OutputWriter.createFile();
		OutputWriter.outputToFile("NF.txt", "TABLE\t\tFORM\t\tCOMPLIES\t\tEXPLANATION");
		// Make an object of the CertifyNormals
		CertifyNormals norm = new CertifyNormals();

		// Read the file
		Reader rd = new Reader();

		// Variable to store the tables returned from Reader class method
		// populateTables
		ArrayList<Table> populatedTables = new ArrayList<Table>();

		// Populating the Tables from the text files
		String fileNamePath=args[0];
		int index=fileNamePath.indexOf("=");
		fileNamePath=fileNamePath.substring(index+1, fileNamePath.length());
		populatedTables = rd.populateTables(fileNamePath);

		// Print the populatedTables
		//norm.showsTables(populatedTables);

		// Getting connection from createConnection Class
		Connection conn = createConnection.createStatement();

		//Object of Decompose
		Decompose decom = new Decompose();
		CertifyNormals cn = new CertifyNormals();
		
		// Normalize the Table one by one
		for (Table tb : populatedTables) {
			try {
				// Getting MetaData from the connection - to get if table exists
				DatabaseMetaData meta = conn.getMetaData();
				// check if "employee" table is there
				ResultSet tbl = meta.getTables(null, null, tb.getTableName(), null);
				//If this tb.getTableName exists in DB then 
				//System.out.println("=================" + tb.getTableName() + "=======================================");
				//System.out.println("PkEY size"+tb.getpKey().size());
				//System.out.println("Attributes size"+tb.getAttributes().size());
								
				if (tbl.next()) {
					if(checkColumns(conn,tb)){
						st = conn.createStatement();	
						//Check if it is in 1st Normal Form or not
						if(firstNormal(tb)){						
							//System.out.println("The table " + tb.getTableName() + " is in 1st normal form.");
							//If it is 1st Normal Form then only we check for 2nd Normal Form
							if(secondNormal(tb)){
								//System.out.println("The table " + tb.getTableName() + " is in 2nd normal form.");
								//If it is in 2nd Noraml Form then only we check for 3rd Normal Form
								if(thirdNormal(tb)){
									//System.out.println("The table " + tb.getTableName() + " is in 3rd normal form.");
									OutputWriter.outputToFile("NF.txt",tb.getTableName()+"\t\t\t3NF\t\t\tY");
								}else{  //In case of table is not in 3rd normal form
									//System.out.println("The table " + tb.getTableName() + " is NOT in 3rd normal form.");
									decom.decompose3NFTable(tb);
									OutputWriter.outputToFile("NF.txt",tb.getTableName()+"\t\t\t3NF\t\t\tN\t\t\t\tnot 3NF,"+cn.printDependencies(tb));
									//if(tb.getDepLHS().size() != 0 || tb.getDepLHS_temp3().size() != 0)//Call decomposeToTable and Verify
								}
							}else{ //In case table is not 2NF
								//System.out.println("The table " + tb.getTableName() + " is NOT in 2nd normal form.");
								decom.decompose3NFTable(tb);
								OutputWriter.outputToFile("NF.txt",tb.getTableName()+"\t\t\t3NF\t\t\tN\t\t\t\tnot 2NF,"+cn.printDependencies(tb));
							}
						}else{ //In case table is not 1NF
							//System.out.println("The table " + tb.getTableName() + " is NOT in 1st normal form.");
							OutputWriter.outputToFile("NF.txt",tb.getTableName()+"\t\t\t3NF\t\t\tN\t\t\t\tnot 1NF, voilates primary key constraint.");
						}					
					
					}else{
						
					}
				} else { //If tb.getTableName doesn't exists
					//System.out.println("The table " + tb.getTableName() + " does NOT exists.");
				}
				//System.out.println("============================================================================");
			} catch (SQLException e) {
				//System.err.println("Could not create statement");
				e.printStackTrace();
				return;
			}
			powerSet.clear();
			
			//Printing the dependencies of each table
			if(tb.getDepLHS().size() != 0 || tb.getDepLHS_temp3().size() != 0){
				cn.printDependencies(tb);
			}
			
			//Call decomposeToTable and Verify
			if(tb.getDepLHS().size() != 0 || tb.getDepLHS_temp3().size() != 0){
				try {
					CertifyNormals.decomposeTableVerify(tb);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					//System.err.println("Could not create statement");
					e.printStackTrace();
				}
			}
		}
	}

	// Function to print the Tables read from the TestSchema file
	private void showsTables(ArrayList<Table> pT) {
		for (Table tb : pT) {
			//System.out.println("================== " + tb.getTableName() + " ===================");
			//System.out.println("Keys are following: " + tb.getpKey());
			//System.out.println("Non-Keys attributes are following: " + tb.getAttributes() + "\n");
		}
	}

	//Function to run SQL query
	private static ResultSet executeQuery(String sqlStatement) {
		try {
			return st.executeQuery(sqlStatement);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	//Method to detect if the Table tb is in 1st normal form or not
	private static boolean firstNormal(Table tb) throws SQLException{
		boolean flag = false;
		String QUERY;
		
		//check if any of the key has NULL value or not
		for(String key : tb.getpKey()){
			//Check if the any of the value is NULL
			String st1,st2;
			st1="SELECT COUNT(*) AS total from cosc6340." + tb.getTableName();
			st2=" WHERE "+ key+ " IS NULL;";
			QUERY=st1+st2;
			//QUERY = "SELECT COUNT(*) AS total from cosc6340." + tb.getTableName() +" WHERE "+ key+ " IS NULL";
			OutputWriter.outputToFile("NF.SQL", st1);	
			OutputWriter.outputToFile("NF.SQL", st2);
			OutputWriter.outputToFile("NF.SQL", "\n");
			ResultSet rs1 = CertifyNormals.executeQuery(QUERY);
			rs1.next();
			//if the total count is 0 - then it means that key doesn't have any NULL values
			if(rs1.getInt("total") == 0){
				//System.out.println(rs1.getInt("total"));
				flag = true;
			}else{//if the total count is not 0 - then it means that key have NULL value
				//System.out.println("There are "+rs1.getInt("total")+"null values in table "+tb.getTableName()+". In key column "+key+".");
				flag = false;
				break;
			}
		}
		//if flag is true means table passed NOT NULL constraints for primary keys
		//so we can check for unique keys constraint
		if(flag){
			//Check for the condition that there are no duplicates in Primary keys
			String keys = tb.getpKey().toString().substring(1, tb.getpKey().toString().length()-1);
			String st1,st2,st3;
			//Get the Distinct count from the table
			st1="SELECT COUNT(*) AS COUNT1 FROM (";
			st2="SELECT DISTINCT "+keys;
			st3=" FROM "+tb.getTableName()+") AS InternalQuery;";
			QUERY=st1+st2+st3;
			OutputWriter.outputToFile("NF.SQL", st1);	
			OutputWriter.outputToFile("NF.SQL", st2);
			OutputWriter.outputToFile("NF.SQL", st3);
			OutputWriter.outputToFile("NF.SQL", "\n");
			//QUERY = "SELECT COUNT(*) AS COUNT1 FROM (SELECT DISTINCT "+keys+" FROM "+tb.getTableName()+") AS InternalQuery";
			//Writer.outputToFile(QUERY);
			ResultSet rs2 = CertifyNormals.executeQuery(QUERY);
			rs2.next();
			int count1 = rs2.getInt("COUNT1");
			
			//Get the total count from the table
			String st4,st5,st6;
			st4="SELECT COUNT(*) AS COUNT2 FROM (";
			st5="SELECT "+keys;
			st6=" FROM "+tb.getTableName()+") AS InternalQuery;";
			QUERY=st4+st5+st6;
			OutputWriter.outputToFile("NF.SQL", st1);	
			OutputWriter.outputToFile("NF.SQL", st2);
			OutputWriter.outputToFile("NF.SQL", st3);
			OutputWriter.outputToFile("NF.SQL", "\n");
			//QUERY = "SELECT COUNT(*) AS COUNT2 FROM (SELECT "+keys+" FROM "+tb.getTableName()+") AS InternalQuery";
			//Writer.outputToFile(QUERY);
			ResultSet rs3 = CertifyNormals.executeQuery(QUERY);
			rs3.next();
			int count2 = rs3.getInt("COUNT2");
			
			//If both the counts are not same - it means there are some duplication in the keys
			if(count1 != count2){
				flag = false;
			}
		}
		return flag;
	}
	
	//Method to detect if the Table tb is in 2nd Normal form or not
	private static boolean secondNormal(Table tb) throws SQLException{
		boolean flag =false;
		String QUERY;
		String dependency = "";
		//System.out.println("Dependency size"+dependency.length());
		
		//build powerset1
		buildPowerSet(tb);
		//System.out.println();
		
		//If the number of primary keys in table is 1 - then it is in 2NF
		if(tb.getpKey().size() == 1){
			flag = true;
		}else{
			//Two alias for the same table name
			String alias1 = tb.getTableName()+"AG1";
			String alias2 = tb.getTableName()+"AG2";
			//All the subsets of keys are in itr
			Iterator<List<String>> itr = powerSet.iterator();
	        while(itr.hasNext()){
	        	List<String> subset = itr.next(); //Each combinations
	            int leng = subset.size();
	            //System.out.println(subset+"\t"+leng);
	            
	            //for combination when leng is 1
	            if(leng == 1){
	            	String key = subset.get(0);
	            	for(String attribute : tb.getAttributes()){
	            		String str1,str2,str3,str4;
	            		str1="SELECT COUNT (*) AS COUNT3 FROM "+tb.getTableName();
	            		str2=" AS "+alias1+", "+tb.getTableName()+" AS "+alias2;
	            		str3=" WHERE (("+alias1+"."+attribute+" != "+alias2+"."+attribute+") AND ("+alias1+"."+key+" = "+alias2+"."+key+")) OR ";
	            		str4="(("+alias1+"."+attribute+"  is NULL) AND ("+alias2+"."+attribute+" is NOT NULL) AND ("+alias1+"."+key+" = "+alias2+"."+key+"));";
	            		QUERY=str1+str2+str3+str4;
	            		//QUERY = "SELECT COUNT (*) AS COUNT3 FROM "+tb.getTableName()+" AS "+alias1+", "+tb.getTableName()+" AS "+alias2+" WHERE (("+alias1+"."+attribute+" != "+alias2+"."+attribute+") AND ("+alias1+"."+key+" = "+alias2+"."+key+")) OR (("+alias1+"."+attribute+"  is NULL) AND ("+alias2+"."+attribute+" is NOT NULL) AND ("+alias1+"."+key+" = "+alias2+"."+key+"))";
	            		//System.out.println(QUERY);
	            		//Writer.outputToFile(QUERY);
	            		OutputWriter.outputToFile("NF.SQL", str1);	
	        			OutputWriter.outputToFile("NF.SQL", str2);
	        			OutputWriter.outputToFile("NF.SQL", str3);
	        			OutputWriter.outputToFile("NF.SQL", str4);
	        			OutputWriter.outputToFile("NF.SQL", "\n");
						ResultSet rs4 = CertifyNormals.executeQuery(QUERY);
						rs4.next();
						int count3 = rs4.getInt("COUNT3");
						if(count3 == 0){
							dependency = dependency.concat(" "+key+" -> "+attribute+";");
							//Add the dependency to the table
							tb.getDepLHS().add(subset);
							tb.getDepRHS().add(attribute);
						}
	            	}
	            }
	            
	          //for combination when leng is 2
	           if(leng == 2){
	            	String key1 = subset.get(0);
	            	String key2 = subset.get(1);
	            	for(String attribute : tb.getAttributes()){
	            		String str1,str2,str3,str4;
	            		str1="SELECT COUNT (*) AS COUNT3 FROM "+tb.getTableName();
	            		str2=" AS "+alias1+", "+tb.getTableName()+" AS "+alias2;
	            		str3=" WHERE (("+alias1+"."+attribute+" != "+alias2+"."+attribute+") AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+")) OR ";
	            		str4="(("+alias1+"."+attribute+"  is NULL) AND ("+alias2+"."+attribute+" is NOT NULL) AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+"));";
	            		QUERY=str1+str2+str3+str4;
	            		//QUERY = "SELECT COUNT (*) AS COUNT3 FROM "+tb.getTableName()+" AS "+alias1+", "+tb.getTableName()+" AS "+alias2+" WHERE (("+alias1+"."+attribute+" != "+alias2+"."+attribute+") AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+")) OR (("+alias1+"."+attribute+"  is NULL) AND ("+alias2+"."+attribute+" is NOT NULL) AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+"))";
	            		//System.out.println(QUERY);
	            		OutputWriter.outputToFile("NF.SQL", str1);	
	        			OutputWriter.outputToFile("NF.SQL", str2);
	        			OutputWriter.outputToFile("NF.SQL", str3);
	        			OutputWriter.outputToFile("NF.SQL", str4);
	        			OutputWriter.outputToFile("NF.SQL", "\n");
	            		//Writer.outputToFile(QUERY);
						ResultSet rs4 = CertifyNormals.executeQuery(QUERY);
						rs4.next();
						int count3 = rs4.getInt("COUNT3");
						if(count3 == 0){
							dependency = dependency.concat(" "+key1+","+key2+" -> "+attribute+";");
							//Add the dependency to the table
							tb.getDepLHS().add(subset);
							tb.getDepRHS().add(attribute);
						}
	            	}
	            }
	            
	          //for combination when leng is 3
	          if(leng == 3){
	            	String key1 = subset.get(0);
	            	String key2 = subset.get(1);
	            	String key3 = subset.get(2);
	            	String q1,q2,q3,q4;
	            	for(String attribute : tb.getAttributes()){
	            		q1="SELECT COUNT (*) AS COUNT3 FROM "+tb.getTableName();
	            		q2=" AS "+alias1+", "+tb.getTableName()+" AS "+alias2;
	            		q3=" WHERE (("+alias1+"."+attribute+" != "+alias2+"."+attribute+") AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+") AND ("+alias1+"."+key3+" = "+alias2+"."+key3+")) OR ";
	            		q4="(("+alias1+"."+attribute+"  is NULL) AND ("+alias2+"."+attribute+" is NOT NULL) AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+") AND ("+alias1+"."+key3+" = "+alias2+"."+key3+"));";
	            		QUERY=q1+q2+q3+q4;
	            		//QUERY = "SELECT COUNT (*) AS COUNT3 FROM "+tb.getTableName()+" AS "+alias1+", "+tb.getTableName()+" AS "+alias2+" WHERE (("+alias1+"."+attribute+" != "+alias2+"."+attribute+") AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+") AND ("+alias1+"."+key3+" = "+alias2+"."+key3+")) OR (("+alias1+"."+attribute+"  is NULL) AND ("+alias2+"."+attribute+" is NOT NULL) AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+") AND ("+alias1+"."+key3+" = "+alias2+"."+key3+"))";
	            		//System.out.println(QUERY);
	            		OutputWriter.outputToFile("NF.SQL", q1);	
	        			OutputWriter.outputToFile("NF.SQL", q2);
	        			OutputWriter.outputToFile("NF.SQL", q3);
	        			OutputWriter.outputToFile("NF.SQL", q4);
	        			OutputWriter.outputToFile("NF.SQL", "\n");
	            		//Writer.outputToFile(QUERY);
						ResultSet rs4 = CertifyNormals.executeQuery(QUERY);
						rs4.next();
						int count3 = rs4.getInt("COUNT3");
						if(count3 == 0){
							dependency = dependency.concat(" "+key1+","+key2+","+key3+" -> "+attribute+";");
							//Add the dependency to the table
							tb.getDepLHS().add(subset);
							tb.getDepRHS().add(attribute);
						}
	            	}
	          }
	          
	        //for combination when leng is 4
	          if(leng == 4){
	            	String key1 = subset.get(0);
	            	String key2 = subset.get(1);
	            	String key3 = subset.get(2);
	            	String key4 = subset.get(3);
	            	String st1,st2,st3,st4;
	            	for(String attribute : tb.getAttributes()){
	            		st1="SELECT COUNT (*) AS COUNT3 FROM "+tb.getTableName();
	            		st2=" AS "+alias1+", "+tb.getTableName()+" AS "+alias2;
	            		st3=" WHERE (("+alias1+"."+attribute+" != "+alias2+"."+attribute+") AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+") AND ("+alias1+"."+key3+" = "+alias2+"."+key3+") AND ("+alias1+"."+key4+" = "+alias2+"."+key4+")) OR ";
	            		st4="(("+alias1+"."+attribute+"  is NULL) AND ("+alias2+"."+attribute+" is NOT NULL) AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+") AND ("+alias1+"."+key3+" = "+alias2+"."+key3+") AND ("+alias1+"."+key4+" = "+alias2+"."+key4+"));";
	            		QUERY=st1+st2+st3+st4;
	            		//QUERY = "SELECT COUNT (*) AS COUNT3 FROM "+tb.getTableName()+" AS "+alias1+", "+tb.getTableName()+" AS "+alias2+" WHERE (("+alias1+"."+attribute+" != "+alias2+"."+attribute+") AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+") AND ("+alias1+"."+key3+" = "+alias2+"."+key3+") AND ("+alias1+"."+key4+" = "+alias2+"."+key4+")) OR (("+alias1+"."+attribute+"  is NULL) AND ("+alias2+"."+attribute+" is NOT NULL) AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+") AND ("+alias1+"."+key3+" = "+alias2+"."+key3+") AND ("+alias1+"."+key4+" = "+alias2+"."+key4+"))";
	            		//System.out.println(QUERY);
	            		OutputWriter.outputToFile("NF.SQL", st1);	
	        			OutputWriter.outputToFile("NF.SQL", st2);
	        			OutputWriter.outputToFile("NF.SQL", st3);
	        			OutputWriter.outputToFile("NF.SQL", st4);
	        			OutputWriter.outputToFile("NF.SQL", "\n");
	            		//Writer.outputToFile(QUERY);
						ResultSet rs4 = CertifyNormals.executeQuery(QUERY);
						rs4.next();
						int count3 = rs4.getInt("COUNT3");
						if(count3 == 0){
							dependency = dependency.concat(" "+key1+","+key2+","+key3+","+key4+" -> "+attribute+";");
							//Add the dependency to the table
							tb.getDepLHS().add(subset);
							tb.getDepRHS().add(attribute);
						}
	            	}
	          }
	        }
	        //System.out.println("Dependency : "+ dependency);
			if(dependency.length() == 0){
				flag = true;
			}else{
				flag = false;
			}
		}
		return flag;
	}
	
	//Method to detect if the Table tb is in 3rd Normal form or not
	private static boolean thirdNormal(Table tb) throws SQLException{
		boolean flag =false;
		String QUERY;
		String dependency = "";
		
		//System.out.println("Dependency size"+dependency.length());
		
		attributeSet.clear();
		buildSet(tb); //Build the attributeSet
		
		//Two alias for the same table name
		String alias1 = tb.getTableName()+"AG1";
		String alias2 = tb.getTableName()+"AG2";
		//All the subsets of keys are in itr
		Iterator<List<String>> itr = attributeSet.iterator();
		while(itr.hasNext()){
        	List<String> subset = itr.next(); //Each combinations
            int leng = subset.size();
            //System.out.println(subset+"\t"+leng);
            
            //for combination when leng is 1
            if(leng == 1){
            	String key = subset.get(0);
            	for(String attribute : tb.getAttributes()){
            		if(key != attribute){
            			String st1,st2,st3,st4;
            			st1="SELECT COUNT (*) AS COUNT3 FROM "+tb.getTableName();
            			st2=" AS "+alias1+", "+tb.getTableName()+" AS "+alias2;
            			st3=" WHERE (("+alias1+"."+attribute+" != "+alias2+"."+attribute+") AND ("+alias1+"."+key+" = "+alias2+"."+key+")) OR ";
            			st4="(("+alias1+"."+attribute+"  is NULL) AND ("+alias2+"."+attribute+" is NOT NULL) AND ("+alias1+"."+key+" = "+alias2+"."+key+"));";
            			QUERY = "SELECT COUNT (*) AS COUNT3 FROM "+tb.getTableName()+" AS "+alias1+", "+tb.getTableName()+" AS "+alias2+" WHERE (("+alias1+"."+attribute+" != "+alias2+"."+attribute+") AND ("+alias1+"."+key+" = "+alias2+"."+key+")) OR (("+alias1+"."+attribute+"  is NULL) AND ("+alias2+"."+attribute+" is NOT NULL) AND ("+alias1+"."+key+" = "+alias2+"."+key+"))";
                		//System.out.println(QUERY);
                		//Writer.outputToFile(QUERY);
                		OutputWriter.outputToFile("NF.SQL", st1);	
	        			OutputWriter.outputToFile("NF.SQL", st2);
	        			OutputWriter.outputToFile("NF.SQL", st3);
	        			OutputWriter.outputToFile("NF.SQL", st4);
	        			OutputWriter.outputToFile("NF.SQL", "\n");
    					ResultSet rs4 = CertifyNormals.executeQuery(QUERY);
    					rs4.next();
    					int count3 = rs4.getInt("COUNT3");
    					if(count3 == 0){
    						dependency = dependency.concat(" "+key+" -> "+attribute+";");
    						//Add the dependency to the table
							tb.getDepLHS().add(subset);
							tb.getDepRHS().add(attribute);
    					}
            		}
            	}
            }
            
            //for combination when leng is 2
           if(leng == 2){
            	String key1 = subset.get(0);
            	String key2 = subset.get(1);
            	for(String attribute : tb.getAttributes()){
            		if((key1 != attribute) && (key2 != attribute)){
            			String st1,st2,st3,st4;
            			st1="SELECT COUNT (*) AS COUNT3 FROM "+tb.getTableName();
            			st2=" AS "+alias1+", "+tb.getTableName()+" AS "+alias2;
            			st3=" WHERE (("+alias1+"."+attribute+" != "+alias2+"."+attribute+") AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+")) OR ";
            			st4="(("+alias1+"."+attribute+"  is NULL) AND ("+alias2+"."+attribute+" is NOT NULL) AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+"));";
            			QUERY=st1+st2+st3+st4;
            			//QUERY = "SELECT COUNT (*) AS COUNT3 FROM "+tb.getTableName()+" AS "+alias1+", "+tb.getTableName()+" AS "+alias2+" WHERE (("+alias1+"."+attribute+" != "+alias2+"."+attribute+") AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+")) OR (("+alias1+"."+attribute+"  is NULL) AND ("+alias2+"."+attribute+" is NOT NULL) AND ("+alias1+"."+key1+" = "+alias2+"."+key1+") AND ("+alias1+"."+key2+" = "+alias2+"."+key2+"))";
                		//System.out.println(QUERY);
                		OutputWriter.outputToFile("NF.SQL", st1);	
	        			OutputWriter.outputToFile("NF.SQL", st2);
	        			OutputWriter.outputToFile("NF.SQL", st3);
	        			OutputWriter.outputToFile("NF.SQL", st4);
	        			OutputWriter.outputToFile("NF.SQL", "\n");
                		//Writer.outputToFile(QUERY);
    					ResultSet rs4 = CertifyNormals.executeQuery(QUERY);
    					rs4.next();
    					int count3 = rs4.getInt("COUNT3");
    					if(count3 == 0){
    						dependency = dependency.concat(" "+key1+","+key2+" -> "+attribute+";");
    						//Add the dependency to the table
							tb.getDepLHS().add(subset);
							tb.getDepRHS().add(attribute);
    					}
            		}
            	}
            }
        }	
		
		//System.out.println("Dependency : "+ dependency);
		if(dependency.length() == 0){
			flag = true;
		}else{
			flag = false;
		}
		
		return flag;
	}
	
	//Recursive method to make powerset from a list
	private static void PowerSet(List<String> list)
	{
	    CertifyNormals.powerSet.add(list);

	    for(int i=0; i<list.size(); i++)
	    {
	        List<String> temp = new ArrayList<String>(list);
	        temp.remove(i);
	        if(! (temp.isEmpty())){ //To remove null set
	        	PowerSet(temp);
	        }	        
	    }
	}
	
	//Method to remove the first element from the powerset which contains all the elements from list
	private static void buildPowerSet(Table tb){
		List<String> mainList = new ArrayList<String>(tb.getpKey()); //Make a powerset of from the list of keys of table tb
		PowerSet(mainList);
		
		//Iterate to remove the first element
		Iterator<List<String>> iterator = powerSet.iterator(); 
		List<String> FirstString = iterator.next(); //First element
		powerSet.remove(FirstString);
		
		//System.out.println(powerSet);
	}

	//Recursive method to make attributeSet from a list
	private static void PowerSet1(List<String> list){
		CertifyNormals.attributeSet.add(list);

	    for(int i=0; i<list.size(); i++)
	    {
	        List<String> temp = new ArrayList<String>(list);
	        temp.remove(i);
	        if(temp.size()<=2 && !(temp.isEmpty())){ //To remove null set
	        	PowerSet1(temp);
	        }	        
	    }
	}
		
	//Method to make attributeSet
	private static void buildSet(Table tb){
		List<String> mainList = new ArrayList<String>(tb.getAttributes()); //Make a powerset of from the list of keys of table tb
		PowerSet1(mainList);
		
		//System.out.println(attributeSet);
	}
	
	//Method to print Dependencies
	private ArrayList<String> printDependencies(Table tb){
		ArrayList<String> deps=new ArrayList<String>();
		//Get the size of the dependency arraylist
		int sz = tb.getDepLHS().size();
		int sz1 = tb.getDepLHS_temp3().size();
		//System.out.println("===Dependencies====");
		String temp;
		for(int i=0; i<sz; i++){
			//System.out.println(tb.getDepLHS().get(i)+" -> "+tb.getDepRHS().get(i));
			temp=tb.getDepLHS().get(i)+" -> "+tb.getDepRHS().get(i);
			deps.add(temp);
		}
		if(sz1 != 0){
			for(int j=0; j<sz1; j++){
				//System.out.println(tb.getDepLHS_temp3().get(j)+" -> "+tb.getDepRHS_temp3().get(j));
				temp=tb.getDepLHS_temp3().get(j)+" -> "+tb.getDepRHS_temp3().get(j);
				deps.add(temp);
			}
		}
		
		return deps;
	}
	
	//Decompose the table, create the decomposed tables in DB, Join them and verify the decomposition.
	public static void decomposeTableVerify(Table tb) throws SQLException{
		
		//Find how many tables you need to create
		int szz1 = tb.getDepLHS().size();
		int szz2 = tb.getDepLHS_temp3().size();
		
		//AL to store all the table names created for this tb while decomposing
		ArrayList<String> createdTables = new ArrayList<String>();
		
		//int numOfTables = szz1 + szz2 + 1;
		
		//Define the createQuery
		String createQUERY="";
		
		for(int i=0; i<szz1; i++){ //Here the table will have only one element in right hand side
			//Get the RHS and LHS
			String right = tb.getDepRHS().get(i);
			String st1,st2,st3;
			List<String> lf = tb.getDepLHS().get(i);
			//Find the number of elements in lHS
			int numOfLeftEle = lf.size();
			
			if(numOfLeftEle == 1){ //if number of elements in LHS is 1
				st1="CREATE TABLE AGC"+tb.getTableName()+i;
				st2=" AS SELECT DISTINCT "+lf.get(0)+", "+right;
				st3=" FROM "+tb.getTableName()+";";
				createQUERY=st1+st2+st3;
				//createQUERY = "CREATE TABLE AGC"+tb.getTableName()+i+" AS SELECT DISTINCT "+lf.get(0)+", "+right+" FROM "+tb.getTableName()+";";
				st.execute(createQUERY);
				OutputWriter.outputToFile("NF.SQL", st1);	
    			OutputWriter.outputToFile("NF.SQL", st2);
    			OutputWriter.outputToFile("NF.SQL", st3);
    			OutputWriter.outputToFile("NF.SQL", "\n");
				createdTables.add("AGC"+tb.getTableName()+i);
				//System.out.println(createQUERY);
			}
			
			if(numOfLeftEle == 2){ //if number of elements in LHS is 2
				st1="CREATE TABLE AGC"+tb.getTableName()+i;
				st2=" AS SELECT DISTINCT "+lf.get(0)+", "+lf.get(1)+", "+right;
				st3=" FROM "+tb.getTableName()+";";
				createQUERY=st1+st2+st3;
				//createQUERY = "CREATE TABLE AGC"+tb.getTableName()+i+" AS SELECT DISTINCT "+lf.get(0)+", "+lf.get(1)+", "+right+" FROM "+tb.getTableName()+";";
				st.execute(createQUERY);
				OutputWriter.outputToFile("NF.SQL", st1);	
    			OutputWriter.outputToFile("NF.SQL", st2);
    			OutputWriter.outputToFile("NF.SQL", st3);
    			OutputWriter.outputToFile("NF.SQL", "\n");
				createdTables.add("AGC"+tb.getTableName()+i);
				//System.out.println(createQUERY);
			}
			
			if(numOfLeftEle == 3){ //if number of elements in LHS is 3
				st1="CREATE TABLE AGC"+tb.getTableName()+i;
				st2=" AS SELECT DISTINCT "+lf.get(0)+", "+lf.get(1)+", "+lf.get(2)+", "+right;
				st3=" FROM "+tb.getTableName()+";";
				createQUERY=st1+st2+st3;
				//createQUERY = "CREATE TABLE AGC"+tb.getTableName()+i+" AS SELECT DISTINCT "+lf.get(0)+", "+lf.get(1)+", "+lf.get(2)+", "+right+" FROM "+tb.getTableName()+";";
				st.execute(createQUERY);
				OutputWriter.outputToFile("NF.SQL", st1);	
    			OutputWriter.outputToFile("NF.SQL", st2);
    			OutputWriter.outputToFile("NF.SQL", st3);
    			OutputWriter.outputToFile("NF.SQL", "\n");
				createdTables.add("AGC"+tb.getTableName()+i);
				//System.out.println(createQUERY);
			}
			
			if(numOfLeftEle == 4){ //if number of elements in LHS is 4
				st1="CREATE TABLE AGC"+tb.getTableName()+i;
				st2=" AS SELECT DISTINCT "+lf.get(0)+", "+lf.get(1)+", "+lf.get(2)+", "+lf.get(3)+", "+right;
				st3=" FROM "+tb.getTableName()+";";
				createQUERY=st1+st2+st3;
				//createQUERY = "CREATE TABLE AGC"+tb.getTableName()+i+" AS SELECT DISTINCT "+lf.get(0)+", "+lf.get(1)+", "+lf.get(2)+", "+lf.get(3)+", "+right+" FROM "+tb.getTableName()+";";
				st.execute(createQUERY);
				OutputWriter.outputToFile("NF.SQL", st1);	
    			OutputWriter.outputToFile("NF.SQL", st2);
    			OutputWriter.outputToFile("NF.SQL", st3);
    			OutputWriter.outputToFile("NF.SQL", "\n");
				createdTables.add("AGC"+tb.getTableName()+i);
				//System.out.println(createQUERY);
			}
					
		}
		
		for(int j=0; j<(szz2); j++){ //Here the RHS may be more than 1 and also the LHS
			//Get the RHS and LHS
			List<String> sLHS = tb.getDepLHS_temp3().get(j);
			Set<String> sRHS = tb.getDepRHS_temp3().get(j);
			String str1,str2,str3;
			
			String LHS = sLHS.toString();
			LHS = LHS.substring(1, LHS.length()-1);
			
			String RHS = sRHS.toString();
			RHS = RHS.substring(1, RHS.length()-1);
			
			//System.out.println(LHS);
			//System.out.println(RHS);
			
			int numOfRightEle = sRHS.size();
			int numoflEle = sLHS.size();
			
			str1="CREATE TABLE AGC"+tb.getTableName()+j+szz1;
			str2=" AS SELECT DISTINCT "+LHS+", "+RHS;
			str3=" FROM "+tb.getTableName()+";";
			createQUERY=str1+str2+str3;
			//createQUERY = "CREATE TABLE AGC"+tb.getTableName()+j+szz1+" AS SELECT DISTINCT "+LHS+", "+RHS+" FROM "+tb.getTableName()+";";
			st.execute(createQUERY);
			OutputWriter.outputToFile("NF.SQL", str1);	
			OutputWriter.outputToFile("NF.SQL", str2);
			OutputWriter.outputToFile("NF.SQL", str3);
			OutputWriter.outputToFile("NF.SQL", "\n");
			createdTables.add("AGC"+tb.getTableName()+j+szz1);
			//System.out.println(createQUERY);
		}
		
		
		//Create the table and compare the count
		String stri1,stri2;
		stri1="SELECT COUNT(*) AS COUNT4 ";
		stri2="FROM "+tb.getTableName()+";";
		OutputWriter.outputToFile("NF.SQL", stri1);
		OutputWriter.outputToFile("NF.SQL", stri2);
		OutputWriter.outputToFile("NF.SQL", "\n");
		String cou1 = "SELECT COUNT(*) AS COUNT4 FROM "+tb.getTableName()+";";
		//System.out.println(cou1);
		ResultSet rs5 = CertifyNormals.executeQuery(cou1);
		rs5.next();
		int count1 = rs5.getInt("COUNT4");
		//System.out.println("Count1 "+count1);
		
		//Drop all the tables created for verification
		for(int k=0; k<createdTables.size();k++){
			String DROP = "";
			DROP = "DROP TABLE "+createdTables.get(k)+";";
			st.execute(DROP);
			OutputWriter.outputToFile("NF.SQL", DROP);
			//System.out.println(DROP);
		}
	}
	
	
	public static boolean checkColumns(Connection conn, Table tb){
		ArrayList<String> colPKeys=tb.getpKey();
		ArrayList<String> colNonKey=tb.getAttributes();
		ArrayList<String> allCols=new ArrayList<String>();
		allCols.addAll(colPKeys);
		allCols.addAll(colNonKey);
		try {
			//createConnection.st=createConnection.ConnectionTest().createStatement();
			Statement stmt1 = conn.createStatement();
			String st1="SELECT COLUMN_NAME ";
			String st2="FROM COLUMNS ";
			String st3="WHERE TABLE_NAME='"+tb.getTableName()+"' ;";
			String sqlQuery=st1+st2+st3;
			OutputWriter.outputToFile("NF.SQL", st1);
			OutputWriter.outputToFile("NF.SQL", st2);
			OutputWriter.outputToFile("NF.SQL", st3);
			OutputWriter.outputToFile("NF.SQL", "\n");
			ResultSet rs = stmt1.executeQuery(sqlQuery);
		    ArrayList<String> allColsFromDB = new ArrayList<String>();
			while(rs.next()){
		    	String col = rs.getString(1);
		    	allColsFromDB.add(col);
		    }
			if(allColsFromDB.containsAll(allCols)){
				return true;
			}
			else{
				//System.out.println("Invalid Column Name");
				return false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
}
