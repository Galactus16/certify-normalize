import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class Decompose{
	
	//Decompose 3NF dependencies
	public void decompose3NFTable(Table tb){
		//Here the keys will be always in the main table
		//The dependencies always contain the non-key attributes
		
		//Creating a ArrayList to contain all the dependencies RHS in the table
		ArrayList<List<String>> depLHS_temp = new ArrayList<List<String>>(tb.getDepLHS());
		
		//Creating a ArrayList to contain all the dependencies LHS in the table
		ArrayList<String> depRHS_temp = new ArrayList<String>(tb.getDepRHS());
		
		//ArrayList of the index to be removed from the main tb dependency
		List<Integer> indexArray = new ArrayList<>();
		
		//System.out.println(depLHS_temp);
		//System.out.println(depRHS_temp);
		
		for(int i=0; i<depLHS_temp.size();i++){ 
			List<String> LHS = depLHS_temp.get(i); //Get the LHS
			if(LHS.size() == 1){ //Only one element in LHS
				//Get the LHS and RHS
				String L_attribute = LHS.get(0);
				String R_attribute = depRHS_temp.get(i);
				
				//Now check each LHS in depLHS_temp and remove having same LHS and RHS from main one
				for(int j=0;j<depLHS_temp.size();j++){
					if(i != j){ //Shouldn't be the same row
						if( R_attribute == depRHS_temp.get(j)){ //if it contains the same RHS
							List<String> LHS_inner = depLHS_temp.get(j);
							
							if(LHS_inner.contains(L_attribute)){ //and if it contains same LHS as one of the element
								/*tb.getDepLHS().remove(j); //Remove this dependencies from the tb LHS
								tb.getDepRHS().remove(j); //Remove this dependencies from the tb RHS */
								indexArray.add(j);
							} 
						}						
					}
				}
			}
		}
		
		//Case where this RHS comes in LHS AND LHS = RHS
		for(int i=0; i<depLHS_temp.size();i++){ 
			List<String> LHS = depLHS_temp.get(i); //Get the LHS
			if(LHS.size() == 1){ //Only one element in LHS
				//Get the LHS and RHS
				String L_attribute = LHS.get(0);
				String R_attribute = depRHS_temp.get(i);
				
				//Now check each LHS in depLHS_temp and remove having same LHS and RHS from main one
				for(int j=i;j<depLHS_temp.size();j++){
					if(i != j){ //Shouldn't be the same row
						if( depLHS_temp.get(j).contains(R_attribute) && L_attribute == depRHS_temp.get(j)){
							indexArray.add(j);
						}						
					}
				}
			}
		}
		
		//Update the main tb dependencies
		Decompose.ListUpdater(tb, indexArray, depLHS_temp);
		depLHS_temp.clear();
		depRHS_temp.clear();
		
		//case of i -> B; i -> C; i -> F change to i -> B, C, F
		depLHS_temp.addAll(tb.getDepLHS());
		depRHS_temp.addAll(tb.getDepRHS());
		
		//Set of index to be removed from the main tb dependencies
		TreeSet<Integer> id = new TreeSet<Integer>();
		TreeSet<String> RHS = new TreeSet<String>();
		
		for(int h=0; h < depLHS_temp.size(); h++){
			List<String> LHS = depLHS_temp.get(h); //Get the LHS
			String R_attribute = depRHS_temp.get(h); //Get the RHS
			
			//Now check each LHS and see if it is similar to this LHS
			//Moreover also check if the h is not same
			//Moreover also check if the RHS is not same
			//If it passes all this -> then delete both the dependencies from the tb one
			//Aggregate both of them to new List which can finally be added to tb in end
			for(int p=0; p<depRHS_temp.size();p++){
				if((p != h) && (R_attribute != depRHS_temp.get(p))){
					//System.out.println("case found correct 1");
					//System.out.println("LHS reeee: "+LHS);
					//System.out.println("DepLHS"+depLHS_temp.get(p));
					if(LHS.equals(depLHS_temp.get(p))){
						//System.out.println("case found correct 2");
						id.add(h);
						id.add(p);
						if(!tb.getDepLHS_temp3().contains(LHS)){
							tb.getDepLHS_temp3().add(LHS);
						}
						if(!RHS.contains(R_attribute)){
							RHS.add(R_attribute);
						}
						if(!RHS.contains(depRHS_temp.get(p))){
							RHS.add(depRHS_temp.get(p));
						}						
						if(!tb.getDepRHS_temp3().contains(RHS)){
							tb.getDepRHS_temp3().add(RHS);
						}
					}					
				}
			}
			//RHS.clear();
		}
		
		//Clear the temp and we will use it
		depLHS_temp.clear();
		depRHS_temp.clear();
		
		//Remove these indexed elements from the main tb LHS and RHS dependencies list
		for(int u=0; u < tb.getDepLHS().size(); u++){
			if(!(id.contains(u))){
				depLHS_temp.add(tb.getDepLHS().get(u));
				depRHS_temp.add(tb.getDepRHS().get(u));
			}
		}
		
		//Update the tb
		tb.getDepLHS().clear();
		tb.getDepRHS().clear();
		
		tb.getDepLHS().addAll(depLHS_temp);
		tb.getDepRHS().addAll(depRHS_temp);
		
	}
	
	//Update the main dependency
	public static void ListUpdater(Table tb, List<Integer> indexArray, ArrayList<List<String>> depLHS_temp){
				//The values of index not present in indexArray are the one which are real dependencies on which we need to decompose the tables
				//Creating a ArrayList to contain all the dependencies RHS in the table
				ArrayList<List<String>> depLHS_temp2 = new ArrayList<List<String>>();
						
				//Creating a ArrayList to contain all the dependencies LHS in the table
				ArrayList<String> depRHS_temp2 = new ArrayList<String>();
				
				//Sort the indexArray before doing anything
				Collections.sort(indexArray);
				
				//For the index which is not there in the list add that elements to new list
				for(int k = 0;k<depLHS_temp.size(); k++){
					if(!indexArray.contains(k)){
						depLHS_temp2.add(tb.getDepLHS().get(k));
						depRHS_temp2.add(tb.getDepRHS().get(k));
					}
				}
				
				//System.out.println("LHS : "+depLHS_temp2);
				//System.out.println("RHS : "+depRHS_temp2);
				
				tb.getDepLHS().clear();
				tb.getDepRHS().clear();
				
				tb.getDepLHS().addAll(depLHS_temp2);
				tb.getDepRHS().addAll(depRHS_temp2);
	}
	
	//Decompose the table, create the decomposed tables in DB, Join them and verify the decomposition.
	/*public void decomposeTableVerify(Table tb){
		
		//Find how many tables you need to create
		int szz1 = tb.getDepLHS().size();
		int szz2 = tb.getDepLHS_temp3().size();
		//int numOfTables = szz1 + szz2 + 1;
		
		//Define the createQuery
		String createQUERY = "";
		
		for(int i=0; i<szz1; i++){ //Here the table will have only one element in right hand side
			//Get the RHS and LHS
			String right = tb.getDepRHS().get(i);
			List<String> lf = tb.getDepLHS().get(i);
			//Find the number of elements in lHS
			int numOfLeftEle = lf.size();
			
			if(numOfLeftEle == 1){ //if number of elements in LHS is 1
				//String create
			}
					
		}
		
		
	}*/
}