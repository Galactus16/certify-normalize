import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class OutputWriter {
	
	public static void createFile() {
		try {
			File OutputFile, SQLFile;

			//Create the NF.txt file			
			OutputFile = new File("NF.txt");
			if (OutputFile.createNewFile()) {
				//System.out.println("Output File is created!");
			} else {
				System.out.println(OutputFile + " already exists.");
			}
			
			//Create the NF.SQL file
			SQLFile = new File("NF.SQL");
			if (SQLFile.createNewFile()) {
				//System.out.println("SQL File is created!");
			} else {
				System.out.println(SQLFile + " already exists.");
			}
		} catch (IOException e) {
			//System.err.println("Could not create Output File");
			e.printStackTrace();
		}
	}
	
	//Method to write to the file
	public static void outputToFile(String FName,String str){
		FileWriter writer;
	 	BufferedWriter bw;
	
	 	try{
	 		writer=new FileWriter(FName,true);
	 		bw=new BufferedWriter(writer);
	 		bw.write(str);
	 		bw.write("\n");
		 	bw.close();
	 	}catch(Exception ex){
	 		//System.err.println("Could not write to Output File");
	 		ex.printStackTrace();
	 	}
	 
	}


	
}
