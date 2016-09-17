import java.sql.*;
import java.util.Properties;

public class createConnection {    
	public static Connection createStatement(){
		
		Connection conn = null;
		
        // Load JDBC driver
        try {
        	//System.out.println("In try to connect to Driver");
            Class.forName("com.vertica.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // Could not find the driver class. Likely an issue
            // with finding the .jar file.
            System.err.println("Could not find the JDBC driver class.");
            e.printStackTrace();
            return conn; 
        }

        // Create property object to hold user name & password
        Properties myProp = new Properties();
        myProp.put("user", "xxxxxxxx");
        myProp.put("password", "xxxxxxxxx");
        
        try {
        	//System.out.println("In try to make connection");
            conn = DriverManager.getConnection("jdbc:vertica://xxxxxxxxx", myProp);
            //System.out.println("In try Connection successful");
            return conn;
        } catch (SQLException e) {
            // Could not connect to database.
            System.err.println("Could not connect to database.");
            e.printStackTrace();
            return conn; 
        }        
   }
}
