import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class Reader {

	public ArrayList<Table> TableList;

	public ArrayList<Table> populateTables(String fileName) {
		String tableName, readString;
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String> lines = new ArrayList<String>();

		// Creating an arraylist of the Table objects
		Reader rd = new Reader();
		rd.TableList = new ArrayList<Table>();

		// Reading the file
		File file = new File(fileName);

		try {
			FileReader reader = new FileReader(file);
			BufferedReader buffReader = new BufferedReader(reader);
			int x = 0;
			String s;
			while ((s = buffReader.readLine()) != null) {
				lines.add(s);
				x++;
			}
		} catch (IOException e) {
			System.out.println("Exception in Reading the Text file");
			System.exit(0);
		}
		for (int i = 0; i < lines.size(); i++) {

			// Create a new object of the Table class
			Table tb = new Table();

			String[] tokens = lines.get(i).split("\\(");
			tableName = tokens[0];

			// add this table name to the object
			tb.setTableName(tokens[0]);

			CharSequence regexKey = "(k";
			// tableName.length();
			readString = lines.get(i).substring(tableName.length());

			StringTokenizer st = new StringTokenizer(readString, ",");
			while (st.hasMoreTokens()) {
				String token = st.nextToken();
				int flag = 0;
				if (token.startsWith("(")) {
					token = token.substring(1);
				}
				if (token.endsWith(")")) {
					token = token.substring(0, token.indexOf(")"));
				}
				if (token.contains(regexKey)) {
					token = token.substring(0, token.indexOf("("));
					tb.getpKey().add(token);
					flag = 1;
				}
				if (flag == 0) {
					columnNames.add(token);
					tb.getAttributes().add(token);
				}
			}
			// Add this table to the TableList arraylist
			rd.TableList.add(tb);
		}

		return rd.TableList;
	}

}
