import java.io.File;

public class Client {
	public static void main(String[] args) {
		File file = new File("/Users/roshan/Documents/UTD/Fall 2015/Database/Programming Project 2/PHARMA_TRIALS_1000B.csv");
		String name = file.getName();
		int pos = name.lastIndexOf(".");
		if (pos > 0)
		{
			name = name.substring(0, pos);
		}
		
		MyDatabase myDb = new MyDatabase(name);
		QueryProcessor qp = new QueryProcessor(myDb);

		//bulk import
		//myDb.importCSV(file);

		//updating indexes
		//myDb.updateIndexes(name);

		//view record
		//read index from file on load of db
		myDb.populateIndexes(name);
		
		String sqlStmt = "SELECT * FROM PHARMA_TRIALS_1000B WHERE drug_id = 'IW-308';";
		qp.processQuery(sqlStmt);
	}
}
