import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import org.gibello.zql.ParseException;
import org.gibello.zql.ZDelete;
import org.gibello.zql.ZExpression;
import org.gibello.zql.ZInsert;
import org.gibello.zql.ZQuery;
import org.gibello.zql.ZStatement;
import org.gibello.zql.ZqlParser;
import org.gibello.zql.ZSelectItem;


public class QueryProcessor {
	private ZqlParser p = new ZqlParser();
	private MyDatabase myDb;
	private static final List<String> operators = Arrays.asList("<=", ">=", ">", "<", "=", "NOT");
	private static final List<String> bool_operators = Arrays.asList("=", "NOT");
	private String dbLocation;
	private boolean firstQuery = true;

	public QueryProcessor(MyDatabase myDb) {
		this.myDb = myDb;
	}

	public void setDbLocation(String dbLocation) {
		this.dbLocation = dbLocation;
	}

	public static String trimQuotes(String str) {
		return str.replaceAll("^\'|\'$", "");
	}

	private boolean validateWhereClause(String operator, Vector operands) {
		int index = -1;

		//LHS
		String lhs = operands.elementAt(0).toString();

		//RHS
		String rhs = trimQuotes(operands.elementAt(1).toString());

		//find element index in FILE_HEADER_MAPPING
		for(int i=0;i<MyDatabase.FILE_HEADER_MAPPING.length;i++) {
			if(lhs.equalsIgnoreCase(MyDatabase.FILE_HEADER_MAPPING[i])) {
				index = i;
				break;
			}
		}

		if(index < 0) {
			System.err.println("Unknow column "+lhs+" in the where clause");
			return false;
		}

		String data_type = MyDatabase.DATA_TYPE_MAPPING[index];

		switch(data_type) {
		case "int":
			try {
				Integer.parseInt(rhs);
			} catch(NumberFormatException nfe) {
				System.err.println("Invalid value "+rhs+" for column "+lhs);
				return false;
			}

			if(!operators.contains(operator))
			{
				System.err.println("Invalid operator "+operator+" for "+lhs+" and "+rhs);
			}
			break;

		case "float":
			try {
				Float.parseFloat(rhs);
			} catch(NumberFormatException nfe) {
				System.err.println("Invalid value "+rhs+" for column "+lhs);
				return false;
			}

			if(!operators.contains(operator))
			{
				System.err.println("Invalid operator "+operator+" for "+lhs+" and "+rhs);
			}
			break;

		case "short":
			try {
				Short.parseShort(rhs);
			} catch(NumberFormatException nfe) {
				System.err.println("Invalid value "+rhs+" for column "+lhs);
				return false;
			}

			if(!operators.contains(operator))
			{
				System.err.println("Invalid operator "+operator+" for "+lhs+" and "+rhs);
			}
			break;

		case "string":
			if(!operators.contains(operator))
			{
				System.err.println("Invalid operator "+operator+" for "+lhs+" and "+rhs);
			}
			break;

		case "boolean":
			try {
				Boolean.parseBoolean(rhs);
			} catch(NumberFormatException nfe) {
				System.err.println("Invalid value "+rhs+" for column "+lhs);
				return false;
			}

			if(!bool_operators.contains(operator))
			{
				System.err.println("Invalid operator "+operator+" for "+lhs+" and "+rhs);
			}
			break;			
		}

		return true;
	}

	public boolean validateSelect(Vector columns) {
		Iterator<ZSelectItem> it = columns.iterator();
		String column;
		boolean flag = true;

		if(!columns.elementAt(0).toString().equalsIgnoreCase("*")) {
			while(it.hasNext()) {
				column = it.next().toString();

				if(!Arrays.asList(MyDatabase.FILE_HEADER_MAPPING).contains(column)) {
					System.err.println("Invalid column name "+column+ " in the select query");
					flag = false;
				}
			}
		}
		return flag;
	}

	public boolean validateInsert(Vector columns, Vector values) {
		if(values.size() != MyDatabase.FILE_HEADER_MAPPING.length) {
			System.err.println("Number of columns in insert doesn't match number of columns in table");
			return false;
		}

		if(columns != null) {
			if(columns.size() > 0) {
				System.err.println("Column names should not be specified in the insert statement");
				return false;
			}
		}
		
		return true;
	}

	public boolean checkIfDbFileExists(String tableName) {
		File file = new File(dbLocation+"/"+tableName+".db");
		if(!file.exists())
			return false;

		return true;
	}
	
	//for this project
	public String handleReverseNot(String stmt) {
		
		int notStartIndex = stmt.indexOf(" not");
		
		char[] charArray = stmt.toCharArray();
		
		boolean inWord = false;
		int endIndex = 0;
		int startIndex = 0;
		int j = notStartIndex + 1;
		notStartIndex++;
		
		int notEndIndex = notStartIndex + "not".length();
		
		while(charArray[j - 1] == ' ') {
			j--;
		}
		
		int columnInx = j - 1;
		
		
		for(int i = columnInx; i >= 0; i--) {
			
			if(charArray[i] != ' ') {
				if(!inWord) {
					endIndex = i;
					inWord = true;
				}
			} else {
				if(inWord) {
					inWord = false;
					startIndex = i + 1;
					break;
				}
			}
		}
		
		String column = stmt.substring(startIndex, endIndex+1);
		
		StringBuilder strBulider = new StringBuilder(stmt);
		strBulider.replace(startIndex, endIndex+1, "not");
		
		strBulider.replace(notStartIndex+1, notEndIndex+1, column);
		
		return strBulider.toString();

	}

	public int processQuery(String stmt) {

		String name;
		boolean negation = false;

		if(stmt.equalsIgnoreCase("exit;")) {
			return -1;
		}
		else if(stmt.toLowerCase().startsWith("import ") && stmt.toLowerCase().endsWith(";")) {
			String csvFileLoc = stmt.substring(7, stmt.length() - 1).trim();
			File file = new File(csvFileLoc);
			if(file.exists() && !file.isDirectory()) { 
				name = file.getName();
				int pos = name.lastIndexOf(".");
				if (pos > 0)
				{
					name = name.substring(0, pos);
				}
				
				File dbFile = new File(dbLocation+"/"+name+".db");
				
				if(dbFile.exists()) {
					dbFile.delete();
				}

				myDb.importCSV(file, dbLocation);

				myDb.updateIndexes(name, dbLocation);
			} else {
				System.err.println("Please enter a valid path pointing towards the CSV file");
				return 0;
			}
		}
		else {
			
			if(stmt.contains(" not")) {
				stmt = handleReverseNot(stmt);
			}
			
			p.initParser(new ByteArrayInputStream(stmt.getBytes()));

			try {
				ZStatement st = p.readStatement();

				//insert statement
				if(st instanceof ZInsert)
				{
					Record record = new Record();
					ZInsert ins = (ZInsert)st;
					Vector values = ins.getValues();
					name = ins.getTable();

					if(!checkIfDbFileExists(name)) {
						System.err.println("Please import the csv file before executing sql queries");
						return 0;
					} else {
						if(firstQuery) {
							myDb.populateIndexes(name, dbLocation);
							firstQuery = false;
						}
					}

					try
					{
						//form the record object by looping through the vector
						String str = trimQuotes(values.elementAt(0).toString());
						record.setId(Integer.parseInt(str));

						str = trimQuotes(values.elementAt(1).toString());
						record.setCompany(str);

						str = trimQuotes(values.elementAt(2).toString());
						record.setDrug_id(str);

						str = trimQuotes(values.elementAt(3).toString());
						record.setTrials(Short.parseShort(str));

						str = trimQuotes(values.elementAt(4).toString());
						record.setPatients(Short.parseShort(str));

						str = trimQuotes(values.elementAt(5).toString());
						record.setDosage_mg(Short.parseShort(str));

						str = trimQuotes(values.elementAt(6).toString());
						record.setReading(Float.parseFloat(str));

						str = trimQuotes(values.elementAt(7).toString());
						record.setDouble_blind(Boolean.parseBoolean(str));

						str = trimQuotes(values.elementAt(8).toString());
						record.setControlled_study(Boolean.parseBoolean(str));

						str = trimQuotes(values.elementAt(9).toString());
						record.setGovt_funded(Boolean.parseBoolean(str));

						str = trimQuotes(values.elementAt(10).toString());
						record.setFda_approved(Boolean.parseBoolean(str));
					}
					catch(NumberFormatException nfe) {
						nfe.printStackTrace();
						System.err.println("Values entered not correct");
					}

					if(validateInsert(ins.getColumns(), values)) {
						if(myDb.validatePKConstraint(record)) {
							myDb.addRecord(record, name, dbLocation);
							myDb.updateIndexes(name, dbLocation);
							System.out.println("1 row(s) inserted");
						} else {
							System.err.println("Duplicate value for primary key coulmn");
							return 0;
						}
					}
				}
				else if(st instanceof ZQuery)
				{
					//select query
					ZQuery query = (ZQuery)st;

					Vector columns = query.getSelect();
					Vector from = query.getFrom();

					if(!checkIfDbFileExists(from.elementAt(0).toString())) {
						System.err.println("Please import the csv file before executing sql queries");
						return 0;
					} else {
						if(firstQuery) {
							myDb.populateIndexes(from.elementAt(0).toString(), dbLocation);
							firstQuery = false;
						}
					}

					ZExpression where = (ZExpression) query.getWhere();

					if(where != null) {
						String operator = where.getOperator();
						Vector operands = where.getOperands();

						if(operator.equalsIgnoreCase("NOT")) {
							ZExpression exp =  (ZExpression) operands.elementAt(0);
							operator = exp.getOperator();
							operands = exp.getOperands();
							negation = true;
						}

						//validate the operator and operand combination
						if(validateWhereClause(operator, operands) && validateSelect(columns))
						{
							myDb.printResult(columns, myDb.queryRecords(from, operator, operands, negation, dbLocation));
						}
						else
						{
							return 0;
						}
					} else {
						myDb.printResult(columns, myDb.queryAllRecords(from, dbLocation));
					}
				}
				else if(st instanceof ZDelete)
				{
					ZDelete delete = (ZDelete)st;

					String table = delete.getTable();

					if(!checkIfDbFileExists(table)) {
						System.err.println("Please import the csv file before executing sql queries");
						return 0;
					} else {
						if(firstQuery) {
							myDb.populateIndexes(table, dbLocation);
							firstQuery = false;
						}
					}

					ZExpression where = (ZExpression) delete.getWhere();

					if(where != null) {
						String operator = where.getOperator();
						Vector operands = where.getOperands();

						if(operator.equalsIgnoreCase("NOT")) {
							ZExpression exp =  (ZExpression) operands.elementAt(0);
							operator = exp.getOperator();
							operands = exp.getOperands();
							negation = true;
						}

						if(validateWhereClause(operator, operands))
						{
							myDb.deleteRecord(table, operator, operands, negation, dbLocation);
						}
						else
						{
							System.err.println("Failed in query processing");
						}
					} else {
						//delete all records
						myDb.deleteAllRecords(table, dbLocation);
					}
				}
			} 
			catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return 0;
			}
		}
		return 1;
	}
}
