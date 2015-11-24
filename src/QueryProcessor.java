import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.gibello.zql.ParseException;
import org.gibello.zql.ZExp;
import org.gibello.zql.ZExpression;
import org.gibello.zql.ZInsert;
import org.gibello.zql.ZQuery;
import org.gibello.zql.ZStatement;
import org.gibello.zql.ZqlParser;

import sun.awt.geom.AreaOp.IntOp;


public class QueryProcessor {
	private ZqlParser p = new ZqlParser();
	private MyDatabase myDb;
	private static final List<String> operators = Arrays.asList("<=", ">=", ">", "<", "=", "NOT");
	private static final List<String> bool_operators = Arrays.asList("=", "NOT");

	public QueryProcessor(MyDatabase myDb) {
		this.myDb = myDb;
	}

	public static String trimQuotes(String str) {
		return str.replaceAll("^\'|\'$", "");
	}

	private boolean validateWhereClause(String operator, Vector operands) {
		int index = -1;

		//LHS
		String lhs = operands.elementAt(0).toString();

		//RHS
		String rhs = operands.elementAt(1).toString();

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

	public void processQuery(String stmt) {
		p.initParser(new ByteArrayInputStream(stmt.getBytes()));
		String name;

		try {
			ZStatement st = p.readStatement();

			//insert statement
			if(st instanceof ZInsert)
			{
				Record record = new Record();
				ZInsert ins = (ZInsert)st;
				Vector values = ins.getValues();
				name = ins.getTable();

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
				record.setTrials(Short.parseShort(str));

				str = trimQuotes(values.elementAt(5).toString());
				record.setPatients(Short.parseShort(str));

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

				myDb.addRecord(record, name);
				myDb.updateIndexes(name);
			}
			else if(st instanceof ZQuery)
			{
				//select query
				ZQuery query = (ZQuery)st;
				boolean negation = false;

				Vector columns = query.getSelect();
				Vector from = query.getFrom();
				ZExpression where = (ZExpression) query.getWhere();

				String operator = where.getOperator();
				Vector operands = where.getOperands();

				if(operator.equalsIgnoreCase("NOT")) {
					ZExpression exp =  (ZExpression) operands.elementAt(0);
					operator = exp.getOperator();
					operands = exp.getOperands();
					negation = true;
				}

				//validate the operator and operand combination
				if(validateWhereClause(operator, operands))
				{
					//myDb.queryRecords(columns, from, operator, operands, negation);
					myDb.printResult(columns, myDb.queryRecords(columns, from, operator, operands, negation));
				}
				else
				{
					System.err.println("Failed in query processing");
				}
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
