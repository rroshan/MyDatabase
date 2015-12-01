import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.gibello.zql.ZSelectItem;

import com.bethecoder.ascii_table.ASCIITable;


public class MyDatabase {

	//Hardcoing. In actual implemenation will be read from metadata.
	public static final String [] FILE_HEADER_MAPPING = {"id","company","drug_id","trials","patients","dosage_mg","reading","double_blind","controlled_study","govt_funded","fda_approved"};
	public static final String [] DATA_TYPE_MAPPING = {"int","string","string","short","short","short","float","boolean","boolean","boolean","boolean"};
	public static final String primaryKey = "id";

	private static final String ID = "id";
	private static final String COMPANY = "company";
	private static final String DRUG_ID = "drug_id";
	private static final String TRIALS = "trials";
	private static final String PATIENTS = "patients";
	private static final String DOSAGE_MG = "dosage_mg";
	private static final String READING = "reading";
	private static final String DOUBLE_BLIND = "double_blind";
	private static final String CONTROLLED_STUDY = "controlled_study";
	private static final String GOVT_FUNDED = "govt_funded";
	private static final String FDA_APPROVED = "fda_approved";



	final static byte double_blind_mask      = 8;    // binary 0000 1000
	final static byte controlled_study_mask  = 4;    // binary 0000 0100
	final static byte govt_funded_mask       = 2;    // binary 0000 0010
	final static byte fda_approved_mask      = 1;    // binary 0000 0001

	private Map<Integer, List<Long>> id_map;
	private Map<String, List<Long>> company_map;
	private Map<String, List<Long>> drug_id_map;
	private Map<Short, List<Long>> trials_map;
	private Map<Short, List<Long>> patients_map;
	private Map<Short, List<Long>> dosage_mg_map;
	private Map<Float, List<Long>> reading_map;
	private Map<Boolean, List<Long>> double_blind_map;
	private Map<Boolean, List<Long>> controlled_study_map;
	private Map<Boolean, List<Long>> govt_funded_map;
	private Map<Boolean, List<Long>> fda_approved_map;

	private <K extends Comparable<K>> List<Long> getRecordLocations(Map<K, List<Long>> map, K indexValue, String tableName, String operator) {
		List<Long> value = null;
		Iterator<Long> it;
		long fileLocation;

		List<Long> recordLocations = new ArrayList<Long>();

		for(Map.Entry<K,List<Long>> entry : map.entrySet()) {
			K key = entry.getKey();

			if(operator.equalsIgnoreCase("<")) {
				if(key.compareTo(indexValue) < 0) {
					value = entry.getValue();  
				}
			} else if(operator.equalsIgnoreCase(">")) {
				if(key.compareTo(indexValue) > 0) {
					value = entry.getValue();  
				}
			} else if(operator.equalsIgnoreCase("=")) {
				if(key.compareTo(indexValue) == 0) {
					value = entry.getValue();  
				}
			} else if(operator.equalsIgnoreCase("<=")) {
				if(key.compareTo(indexValue) < 0) {
					value = entry.getValue();  
				} else if(key.compareTo(indexValue) == 0) {
					value = entry.getValue();  
				}
			} else if(operator.equalsIgnoreCase(">=")) {
				if(key.compareTo(indexValue) > 0) {
					value = entry.getValue();  
				} else if(key.compareTo(indexValue) == 0) {
					value = entry.getValue();  
				}
			} else if(operator.equalsIgnoreCase("!=")) {
				if(key.compareTo(indexValue) != 0) {
					value = entry.getValue(); 
				}
			}

			if(value != null) {
				it = value.iterator();
				while(it.hasNext()) {
					fileLocation = it.next();
					recordLocations.add(fileLocation);
				}
			}

			value = null;
		}

		return recordLocations;
	}

	public Record fetchRecordFromFile(long offset, String tableName, String dbLocation) {
		RandomAccessFile raf = null;
		File file = new File(dbLocation+"/"+tableName+".db");
		Record record = new Record();
		byte[] b;

		int dbm_position = 3;    // binary 0000 1000
		int csm_position = 2;    // binary 0000 0100
		int gfm_position = 1;    // binary 0000 0010
		int fam_position = 0;    // binary 0000 0001

		try {
			raf = new RandomAccessFile(file, "r");
			raf.seek(offset);

			//read the record and add to a record object. Print the record object.
			record.setId(raf.readInt());

			int varcharLength = raf.readByte();
			b = new byte[varcharLength];
			raf.read(b);
			String str = new String(b);
			record.setCompany(str);

			b = new byte[6];
			raf.read(b);
			str = new String(b);
			record.setDrug_id(str);

			record.setTrials(raf.readShort());

			record.setPatients(raf.readShort());

			record.setDosage_mg(raf.readShort());

			record.setReading(raf.readFloat());

			byte byt = raf.readByte();

			if(byt < 0) {
				return null;
			} else {
				int op = (byt >> fam_position) & 1;
				if(op == 1) {
					record.setFda_approved(true);
				}

				op = (byt >> gfm_position) & 1;
				if(op == 1) {
					record.setGovt_funded(true);
				}

				op = (byt >> csm_position) & 1;
				if(op == 1) {
					record.setControlled_study(true);
				}

				op = (byt >> dbm_position) & 1;
				if(op == 1) {
					record.setDouble_blind(true);
				}
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				raf.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return record;
	}

	public void printResult(Vector columns, List<Record> result) {
		//mysql type output implementation
		//header formation
		if(result.size() > 0) {
			Vector<String> strColumn = new Vector<String>();

			if(columns.elementAt(0).toString().equalsIgnoreCase("*")) {
				for(int i=0;i<FILE_HEADER_MAPPING.length;i++) {
					strColumn.add(FILE_HEADER_MAPPING[i]);
				}
			} else {
				Iterator<ZSelectItem> headerIterator = columns.iterator();
				while(headerIterator.hasNext()) {
					strColumn.add(headerIterator.next().toString());
				}
			}

			String[] header = new String[strColumn.size()];
			int count = 0;

			Record record;
			Iterator<String> headerIterator = strColumn.iterator();
			while(headerIterator.hasNext()) {
				header[count++] = headerIterator.next();
			}

			count = 0;
			int rowCount = 0;
			String[][] data = new String[result.size()][strColumn.size()];
			Iterator<Record> dataIterator = result.iterator();
			String title;
			while(dataIterator.hasNext()) {
				record = dataIterator.next();
				count = 0;
				headerIterator = strColumn.iterator();
				while(headerIterator.hasNext()) {
					title = headerIterator.next();

					switch(title) {
					case "id":
						data[rowCount][count] = Integer.toString(record.getId());
						break;

					case "company":
						data[rowCount][count] = record.getCompany();
						break;

					case "drug_id":
						data[rowCount][count] = record.getDrug_id();
						break;

					case "trials":
						data[rowCount][count] = Short.toString(record.getTrials());
						break;

					case "patients":
						data[rowCount][count] = Short.toString(record.getPatients());
						break;

					case "dosage_mg":
						data[rowCount][count] = Short.toString(record.getDosage_mg());
						break;

					case "reading":
						data[rowCount][count] = Float.toString(record.getReading());
						break;

					case "double_blind":
						data[rowCount][count] = Boolean.toString(record.isDouble_blind());
						break;

					case "controlled_study":
						data[rowCount][count] = Boolean.toString(record.isControlled_study());
						break;

					case "govt_funded":
						data[rowCount][count] = Boolean.toString(record.isGovt_funded());
						break;

					case "fda_approved":
						data[rowCount][count] = Boolean.toString(record.isFda_approved());
					}
					count++;
				}
				rowCount++;
			}

			ASCIITable.getInstance().printTable(header, data);	
		}

		System.out.println(result.size()+" in set");

	}

	public List<Long> getRecordLocations(String tableName, String operator, Vector operands, boolean negation) {

		String leftOperand = operands.elementAt(0).toString().toLowerCase();

		int int_ty;
		String string_ty;
		short short_ty;
		float float_ty;
		boolean boolean_ty;
		List<Long> offset = null;

		if(negation) {
			switch(operator) {
			case "=":
				operator = "!=";
				break;

			case ">":
				operator = "<=";
				break;

			case "<":
				operator = ">=";
				break;

			case "<=":
				operator = ">";
				break;

			case ">=":
				operator = "<";
			}
		}


		switch(leftOperand) {
		case "id":
			int_ty = Integer.parseInt(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = getRecordLocations(id_map, int_ty, tableName, operator);
			break;
		case "company":
			string_ty = QueryProcessor.trimQuotes(operands.elementAt(1).toString());
			offset = getRecordLocations(company_map, string_ty, tableName, operator);
			break;
		case "drug_id":
			string_ty = QueryProcessor.trimQuotes(operands.elementAt(1).toString());
			offset = getRecordLocations(drug_id_map, string_ty, tableName, operator);
			break;
		case "trials":
			short_ty = Short.parseShort(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = getRecordLocations(trials_map, short_ty, tableName, operator);
			break;
		case "patients":
			short_ty = Short.parseShort(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = getRecordLocations(patients_map, short_ty, tableName, operator);
			break;
		case "dosage_mg":
			short_ty = Short.parseShort(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = getRecordLocations(dosage_mg_map, short_ty, tableName, operator);
			break;
		case "reading":
			float_ty = Short.parseShort(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = getRecordLocations(reading_map, float_ty, tableName, operator);
			break;
		case "double_blind":
			boolean_ty = Boolean.parseBoolean(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = getRecordLocations(double_blind_map, boolean_ty, tableName, operator);
			break;
		case "controlled_study":
			boolean_ty = Boolean.parseBoolean(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = getRecordLocations(controlled_study_map, boolean_ty, tableName, operator);
			break;
		case "govt_funded":
			boolean_ty = Boolean.parseBoolean(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = getRecordLocations(govt_funded_map, boolean_ty, tableName, operator);
			break;
		case "fda_approved":
			boolean_ty = Boolean.parseBoolean(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = getRecordLocations(fda_approved_map, boolean_ty, tableName, operator);
		}

		return offset;
	}

	//select query
	public List<Record> queryRecords(Vector from, String operator, Vector operands, boolean negation, String dbLocation) {

		String tableName = from.elementAt(0).toString().toUpperCase();

		List<Long> offset = getRecordLocations(tableName, operator, operands, negation);

		long fileLocation;
		Record record;
		List<Record> result = new ArrayList<Record>() ;

		Iterator<Long> it = offset.iterator();
		while(it.hasNext()) {
			fileLocation = it.next();
			record = fetchRecordFromFile(fileLocation, tableName, dbLocation);
			if(record != null) {
				result.add(record);
			}
		}

		return result;
	}

	public void deleteIndexes(List<Long> offset) {
		deleteFromIndex(id_map, offset);
		deleteFromIndex(company_map, offset);
		deleteFromIndex(drug_id_map, offset);
		deleteFromIndex(trials_map, offset);
		deleteFromIndex(patients_map, offset);
		deleteFromIndex(dosage_mg_map, offset);
		deleteFromIndex(reading_map, offset);
		deleteFromIndex(double_blind_map, offset);
		deleteFromIndex(controlled_study_map, offset);
		deleteFromIndex(govt_funded_map, offset);
		deleteFromIndex(fda_approved_map, offset);
	}

	public <K> void deleteFromIndex(Map<K, List<Long>> map, List<Long> offset) {
		Long loc;
		
		List<K> keysToBeRemoved = new ArrayList<K>();
		
		Iterator<Long> it = offset.iterator();
		while(it.hasNext()) {
			loc = it.next();
			List<Long> locs;
			for(Map.Entry<K,List<Long>> entry : map.entrySet()) {
				K key = entry.getKey();
				locs = entry.getValue();
				locs.remove(loc);
				
				if(locs.isEmpty()) {
					keysToBeRemoved.add(key);
				}
			}
		}
		
		Iterator<K> keyIterator = keysToBeRemoved.iterator();
		while(keyIterator.hasNext()) {
			map.remove(keyIterator.next());
		}
	}

	private void performSoftDelete(List<Long> offset, String tableName, String dbLocation) {
		RandomAccessFile raf = null;
		File file = new File(dbLocation+"/"+tableName+".db");
		byte[] b;
		long fileLocation;
		long recordOffset;

		Iterator<Long> it = offset.iterator();
		try {
			raf = new RandomAccessFile(file, "rw");

			while(it.hasNext())
			{
				recordOffset = it.next();
				raf.seek(recordOffset);

				raf.readInt();
				int varcharLength = raf.readByte();
				b = new byte[varcharLength];
				raf.read(b);

				b = new byte[6];
				raf.read(b);

				raf.readShort();

				raf.readShort();

				raf.readShort();

				raf.readFloat();

				fileLocation = raf.getFilePointer();

				byte byt = raf.readByte();

				raf.seek(fileLocation);

				byte delByte = 0;
				delByte = (byte) (delByte | (1 << 7));

				byt = (byte) (byt | delByte);

				raf.write(byt);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				raf.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void deleteRecord(String tableName, String operator, Vector operands, boolean negation, String dbLocation) {
		//check if index has to be deleted
		List<Long> offset = getRecordLocations(tableName, operator, operands, negation);
		performSoftDelete(offset, tableName, dbLocation);
		deleteIndexes(offset);
		updateIndexes(tableName, dbLocation);
		System.out.println(offset.size()+" row(s) deleted");
	}

	public void deleteAllRecords(String from, String dbLocation) {
		int count = 0;
		List<Long> offset = new ArrayList<Long>();
		for(Map.Entry<Integer,List<Long>> entry : id_map.entrySet()) {
			performSoftDelete(entry.getValue(), from, dbLocation);
			offset.add(entry.getValue().get(0));
			count = count + entry.getValue().size();
		}
		
		deleteIndexes(offset);
		updateIndexes(from, dbLocation);
		
		System.out.println(count+" row(s) deleted");
	}
	
	private <K> Map<K, List<Long>> getIndexFromFile(Class<K> cls, String idxFileName, String dbLocation) {
		FileInputStream fis;
		ObjectInputStream ois = null;
		File file;

		Map<K, List<Long>> map = null;

		file = new File(dbLocation+"/"+idxFileName);
		try {
			fis = new FileInputStream(file);
			ois = new ObjectInputStream(fis);
			map =  (TreeMap<K, List<Long>>) ois.readObject();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				ois.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return map;
	}

	public void populateIndexes(String name, String dbLocation) {
		String idxFileName = null;

		for(int i=0;i<FILE_HEADER_MAPPING.length;i++)
		{
			switch(FILE_HEADER_MAPPING[i])
			{
			case "id":
				idxFileName = name+".id.ndx";
				id_map = getIndexFromFile(Integer.class, idxFileName, dbLocation);
				break;
			case "company":
				idxFileName = name+".company.ndx";
				company_map = getIndexFromFile(String.class, idxFileName, dbLocation);
				break;
			case "drug_id":
				idxFileName = name+".drug_id.ndx";
				drug_id_map = getIndexFromFile(String.class, idxFileName, dbLocation);
				break;
			case "trials":
				idxFileName = name+".trials.ndx";
				trials_map = getIndexFromFile(Short.class, idxFileName, dbLocation);
				break;
			case "patients":
				idxFileName = name+".patients.ndx";
				patients_map = getIndexFromFile(Short.class, idxFileName, dbLocation);
				break;
			case "dosage_mg":
				idxFileName = name+".dosage_mg.ndx";
				dosage_mg_map = getIndexFromFile(Short.class, idxFileName, dbLocation);
				break;
			case "reading":
				idxFileName = name+".reading.ndx";
				reading_map = getIndexFromFile(Float.class, idxFileName, dbLocation);
				break;
			case "double_blind":
				idxFileName = name+".double_blind.ndx";
				double_blind_map = getIndexFromFile(Boolean.class, idxFileName, dbLocation);
				break;
			case "controlled_study":
				idxFileName = name+".controlled_study.ndx";
				controlled_study_map = getIndexFromFile(Boolean.class, idxFileName, dbLocation);
				break;
			case "govt_funded":
				idxFileName = name+".govt_funded.ndx";
				govt_funded_map = getIndexFromFile(Boolean.class, idxFileName, dbLocation);
				break;
			case "fda_approved":
				idxFileName = name+".fda_approved.ndx";
				fda_approved_map = getIndexFromFile(Boolean.class, idxFileName, dbLocation);
			}
		}
	}

	public List<Record> queryAllRecords(Vector from, String dbLocation) {
		List<Long> offset = new ArrayList<Long>();
		
		String tableName = from.elementAt(0).toString().toUpperCase();
		
		for(Map.Entry<Integer,List<Long>> entry : id_map.entrySet()) {
			offset.add(entry.getValue().get(0));
		}
		
		long fileLocation;
		Record record;
		List<Record> result = new ArrayList<Record>() ;

		Iterator<Long> it = offset.iterator();
		while(it.hasNext()) {
			fileLocation = it.next();
			record = fetchRecordFromFile(fileLocation, tableName, dbLocation);
			if(record != null) {
				result.add(record);
			}
		}

		return result;
	}
	
	public boolean validatePKConstraint(Record record) {

		int id = record.getId();

		if(id_map.containsKey(id)) {
			return false;
		}

		return true;

	}

	public boolean addRecord(Record record, String name, String dbLocation)
	{
		byte commonByte = 0x00;
		RandomAccessFile raf = null;

		File f = new File(dbLocation+"/"+name+".db");

		try {
			raf = new RandomAccessFile(f, "rw");

			long fileLength = f.length();
			raf.seek(fileLength);

			raf.writeInt(record.getId());
			createIndex(id_map, record.getId(), fileLength);

			raf.write(record.getCompany().length());
			raf.writeBytes(record.getCompany());
			createIndex(company_map, record.getCompany(), fileLength);

			raf.writeBytes(record.getDrug_id());
			createIndex(drug_id_map, record.getDrug_id(), fileLength);

			raf.writeShort(record.getTrials());
			createIndex(trials_map, record.getTrials(), fileLength);

			raf.writeShort(record.getPatients());
			createIndex(patients_map, record.getPatients(), fileLength);

			raf.writeShort(record.getDosage_mg());
			createIndex(dosage_mg_map, record.getDosage_mg(), fileLength);

			raf.writeFloat(record.getReading());
			createIndex(reading_map, record.getReading(), fileLength);

			if(record.isDouble_blind())
			{
				commonByte = (byte)(commonByte | double_blind_mask);
			}
			createIndex(double_blind_map, record.isDouble_blind(), fileLength);

			if(record.isControlled_study())
			{
				commonByte = (byte)(commonByte | controlled_study_mask);
			}
			createIndex(controlled_study_map, record.isControlled_study(), fileLength);

			if(record.isGovt_funded())
			{
				commonByte = (byte)(commonByte | govt_funded_mask);
			}
			createIndex(govt_funded_map, record.isGovt_funded(), fileLength);

			if(record.isFda_approved())
			{
				commonByte = (byte)(commonByte | fda_approved_mask);
			}
			createIndex(fda_approved_map, record.isFda_approved(), fileLength);

			raf.write(commonByte);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} finally {
			try {
				raf.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		}

		return true;
	}

	public boolean addBulkRecords(ArrayList<Record> records, String name, String dbLocation) {
		Iterator<Record> it = records.iterator();
		Record record;

		id_map = new TreeMap<Integer, List<Long>>();
		company_map = new TreeMap<String, List<Long>>();
		drug_id_map = new TreeMap<String, List<Long>>();
		trials_map = new TreeMap<Short, List<Long>>();
		patients_map = new TreeMap<Short, List<Long>>();
		dosage_mg_map = new TreeMap<Short, List<Long>>();
		reading_map = new TreeMap<Float, List<Long>>();
		double_blind_map = new TreeMap<Boolean, List<Long>>();
		controlled_study_map = new TreeMap<Boolean, List<Long>>();
		govt_funded_map = new TreeMap<Boolean, List<Long>>();
		fda_approved_map = new TreeMap<Boolean, List<Long>>();

		while(it.hasNext()) {
			record = it.next();
			addRecord(record, name, dbLocation);
		}

		return true;
	}

	public <K> void createIndex(Map<K, List<Long>> map, K value, long fileLength) {
		List<Long> list;
		if(!map.containsKey(value))
		{
			list = new ArrayList<Long>();
			map.put(value, list);
		}

		map.get(value).add(fileLength);
	}

	public void updateIndexes(String name, String dbLocation) {
		updateIndex(id_map, dbLocation+"/"+name+".id");
		updateIndex(company_map, dbLocation+"/"+name+".company");
		updateIndex(drug_id_map, dbLocation+"/"+name+".drug_id");
		updateIndex(trials_map, dbLocation+"/"+name+".trials");
		updateIndex(patients_map, dbLocation+"/"+name+".patients");
		updateIndex(dosage_mg_map, dbLocation+"/"+name+".dosage_mg");
		updateIndex(reading_map, dbLocation+"/"+name+".reading");
		updateIndex(double_blind_map, dbLocation+"/"+name+".double_blind");
		updateIndex(controlled_study_map, dbLocation+"/"+name+".controlled_study");
		updateIndex(govt_funded_map, dbLocation+"/"+name+".govt_funded");
		updateIndex(fda_approved_map, dbLocation+"/"+name+".fda_approved");
	}

	public <K,V> void updateIndex(Map<K, V> map, String name) {
		File idxFile = new File(name+".ndx");
		FileOutputStream fos;
		ObjectOutputStream oos = null;
		try {
			fos = new FileOutputStream(idxFile);
			oos = new ObjectOutputStream(fos);
			oos.writeObject(map);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				oos.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void importCSV(File file, String dbLocation) {
		FileReader fileReader = null;
		CSVParser csvFileParser = null;

		int int_ty;
		String string_ty;
		short short_ty;
		float float_ty;

		ArrayList<Record> records = new ArrayList<Record>();

		CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING);

		try {
			fileReader = new FileReader(file);
			csvFileParser = new CSVParser(fileReader, csvFileFormat);

			List<CSVRecord> csvRecords = csvFileParser.getRecords();

			String name = file.getName();
			int pos = name.lastIndexOf(".");
			if (pos > 0)
			{
				name = name.substring(0, pos);
			}

			for (int i = 1; i < csvRecords.size(); i++) {
				Record dbRecord = new Record();

				CSVRecord record = csvRecords.get(i);

				int_ty = Integer.parseInt(record.get(ID));
				dbRecord.setId(int_ty);

				string_ty = record.get(COMPANY);
				dbRecord.setCompany(string_ty);

				string_ty = record.get(DRUG_ID);
				dbRecord.setDrug_id(string_ty);

				short_ty = Short.parseShort(record.get(TRIALS));
				dbRecord.setTrials(short_ty);

				short_ty = Short.parseShort(record.get(PATIENTS));
				dbRecord.setPatients(short_ty);

				short_ty = Short.parseShort(record.get(DOSAGE_MG));
				dbRecord.setDosage_mg(short_ty);

				float_ty = Float.parseFloat(record.get(READING));
				dbRecord.setReading(float_ty);

				string_ty = record.get(DOUBLE_BLIND);
				if(string_ty.equalsIgnoreCase("true"))
				{
					dbRecord.setDouble_blind(true);
				}

				string_ty = record.get(CONTROLLED_STUDY);
				if(string_ty.equalsIgnoreCase("true"))
				{
					dbRecord.setControlled_study(true);
				}

				string_ty = record.get(GOVT_FUNDED);
				if(string_ty.equalsIgnoreCase("true"))
				{
					dbRecord.setGovt_funded(true);
				}

				string_ty = record.get(FDA_APPROVED);
				if(string_ty.equalsIgnoreCase("true"))
				{
					dbRecord.setFda_approved(true);
				}

				records.add(dbRecord);
			}

			addBulkRecords(records, name, dbLocation);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {

		MyDatabase myDb = new MyDatabase();
		QueryProcessor qp = new QueryProcessor(myDb);

		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		int message = 1;

		String directory = null;

		System.out.println("Welcome to Pharma Trials Database");
		System.out.print("Enter directory where database is located or you want to import to: ");
		try {
			directory = bufferedReader.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		File dir = new File(directory);

		if(!dir.exists()) {
			dir.mkdir();
		}

		qp.setDbLocation(dir.getAbsolutePath());

		if(dir.isDirectory()) {
			String command;

			while(true) {
				System.out.println();
				System.out.print("prompt> ");
				try {
					command = bufferedReader.readLine();
					message = qp.processQuery(command);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				if(message == -1) {
					System.out.println("Bye");
					System.exit(0);
				}
			}
		}
	}
}
