import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
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
import org.gibello.zql.ZExpression;


public class MyDatabase {

	//Hardcoing. In actual implemenation will be read from metadata.
	public static final String [] FILE_HEADER_MAPPING = {"id","company","drug_id","trials","patients","dosage_mg","reading","double_blind","controlled_study","govt_funded","fda_approved"};
	public static final String [] DATA_TYPE_MAPPING = {"int","string","string","short","short","short","float","boolean","boolean","boolean","boolean"};

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

	public MyDatabase(String name) {
		//fetch all the indexes into memory for faster query processing.
		populateIndexes(name);
	}

	public <K extends Comparable<K>> ArrayList<Record> getRecords(Map<K, List<Long>> map, K indexValue, String tableName, String operator) {
		List<Long> value = null;
		Iterator<Long> it;
		long fileLocation;
		ArrayList<Record> resultSet = new ArrayList<Record>();
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
			}

			if(value != null) {
				it = value.iterator();
				while(it.hasNext()) {
					fileLocation = it.next();
					resultSet.add(fetchRecordFromFile(fileLocation, tableName));
				}
			}
			
			value = null;
		}
		return resultSet;
	}

	public Record fetchRecordFromFile(long offset, String tableName) {
		RandomAccessFile raf = null;
		File file = new File(tableName+".db");
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

	public void printResult(Vector columns, ArrayList<Record> result) {
		//mysql type output implementation
		Iterator<Record> it = result.iterator();
		while(it.hasNext()) {
			System.out.println(it.next());
		}

	}

	//select query
	public ArrayList<Record> queryRecords(Vector columns, Vector from, String operator, Vector operands, boolean negation) {

		String tableName = from.elementAt(0).toString().toUpperCase();

		String leftOperand = operands.elementAt(0).toString().toLowerCase();

		ArrayList<Record> result = new ArrayList<Record>();

		int int_ty;
		String string_ty;
		short short_ty;
		float float_ty;
		boolean boolean_ty;
		List<Long> offset = null;

		switch(leftOperand) {
		case "id":
			int_ty = Integer.parseInt(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = id_map.get(int_ty);
			result = getRecords(id_map, int_ty, tableName, operator);
			break;
		case "company":
			string_ty = QueryProcessor.trimQuotes(operands.elementAt(1).toString());
			offset = company_map.get(string_ty);
			result = getRecords(company_map, string_ty, tableName, operator);
			break;
		case "drug_id":
			string_ty = QueryProcessor.trimQuotes(operands.elementAt(1).toString());
			offset = drug_id_map.get(string_ty);
			result = getRecords(drug_id_map, string_ty, tableName, operator);
			break;
		case "trials":
			short_ty = Short.parseShort(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = trials_map.get(short_ty);
			result = getRecords(trials_map, short_ty, tableName, operator);
			break;
		case "patients":
			short_ty = Short.parseShort(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = patients_map.get(short_ty);
			result = getRecords(patients_map, short_ty, tableName, operator);
			break;
		case "dosage_mg":
			short_ty = Short.parseShort(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = dosage_mg_map.get(short_ty);
			result = getRecords(dosage_mg_map, short_ty, tableName, operator);
			break;
		case "reading":
			float_ty = Short.parseShort(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = reading_map.get(float_ty);
			result = getRecords(reading_map, float_ty, tableName, operator);
			break;
		case "double_blind":
			boolean_ty = Boolean.parseBoolean(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = double_blind_map.get(boolean_ty);
			result = getRecords(double_blind_map, boolean_ty, tableName, operator);
			break;
		case "controlled_study":
			boolean_ty = Boolean.parseBoolean(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = controlled_study_map.get(boolean_ty);
			result = getRecords(controlled_study_map, boolean_ty, tableName, operator);
			break;
		case "govt_funded":
			boolean_ty = Boolean.parseBoolean(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = govt_funded_map.get(boolean_ty);
			result = getRecords(govt_funded_map, boolean_ty, tableName, operator);
			break;
		case "fda_approved":
			boolean_ty = Boolean.parseBoolean(QueryProcessor.trimQuotes(operands.elementAt(1).toString()));
			offset = fda_approved_map.get(boolean_ty);
			result = getRecords(fda_approved_map, boolean_ty, tableName, operator);
		}

		return result;
	}

	private <K> Map<K, List<Long>> getIndexFromFile(Class<K> cls, String idxFileName) {
		FileInputStream fis;
		ObjectInputStream ois = null;
		File file;

		Map<K, List<Long>> map = null;

		file = new File(idxFileName);
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

	public void populateIndexes(String name) {
		String idxFileName = null;

		for(int i=0;i<FILE_HEADER_MAPPING.length;i++)
		{
			switch(FILE_HEADER_MAPPING[i])
			{
			case "id":
				idxFileName = name+".id.ndx";
				id_map = getIndexFromFile(Integer.class, idxFileName);
				break;
			case "company":
				idxFileName = name+".company.ndx";
				company_map = getIndexFromFile(String.class, idxFileName);
				break;
			case "drug_id":
				idxFileName = name+".drug_id.ndx";
				drug_id_map = getIndexFromFile(String.class, idxFileName);
				break;
			case "trials":
				idxFileName = name+".trials.ndx";
				trials_map = getIndexFromFile(Short.class, idxFileName);
				break;
			case "patients":
				idxFileName = name+".patients.ndx";
				patients_map = getIndexFromFile(Short.class, idxFileName);
				break;
			case "dosage_mg":
				idxFileName = name+".dosage_mg.ndx";
				dosage_mg_map = getIndexFromFile(Short.class, idxFileName);
				break;
			case "reading":
				idxFileName = name+".reading.ndx";
				reading_map = getIndexFromFile(Float.class, idxFileName);
				break;
			case "double_blind":
				idxFileName = name+".double_blind.ndx";
				double_blind_map = getIndexFromFile(Boolean.class, idxFileName);
				break;
			case "controlled_study":
				idxFileName = name+".controlled_study.ndx";
				controlled_study_map = getIndexFromFile(Boolean.class, idxFileName);
				break;
			case "govt_funded":
				idxFileName = name+".govt_funded.ndx";
				govt_funded_map = getIndexFromFile(Boolean.class, idxFileName);
				break;
			case "fda_approved":
				idxFileName = name+".fda_approved.ndx";
				fda_approved_map = getIndexFromFile(Boolean.class, idxFileName);
			}
		}
	}

	public void deleteRecord(long recordOffset) {

	}

	public boolean addRecord(Record record, String name)
	{
		byte commonByte = 0x00;
		RandomAccessFile raf = null;

		File f = new File(name+".db");

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

	public boolean addBulkRecords(ArrayList<Record> records, String name) {
		Iterator<Record> it = records.iterator();
		Record record;

		RandomAccessFile raf = null;

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

		File f = new File(name+".db");

		try {
			raf = new RandomAccessFile(f, "rw");

			while(it.hasNext()) {
				record = it.next();
				addRecord(record, name);
			}
		} catch (FileNotFoundException e) {
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

	public <K> void createIndex(Map<K, List<Long>> map, K value, long fileLength) {
		List<Long> list;
		if(!map.containsKey(value))
		{
			list = new ArrayList<Long>();
			map.put(value, list);
		}

		map.get(value).add(fileLength);
	}

	public void updateIndexes(String name) {
		updateIndex(id_map, name+".id");
		updateIndex(company_map, name+".company");
		updateIndex(drug_id_map, name+".drug_id");
		updateIndex(trials_map, name+".trials");
		updateIndex(patients_map, name+".patients");
		updateIndex(dosage_mg_map, name+".dosage_mg");
		updateIndex(reading_map, name+".reading");
		updateIndex(double_blind_map, name+".double_blind");
		updateIndex(controlled_study_map, name+".controlled_study");
		updateIndex(govt_funded_map, name+".govt_funded");
		updateIndex(fda_approved_map, name+".fda_approved");
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

	public void importCSV(File file) {
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

			addBulkRecords(records, name);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
