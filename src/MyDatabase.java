import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;


public class MyDatabase {

	private static final String [] FILE_HEADER_MAPPING = {"id","company","drug_id","trials","patients","dosage_mg","reading","double_blind","controlled_study","govt_funded","fda_approved"};
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
	
	private HashMap<Integer, Long> id_map;
	private HashMap<String, List<Long>> company_map;
	private HashMap<String, List<Long>> drug_id_map;
	private HashMap<Short, List<Long>> trials_map;
	private HashMap<Short, List<Long>> patients_map;
	private HashMap<Short, List<Long>> dosage_mg_map;
	private HashMap<Float, List<Long>> reading_map;
	private HashMap<Boolean, List<Long>> double_blind_map;
	private HashMap<Boolean, List<Long>> controlled_study_map;
	private HashMap<Boolean, List<Long>> govt_funded_map;
	private HashMap<Boolean, List<Long>> fda_approved_map;

	public <K,V> List<HashMap<K, V>> getIndexes() {
		return null;
		
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

			raf.write(record.getCompany().length());
			raf.writeBytes(record.getCompany());

			raf.writeBytes(record.getDrug_id());

			raf.writeShort(record.getTrials());

			raf.writeShort(record.getPatients());

			raf.writeShort(record.getDosage_mg());

			raf.writeFloat(record.getReading());

			if(record.isDouble_blind())
			{
				commonByte = (byte)(commonByte | double_blind_mask);
			}

			if(record.isControlled_study())
			{
				commonByte = (byte)(commonByte | controlled_study_mask);
			}

			if(record.isGovt_funded())
			{
				commonByte = (byte)(commonByte | govt_funded_mask);
			}

			if(record.isFda_approved())
			{
				commonByte = (byte)(commonByte | fda_approved_mask);
			}

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
		
		updateIndexes(name);
		
		return true;
	}

	public boolean addBulkRecords(ArrayList<Record> records, String name) {
		Iterator<Record> it = records.iterator();
		Record record;

		byte commonByte = 0x00;
		RandomAccessFile raf = null;
		long fileLength = 0;
		
		id_map = new HashMap<Integer, Long>(); //since primary key only 1 value
		company_map = new HashMap<String, List<Long>>(); //since not primary key List of values
		drug_id_map = new HashMap<String, List<Long>>(); //since not primary key List of values
		trials_map = new HashMap<Short, List<Long>>(); //since not primary key List of values
		patients_map = new HashMap<Short, List<Long>>(); //since not primary key List of values
		dosage_mg_map = new HashMap<Short, List<Long>>(); //since not primary key List of values
		reading_map = new HashMap<Float, List<Long>>(); //since not primary key List of values
		double_blind_map = new HashMap<Boolean, List<Long>>(); //since not primary key List of values
		controlled_study_map = new HashMap<Boolean, List<Long>>(); //since not primary key List of values
		govt_funded_map = new HashMap<Boolean, List<Long>>(); //since not primary key List of values
		fda_approved_map = new HashMap<Boolean, List<Long>>(); //since not primary key List of values

		File f = new File(name+".db");

		try {
			raf = new RandomAccessFile(f, "rw");

			while(it.hasNext()) {
				record = it.next();

				fileLength = f.length(); //records starting location
				raf.seek(fileLength);

				raf.writeInt(record.getId());
				id_map.put(record.getId(), fileLength);

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
			}
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
		
		updateIndexes(name);
		
		return true;
	}
	
	public <K> void createIndex(HashMap<K, List<Long>> map, K value, long fileLength) {
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
	
	public <K,V> void updateIndex(HashMap<K, V> map, String name) {
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

	public static void main(String[] args) {
		MyDatabase myDb = new MyDatabase();

		File file = new File("/Users/roshan/Documents/UTD/Fall 2015/Database/Programming Project 2/PHARMA_TRIALS_1000B.csv");
		myDb.importCSV(file);
	}
}
