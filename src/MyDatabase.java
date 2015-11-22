import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
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

	public void importCSV(File file) {
		FileReader fileReader = null;
		CSVParser csvFileParser = null;
		RandomAccessFile randomFile = null;

		int int_ty;
		String string_ty;
		short short_ty;
		float float_ty;
		byte commonByte = 0x00;

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

			randomFile = new RandomAccessFile(name+".db", "rw");

			for (int i = 1; i < csvRecords.size(); i++) {
				CSVRecord record = csvRecords.get(i);
				int_ty = Integer.parseInt(record.get(ID));
				randomFile.writeInt(int_ty);

				string_ty = record.get(COMPANY);
				randomFile.write(string_ty.length());
				randomFile.writeBytes(string_ty);

				string_ty = record.get(DRUG_ID);
				randomFile.writeBytes(string_ty);

				short_ty = Short.parseShort(record.get(TRIALS));
				randomFile.writeShort(short_ty);

				short_ty = Short.parseShort(record.get(PATIENTS));
				randomFile.writeShort(short_ty);

				short_ty = Short.parseShort(record.get(DOSAGE_MG));
				randomFile.writeShort(short_ty);

				float_ty = Float.parseFloat(record.get(READING));
				randomFile.writeFloat(float_ty);
				
				string_ty = record.get(DOUBLE_BLIND);
				if(string_ty.equalsIgnoreCase("true"))
				{
					commonByte = (byte)(commonByte | double_blind_mask);
				}
				
				string_ty = record.get(CONTROLLED_STUDY);
				if(string_ty.equalsIgnoreCase("true"))
				{
					commonByte = (byte)(commonByte | controlled_study_mask);
				}
				
				string_ty = record.get(GOVT_FUNDED);
				if(string_ty.equalsIgnoreCase("true"))
				{
					commonByte = (byte)(commonByte | govt_funded_mask);
				}
				
				string_ty = record.get(FDA_APPROVED);
				if(string_ty.equalsIgnoreCase("true"))
				{
					commonByte = (byte)(commonByte | fda_approved_mask);
				}
				
				randomFile.write(commonByte);
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				randomFile.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


	}

	public static void main(String[] args) {
		MyDatabase myDb = new MyDatabase();

		File file = new File("/Users/roshan/Documents/UTD/Fall 2015/Database/Programming Project 2/PHARMA_TRIALS_1000B.csv");
		myDb.importCSV(file);
	}
}
