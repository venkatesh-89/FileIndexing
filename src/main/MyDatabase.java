package main;

import java.io.*;
import java.util.HashMap;

import com.opencsv.CSVReader;

public class MyDatabase {

	//final static int NUM_OF_FILES = 11;
	final static String CSV_FILE_NAME = "/Users/venkat/Documents/workspace/FileIndexing/PHARMA_TRIALS_1000B.csv";
	final static String TABLE_NAME = "PHARMA_TRIALS_1000B";
	final static String TABLE_FILE = "PHARMA_TRIALS_1000B.db";
	
	final static byte doubleBlindMask = 8; 		// binary 0000 1000
	final static byte controlledStudyMask = 4; 	// binary 0000 0100
	final static byte govtFundedMask = 2; 		// binary 0000 0010
	final static byte fdaApprovedMask = 1; 		// binary 0000 0001
	
	static HashMap<Integer, Long> idMap;
	static HashMap<String, String> companyMap;
	static HashMap<String, String> drugIdMap;
	static HashMap<Integer, String> trialsMap;
	static HashMap<Integer, String> patientsMap; 
	static HashMap<Integer, String> dosageMgMap;
	static HashMap<Float, String> readingMap;
	static HashMap<Boolean, String> doubleBlindMap;
	static HashMap<Boolean, String> controlledStudyMap;
	static HashMap<Boolean, String> govtFundedMap;
	static HashMap<Boolean, String> fdaApprovedMap;
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args){
		
		BufferedReader bReader = new BufferedReader(new InputStreamReader(System.in));
		
		int option = showMainMenu(bReader);
		
		if (option == 1){
			System.out.print("prompt > ");
			try {
				String query = bReader.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			} 
			csvToBinary();
		}
		
		//readBinaryFile();
		
		Object objMap = new HashMap<>();
		idMap = (HashMap<Integer, Long>) readHashMap(TABLE_NAME + ".id.ndx", objMap);
		companyMap = (HashMap<String, String>) readHashMap(TABLE_NAME + ".company.ndx", objMap);
		drugIdMap = (HashMap<String, String>) readHashMap(TABLE_NAME + ".drug_id.ndx", objMap);
		trialsMap = (HashMap<Integer, String>) readHashMap(TABLE_NAME + ".trials.ndx", objMap);
		patientsMap = (HashMap<Integer, String>) readHashMap(TABLE_NAME + ".patients.ndx", objMap);
		dosageMgMap = (HashMap<Integer, String>) readHashMap(TABLE_NAME + ".dosage_mg.ndx", objMap);
		readingMap = (HashMap<Float, String>) readHashMap(TABLE_NAME + ".reading.ndx", objMap);
		doubleBlindMap = (HashMap<Boolean, String>) readHashMap(TABLE_NAME + ".double_blind.ndx", objMap);
		controlledStudyMap = (HashMap<Boolean, String>) readHashMap(TABLE_NAME + ".controlled_study.ndx", objMap);
		govtFundedMap = (HashMap<Boolean, String>) readHashMap(TABLE_NAME + ".govt_funded.ndx", objMap);
		fdaApprovedMap = (HashMap<Boolean, String>) readHashMap(TABLE_NAME + ".fda_approved.ndx", objMap);
		
		
	}
	
	private static int showMainMenu(BufferedReader bReader){
		int out = 0;
		while (true) {
			System.out.println("\n");
			System.out.println("***********************************************");
			System.out.println("**************** Main Menu ********************");
			System.out.println("***********************************************");
			System.out.println("1 : Import");
			System.out.println("2 : Query");
			System.out.println("3 : Insert");
			System.out.println("4 : Delete");
			System.out.println("5 : Exit");

			System.out.print("\nEnter option : ");
			try{
				out = Integer.parseInt(bReader.readLine());
			}
			catch(Exception e){
				System.out.println("Please Select the correct option");
				out = 0;
			}
			
			if (out == 5)
			{
				System.out.println("\nBye Bye....!!!");
				System.exit(0);
			}
			if (out != 0)
				break;
		}

		return out;
	}
	
	private static void csvToBinary(){
		
		//Declaring HashMaps
		idMap = new HashMap<>();
		companyMap = new HashMap<>();
		drugIdMap = new HashMap<>();
		trialsMap = new HashMap<>();
		patientsMap = new HashMap<>();
		dosageMgMap = new HashMap<>();
		readingMap = new HashMap<>();
		doubleBlindMap = new HashMap<>();
		controlledStudyMap = new HashMap<>();
		govtFundedMap = new HashMap<>();
		fdaApprovedMap = new HashMap<>();
		
		File file = null;
		CSVReader reader = null;
		RandomAccessFile randomAccessFile = null;
		
		try{
			file = new File(CSV_FILE_NAME);
			
			reader = new CSVReader(new FileReader(file));
			
			randomAccessFile = new RandomAccessFile(TABLE_FILE, "rw");
			
			reader.readNext(); // Reading header
			String[] line = reader.readNext();
			long filePointer = 0;
			
			while(line != null){
				int id = Integer.parseInt(line[0]);
				String company = line[1];
				String drugId = line[2];
				int trials =  Integer.parseInt(line[3]);
				int patients =  Integer.parseInt(line[4]);
				int dosageMg =  Integer.parseInt(line[5]);
				float reading = Float.parseFloat(line[6]);
				boolean doubleBlind = Boolean.parseBoolean(line[7]);
				boolean controlledStudy = Boolean.parseBoolean(line[8]);
				boolean govtFunded = Boolean.parseBoolean(line[9]);
				boolean fdaApproved = Boolean.parseBoolean(line[10]);
				
				/***Converting 4 boolean columns to integer */
				byte booleanSet = 0;
				
				if (doubleBlind)
					booleanSet = (byte) (booleanSet | doubleBlindMask);
				
				if (controlledStudy)
					booleanSet = (byte) (booleanSet | controlledStudyMask);
				
				if (govtFunded)
					booleanSet = (byte) (booleanSet | govtFundedMask);
				
				if (fdaApproved)
					booleanSet = (byte) (booleanSet | fdaApprovedMask);
				
//				System.out.println(id + "," + company + "," + drugId + "," + trials + "," + patients + "," + dosageMg 
//						+ "," + reading + "," + doubleBlind + "," + controlledStudy + "," + govtFunded + "," + fdaApproved
//						+ "," + booleanSet);
				
				
				filePointer = randomAccessFile.getFilePointer();
				
				randomAccessFile.writeShort(id);
				randomAccessFile.write(company.length());
				randomAccessFile.writeBytes(company);
				randomAccessFile.writeBytes(drugId);
				randomAccessFile.writeShort(trials);
				randomAccessFile.writeShort(patients);
				randomAccessFile.writeShort(dosageMg);
				randomAccessFile.writeFloat(reading);
				randomAccessFile.write(booleanSet);
				
				/**Populate HashMaps */
				idMap.put(id, filePointer);
				
				if (companyMap.containsKey(company)){
					companyMap.put(company, companyMap.get(company) + "," + String.valueOf(filePointer));
				}
				else
					companyMap.put(company, String.valueOf(filePointer));
				
				if (drugIdMap.containsKey(drugId)){
					drugIdMap.put(drugId, drugIdMap.get(drugId) + "," + String.valueOf(filePointer));
				}
				else
					drugIdMap.put(drugId, String.valueOf(filePointer));
				
				if (trialsMap.containsKey(trials)){
					trialsMap.put(trials, trialsMap.get(trials) + "," + String.valueOf(filePointer));
				}
				else
					trialsMap.put(trials, String.valueOf(filePointer));
				
				if (patientsMap.containsKey(patients)){
					patientsMap.put(patients, patientsMap.get(patients) + "," + String.valueOf(filePointer));
				}
				else
					patientsMap.put(patients, String.valueOf(filePointer));
				
				if (dosageMgMap.containsKey(dosageMg)){
					dosageMgMap.put(dosageMg, dosageMgMap.get(dosageMg) + "," + String.valueOf(filePointer));
				}
				else
					dosageMgMap.put(dosageMg, String.valueOf(filePointer));
				
				if (readingMap.containsKey(reading)){
					readingMap.put(reading, readingMap.get(reading) + "," + String.valueOf(filePointer));
				}
				else
					readingMap.put(reading, String.valueOf(filePointer));
				
				if (doubleBlindMap.containsKey(doubleBlind)){
					doubleBlindMap.put(doubleBlind, doubleBlindMap.get(doubleBlind) + "," + String.valueOf(filePointer));
				}
				else
					doubleBlindMap.put(doubleBlind, String.valueOf(filePointer));
				
				if (controlledStudyMap.containsKey(controlledStudy)){
					controlledStudyMap.put(controlledStudy, controlledStudyMap.get(controlledStudy) + "," + String.valueOf(filePointer));
				}
				else
					controlledStudyMap.put(controlledStudy, String.valueOf(filePointer));
				
				if (govtFundedMap.containsKey(govtFunded)){
					govtFundedMap.put(govtFunded, govtFundedMap.get(govtFunded) + "," + String.valueOf(filePointer));
				}
				else
					govtFundedMap.put(govtFunded, String.valueOf(filePointer));
				
				if (fdaApprovedMap.containsKey(fdaApproved)){
					fdaApprovedMap.put(fdaApproved, fdaApprovedMap.get(fdaApproved) + "," + String.valueOf(filePointer));
				}
				else
					fdaApprovedMap.put(fdaApproved, String.valueOf(filePointer));
				
				line = reader.readNext();
			}
			
			/**Write the hashMaps to index files */
			
			writeMapsToFile(TABLE_NAME + ".id.ndx", idMap);
			writeMapsToFile(TABLE_NAME + ".company.ndx", companyMap);
			writeMapsToFile(TABLE_NAME + ".drug_id.ndx", drugIdMap);
			writeMapsToFile(TABLE_NAME + ".trials.ndx", trialsMap);
			writeMapsToFile(TABLE_NAME + ".patients.ndx", patientsMap);
			writeMapsToFile(TABLE_NAME + ".dosage_mg.ndx", dosageMgMap);
			writeMapsToFile(TABLE_NAME + ".reading.ndx", readingMap);
			writeMapsToFile(TABLE_NAME + ".double_blind.ndx", doubleBlindMap);
			writeMapsToFile(TABLE_NAME + ".controlled_study.ndx", controlledStudyMap);
			writeMapsToFile(TABLE_NAME + ".govt_funded.ndx", govtFundedMap);
			writeMapsToFile(TABLE_NAME + ".fda_approved.ndx", fdaApprovedMap);
			
			/**End of write hashMaps*/
			
			reader.close();
			randomAccessFile.close();
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
		finally{
					
			
		}
	}
	
	
	private static void writeMapsToFile(String fileName, Object obj){
		File file = null;
		FileOutputStream fileOutStream = null;
		ObjectOutputStream objOutStream = null;
		try{
			file = new File(fileName);
			fileOutStream = new FileOutputStream(file);
			objOutStream = new ObjectOutputStream(fileOutStream);
			
			objOutStream.writeObject(obj);
			objOutStream.flush();
			objOutStream.close();
			fileOutStream.close();
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static void readBinaryFile(){
		RandomAccessFile randomAccessFile = null;
		try{
			randomAccessFile = new RandomAccessFile(TABLE_FILE, "r");
			
			while(true){
				int id = randomAccessFile.readShort();
				int companyLength = randomAccessFile.read();
				byte[] company = new byte[companyLength];
				randomAccessFile.read(company, 0, companyLength);
				byte[] drugId = new byte[6];
				randomAccessFile.read(drugId, 0, 6);
				int trials =  randomAccessFile.readShort();
				int patients =  randomAccessFile.readShort();
				int dosageMg =  randomAccessFile.readShort();
				float reading = randomAccessFile.readFloat();
				byte booleanSet = randomAccessFile.readByte();
				
				boolean doubleBlind = false, 
						controlledStudy = false, 
						govtFunded = false, 
						fdaApproved = false;
				
				if (( booleanSet & doubleBlindMask ) == doubleBlindMask)
					doubleBlind = true;
				
				if (( booleanSet & controlledStudyMask ) == controlledStudyMask)
					controlledStudy = true;
				
				if (( booleanSet & govtFundedMask ) == govtFundedMask)
					govtFunded = true;
				
				if (( booleanSet & fdaApprovedMask ) == fdaApprovedMask)
					fdaApproved = true;
				
				System.out.println(id + "," + new String(company) + "," + new String(drugId) + "," + trials + "," + patients + "," + dosageMg 
						+ "," + reading + "," + doubleBlind + "," + controlledStudy + "," + govtFunded + "," + fdaApproved
						+ "," + booleanSet);
				
			}
			//randomAccessFile.close();
		}
		catch(Exception e){
			e.printStackTrace();
			
		}
	}
	
	@SuppressWarnings("unchecked")
	private static Object readHashMap(String fileName, Object objMap){
		File file = null;
		FileInputStream fileInStream = null;
		ObjectInputStream objInStream = null;
		
		try{
			file = new File(fileName);
			fileInStream = new FileInputStream(file);
			objInStream = new ObjectInputStream(fileInStream);
			
			objMap = (HashMap<Integer, Long>) objInStream.readObject();
			
			objInStream.close();
			fileInStream.close();
		
		}
		catch(Exception e){
			System.out.println(e.getMessage());
			//e.printStackTrace();
		}
		return objMap;
	}
	
}
