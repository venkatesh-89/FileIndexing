package main;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.opencsv.CSVReader;

public class MyDatabase {

	static String TABLE_NAME = "PHARMA_TRIALS_1000B";
	
	final static byte deleteMask = 16;			// binary 0001 0000
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

	static String[] indexFile = {".id.ndx", ".company.ndx", ".drug_id.ndx", ".trials.ndx", ".patients.ndx",
								".dosage_mg.ndx", ".reading.ndx", ".double_blind.ndx", ".controlled_study.ndx",
								".govt_funded.ndx", ".fda_approved.ndx"};

	static ArrayList<String> columnList = new ArrayList<String>();
	
	public static void main(String[] args){
		
		try{

			init();
			
			BufferedReader bReader = new BufferedReader(new InputStreamReader(System.in));
			while(true){
				int option = showMainMenu(bReader);
				String query = "";
				String[] querySet;
				
				if (option == 1){
					
					System.out.print("prompt > ");
					query = bReader.readLine();
					
					querySet =  query.split(" ");
					
					if (! (querySet[0]).equalsIgnoreCase("import")){
						System.out.println("Syntax Error:");
						System.out.println("Correct usage 'import file_name.csv'");
						System.out.print("Press enter to continue");
						bReader.readLine();
						continue;
					}
					else{
						String fileExt = (querySet[1]).substring((querySet[1]).indexOf('.') + 1);
						
						if (! (fileExt).equalsIgnoreCase("csv")){
							System.out.println("Error: In-correct file format");
							System.out.print("Press enter to continue");
							bReader.readLine();
							continue;
						}
						TABLE_NAME = (querySet[1]).trim().substring(0, (querySet[1]).indexOf('.') );
						
						csvToBinary((querySet[1]).trim());
						System.out.println("Import successful");
					}
				}
				
				if (option == 2){
					
					String fullQuery = getFullQuery(bReader, "").toLowerCase();
					int selectIndex, fromIndex, whereIndex, notIndex;
					boolean queryExec = true;
					String tableName = "", columnNames = "", whereClause, 
							fieldName = "", comparator = "", value = "";
					String[] columnSet, temp;
					
					System.out.println();
					
					String pattern1 = "select\\s.*from\\s.*where\\s.*";
					
					Pattern r1 = Pattern.compile(pattern1, Pattern.CASE_INSENSITIVE);
					
					Matcher m1 = r1.matcher(fullQuery);
					
					selectIndex = fullQuery.indexOf("select");
					fromIndex = fullQuery.indexOf("from");
					
					if(m1.matches()){
						
						whereIndex = fullQuery.indexOf("where");
						notIndex = fullQuery.indexOf("not");
						
						if (whereIndex == -1)
							tableName = fullQuery.substring(fromIndex + 4, fullQuery.length() - 1).trim();
						else
							tableName = fullQuery.substring(fromIndex + 4, whereIndex).trim();
						
						if (!new File(tableName + ".db").exists()){
							System.out.println("Table does not exists. Please import using import command");
							queryExec = false;
						}
						else{
							columnNames = fullQuery.substring(selectIndex + 6, fromIndex).trim();
							
							if (columnNames.contains(",")){
								columnSet = columnNames.split(Pattern.quote(","));
								for (String s: columnSet){
									s = s.trim();
									if (!columnList.contains(s)){
										System.out.println("Column '" + s + "' in Select clause does not exists");
										queryExec = false;
									}
								}
							}
							else{
								if (! columnNames.equals("*")){
									if (!columnList.contains(columnNames)){
										System.out.println("Column '" + columnNames + "' in Select clause does not exists");
										queryExec = false;
									}
								}
								
							}
						}
						
						if (whereIndex != -1){
							whereClause = fullQuery.substring(whereIndex);
							temp = whereClause.split(Pattern.quote(" "));
							if (temp.length < 4){
								System.out.println("Syntax error in WHERE clause");
								queryExec = false;
							}
							else{
								fieldName = (temp[1]).trim();
								if (!columnList.contains(fieldName)){
									System.out.println("Column '" + fieldName + "' in where clause does not exists");
									queryExec = false;
								}
								if (notIndex != -1)
									comparator = (temp[3]).trim();
								else
									comparator = (temp[2]).trim();
								value = whereClause.substring(whereClause.indexOf(comparator) + comparator.length(), 
										whereClause.length() - 1).trim();
							}
					
						}
						
						if(queryExec){
							//return the result
							boolean notFlag = (notIndex != -1) ? true : false;
							
							//Set the last flag as false as that is the delete flag
							printQueryResult(columnNames, tableName, fieldName, notFlag, comparator, value, false);
						}
					}
					else
						System.out.println("Syntax error. Please type the correct query");
					
				}
				
				if (option == 3){
					
					String fullQuery = getFullQuery(bReader, "").toLowerCase();
					String tableName = "", value = "";
					String[] valueSet;
					boolean queryExec = true;
					File file = null;
					RandomAccessFile randomAccessFile = null;
					
					System.out.println(fullQuery);
					
					String pattern1 = "insert\\s.*into\\s.*values\\s.*";
					
					Pattern r1 = Pattern.compile(pattern1, Pattern.CASE_INSENSITIVE);
					
					Matcher m1 = r1.matcher(fullQuery);
					
					if (m1.matches()){
						tableName = fullQuery.substring(fullQuery.indexOf("into") + 4, fullQuery.indexOf("values")).trim();
						
						file = new File(tableName + ".db"); 
						if (! file.exists()){
							System.out.println("Table does not exists. Please import using import command");
							queryExec = false;
						}
						else{
							value = fullQuery.substring(fullQuery.indexOf("values") + 6, fullQuery.indexOf(";")).trim().toUpperCase();
							if (value.contains("(") && value.contains(")")){
								int first = value.indexOf("(");
								int last = value.indexOf(")");
								value = value.substring(first + 1, last).trim();
							}
							valueSet = value.split(Pattern.quote(","));
							
							if (valueSet.length != columnList.size()){
								queryExec = false;
								System.out.println("Error: Too less values in INSERT statement");
							}
							
							if (queryExec){
								randomAccessFile = new RandomAccessFile(file, "rw");
								writeDataToFile(randomAccessFile, valueSet);
								writeAllMaps();
								System.out.println("1 row successfully inserted");
							}
						}
					}
					else{
						System.out.println("Syntax error. Please type the correct query");
						System.out.println("Eg: INSERT into table_name values (a,b,c,...);");
					}
				}
				
				if (option == 4){
					
					String fullQuery = getFullQuery(bReader, "").toLowerCase();
					int whereIndex, notIndex;
					boolean queryExec = true;
					String tableName = "", whereClause, 
							fieldName = "", comparator = "", value = "";
					String[] temp;
					
					File file = null;
					
					System.out.println();
					
					String pattern1 = "delete\\sfrom\\s.*where\\s.*";
					
					Pattern r1 = Pattern.compile(pattern1, Pattern.CASE_INSENSITIVE);
					
					Matcher m1 = r1.matcher(fullQuery);
					
					if (m1.matches()){
						whereIndex = fullQuery.indexOf("where");
						notIndex = fullQuery.indexOf("not");
						
						tableName = fullQuery.substring(fullQuery.indexOf("from") + 4, whereIndex).trim();
						
						file = new File(tableName + ".db");
						if (! file.exists()){
							System.out.println("Table does not exists. Please import using import command");
							queryExec = false;
						}
						else{
							if (whereIndex != -1){
								whereClause = fullQuery.substring(whereIndex, fullQuery.indexOf(";"));
								temp = whereClause.split(Pattern.quote(" "));
								if (temp.length < 4){
									System.out.println("Syntax error in WHERE clause");
									queryExec = false;
								}
								else{
									fieldName = (temp[1]).trim();
									if (!columnList.contains(fieldName)){
										System.out.println("Column '" + fieldName + "' in where clause does not exists");
										queryExec = false;
									}
									if (notIndex != -1)
										comparator = (temp[3]).trim();
									else
										comparator = (temp[2]).trim();
									
									value = whereClause.substring(whereClause.indexOf(comparator) + comparator.length(), 
											whereClause.length()).trim();
								}
							}
							
							if(queryExec){
								//return the result
								boolean notFlag = (notIndex != -1) ? true : false;
								
								//Set the last flag as true as that is the delete flag
								printQueryResult("", tableName, fieldName, notFlag, comparator, value, true);
							}
						}
					}
				}
				
			}
		
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	private static void printQueryResult(String columnNames, String tableName, 
			String fieldName, boolean notFlag, String comparator, String value, boolean deleteFlag){
		try{
			
			if (! fieldName.equals("")){
				switch(fieldName){
				case "id":
					searchInteger(columnNames, tableName, fieldName, notFlag, comparator, value, deleteFlag);
					break;
				case "company":
				case "drug_id":
					searchText(columnNames, tableName, fieldName, notFlag, comparator, value, deleteFlag);
					break;
				case "trials":
				case "patients":
				case "dosage_mg":
				case "reading":
					searchInteger(columnNames, tableName, fieldName, notFlag, comparator, value, deleteFlag);
					break;
				case "double_blind":
				case "controlled_study":
				case "govt_funded":
				case "fda_approved":
					searchText(columnNames, tableName, fieldName, notFlag, comparator, value, deleteFlag);
					break;
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static void searchInteger(String columnNames, String tableName, 
			String fieldName, boolean notFlag, String comparator, String value, boolean deleteFlag){
		
		String filePtr = "";
		int intValue = 0;
		float floatValue = 0;
		boolean lt = false,
				gt = false,
				le = false,
				ge = false,
				eq = false;
		try{
			switch(comparator){
			case "=":
				if (fieldName.equals("id")){
					intValue = Integer.parseInt(value);
					if (notFlag){
						for (Integer key : idMap.keySet()){
							if (key == intValue)
								continue;
							
							filePtr = String.valueOf(idMap.get(key));
							read(columnNames, tableName, filePtr, deleteFlag);
							
						}
					}
					else{
						if (idMap.containsKey(intValue)){
							filePtr = String.valueOf(idMap.get(intValue));
							read(columnNames, tableName, filePtr, deleteFlag);
						}
						else
							System.out.println("\nNo records found");
					}
				}
				else if (fieldName.equals("trials")){
					intValue = Integer.parseInt(value);
					if (notFlag){
						for(Integer key : trialsMap.keySet()){
							if (key == intValue)
								continue;
							
							filePtr = trialsMap.get(key);
							read(columnNames, tableName, filePtr, deleteFlag);
						}
					}
					else{
						if (trialsMap.containsKey(intValue)){
							filePtr = trialsMap.get(intValue);
							read(columnNames, tableName, filePtr, deleteFlag);
						}
						else
							System.out.println("\nNo records found");
					}
				}
				else if (fieldName.equals("patients")){
					intValue = Integer.parseInt(value);
					if (notFlag){
						for(Integer key : patientsMap.keySet()){
							if (key == intValue)
								continue;
							
							filePtr = patientsMap.get(key);
							read(columnNames, tableName, filePtr, deleteFlag);
						}
					}
					else{
						if (patientsMap.containsKey(intValue)){
							filePtr = patientsMap.get(intValue);
							read(columnNames, tableName, filePtr, deleteFlag);
						}
						else
							System.out.println("\nNo records found");
					}
				}
				else if (fieldName.equals("dosage_mg")){
					intValue = Integer.parseInt(value);
					if (notFlag){
						for(Integer key : dosageMgMap.keySet()){
							if (key == intValue)
								continue;
							
							filePtr = dosageMgMap.get(key);
							read(columnNames, tableName, filePtr, deleteFlag);
						}
					}
					else{
						if (dosageMgMap.containsKey(intValue)){
							filePtr = String.valueOf(dosageMgMap.get(intValue));
							read(columnNames, tableName, filePtr, deleteFlag);
						}
						else
							System.out.println("\nNo records found");
					}
				}
				else if (fieldName.equals("reading")){
					floatValue = Float.parseFloat(value);
					if (notFlag){
						for(Float key : readingMap.keySet()){
							if (key == floatValue)
								continue;
							
							filePtr = readingMap.get(key);
							read(columnNames, tableName, filePtr, deleteFlag);
						}
					}
					else{
						if (readingMap.containsKey(floatValue)){
							filePtr = String.valueOf(readingMap.get(floatValue));
							read(columnNames, tableName, filePtr, deleteFlag);
						}
						else
							System.out.println("\nNo records found");
					}
				}
				eq = true;
				break;
			
			case "<":
				if (notFlag)
					ge = true;
				else
					lt = true;
				break;
			case ">":
				if (notFlag)
					le = true;
				else
					gt = true;
				break;
			case "<=":
				if (notFlag)
					gt = true;
				else
					le = true;
				break;
			case ">=":
				if (notFlag)
					lt = true;
				else
					ge = true;
				break;
			}
			
			if (eq == false && (lt || gt || le || ge)){
				if (fieldName.equals("id")){
					intValue = Integer.parseInt(value);
					for (Integer key : idMap.keySet()) {
						if (lt || le){
							if ( (key < intValue) || (le && key == intValue)) 
								read(columnNames,tableName,String.valueOf(idMap.get(key)), deleteFlag);
						}
						else if (gt || ge){
							if ( (key > intValue) || (ge && key == intValue)) 
								read(columnNames,tableName,String.valueOf(idMap.get(key)), deleteFlag);
						}
					}
				}
				else if (fieldName.equals("trials")){
					intValue = Integer.parseInt(value);
					for (Integer key : trialsMap.keySet()) {
						if (lt || le){
							if ( (key < intValue) || (le && key == intValue)) 
								read(columnNames,tableName,trialsMap.get(key), deleteFlag);
						}
						else if (gt || ge){
							if ( (key > intValue) || (ge && key == intValue)) 
								read(columnNames,tableName,trialsMap.get(key), deleteFlag);
						}
					}
				}
				else if (fieldName.equals("patients")){
					intValue = Integer.parseInt(value);
					for (Integer key : patientsMap.keySet()) {
						if (lt || le){
							if ( (key < intValue) || (le && key == intValue)) 
								read(columnNames,tableName,patientsMap.get(key), deleteFlag);
						}
						else if (gt || ge){
							if ( (key > intValue) || (ge && key == intValue)) 
								read(columnNames,tableName,patientsMap.get(key), deleteFlag);
						}
					}
				}
				else if (fieldName.equals("dosage_mg")){
					intValue = Integer.parseInt(value);
					for (Integer key : dosageMgMap.keySet()) {
						if (lt || le){
							if ( (key < intValue) || (le && key == intValue)) 
								read(columnNames,tableName,dosageMgMap.get(key), deleteFlag);
						}
						else if (gt || ge){
							if ( (key > intValue) || (ge && key == intValue)) 
								read(columnNames,tableName,dosageMgMap.get(key), deleteFlag);
						}
					}
				}
				else if (fieldName.equals("reading")){
					floatValue = Float.parseFloat(value);
					for (Float key : readingMap.keySet()) {
						if (lt || le){
							if ( (key < floatValue) || (le && key == floatValue)) 
								read(columnNames,tableName,readingMap.get(key), deleteFlag);
						}
						else if (gt || ge){
							if ( (key > floatValue) || (ge && key == floatValue)) 
								read(columnNames,tableName,readingMap.get(key), deleteFlag);
						}
					}
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	private static void searchText(String columnNames, String tableName, 
			String fieldName, boolean notFlag, String comparator, String value, boolean deleteFlag){
		
		boolean read = false, 
				key;
		try{
			switch(comparator){
			case "=":
				read = true;
				break;
			case "<":
			case "<=":
			case ">":
			case ">=":
				read = false;
				System.out.println("Syntax error: Invalid comparator in WHERE clause");
				System.out.println("Cannot compare text with given operator");
				break;
			}
			
			if (read){
				if (value.contains("'")){
					int first = value.indexOf('\'');
					int last = value.lastIndexOf('\'');
					if (first + 1 < last)
						value = value.substring(first + 1 , last);
				}
				
				switch(fieldName){
				case "company":
					if (notFlag){
						for (String keyValue: companyMap.keySet()){
							if(keyValue.equalsIgnoreCase(value))
								continue;
							read(columnNames, tableName, companyMap.get(keyValue), deleteFlag);
						}
					}
					else{
						if (companyMap.containsKey(value))
							read(columnNames, tableName, companyMap.get(value), deleteFlag);
						else
							System.out.println("\nNo records found");
					}
					break;
				case "drug_id":
					if (notFlag){
						for (String keyValue: drugIdMap.keySet()){
							if(keyValue.equalsIgnoreCase(value))
								continue;
							read(columnNames, tableName, drugIdMap.get(keyValue), deleteFlag);
						}
					}
					else{
						if (drugIdMap.containsKey(value))
								read(columnNames, tableName, drugIdMap.get(value), deleteFlag);
						else
							System.out.println("\nNo records found");
					}
					break;
				case "double_blind":
					if (notFlag)
						key = ! Boolean.parseBoolean(value);
					else
						key = Boolean.parseBoolean(value);
					
					read(columnNames, tableName, doubleBlindMap.get(key), deleteFlag);
					break;
				case "controlled_study":
					if (notFlag)
						key = ! Boolean.parseBoolean(value);
					else
						key = Boolean.parseBoolean(value);
					
					read(columnNames, tableName, controlledStudyMap.get(key), deleteFlag);
					break;
				case "govt_funded":
					if (notFlag)
						key = ! Boolean.parseBoolean(value);
					else
						key = Boolean.parseBoolean(value);
					
					read(columnNames, tableName, govtFundedMap.get(key), deleteFlag);
					break;
				case "fda_approved":
					if (notFlag)
						key = ! Boolean.parseBoolean(value);
					else
						key = Boolean.parseBoolean(value);
					
					read(columnNames, tableName, fdaApprovedMap.get(key), deleteFlag);
					break;
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	} 
	
	private static void read(String columnNames, String tableName, String filePointer, boolean deleteFlag){
		RandomAccessFile randomAccessFile = null;
		try{
			if (deleteFlag)
				randomAccessFile = new RandomAccessFile(tableName + ".db", "rws");
			else
				randomAccessFile = new RandomAccessFile(tableName + ".db", "r");
			
			String[] pointers;
			long booleanSetPointer;
			int count = 0;
			
			if (filePointer.contains(",")){
				pointers = filePointer.split(Pattern.quote(","));
			}
			else
				pointers = new String[] {filePointer};
			
			for (String ptr : pointers){
				ptr = ptr.trim();
				randomAccessFile.seek(Long.parseLong(ptr));
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
					
					//Get pointer to start over-writing the booleanSet
					booleanSetPointer  = randomAccessFile.getFilePointer();
					
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
					
					if (! deleteFlag){
						if ( (booleanSet & deleteMask) != deleteMask){
						
							if (columnNames.equals("*")){
								System.out.println(id + "," + new String(company) + "," + new String(drugId) + "," + trials + "," + patients + "," + dosageMg 
									+ "," + reading + "," + doubleBlind + "," + controlledStudy + "," + govtFunded + "," + fdaApproved);
							}
							else if(columnNames.contains(",")){
								String[] columnArr = columnNames.split(Pattern.quote(","));
								for (String column : columnArr){
									column = column.trim();
									switch(column){
									case "id":
										System.out.print(id);
										break;
									case "company":
										System.out.print("," + new String(company));
										break;
									case "drug_id":
										System.out.print("," + new String(drugId));
										break;
									case "trials":
										System.out.print("," + trials);
										break;
									case "patients":
										System.out.print("," + patients);
										break;
									case "dosage_mg":
										System.out.print("," + dosageMg);
										break;
									case "reading":
										System.out.print("," + reading);
										break;
									case "double_blind":
										System.out.print("," + doubleBlind);
										break;
									case "controlled_study":
										System.out.print("," + controlledStudy);
										break;
									case "govt_funded":
										System.out.print("," + govtFunded);
										break;
									case "fda_approved":
										System.out.print("," + fdaApproved);
										break;
									}
								}
								System.out.print("\n");
							}
							else{
								columnNames = columnNames.trim();
								switch(columnNames){
								case "id":
									System.out.print(id);
									break;
								case "company":
									System.out.print("," + new String(company));
									break;
								case "drug_id":
									System.out.print("," + new String(drugId));
									break;
								case "trials":
									System.out.print("," + trials);
									break;
								case "patients":
									System.out.print("," + patients);
									break;
								case "dosage_mg":
									System.out.print("," + dosageMg);
									break;
								case "reading":
									System.out.print("," + reading);
									break;
								case "double_blind":
									System.out.print("," + doubleBlind);
									break;
								case "controlled_study":
									System.out.print("," + controlledStudy);
									break;
								case "govt_funded":
									System.out.print("," + govtFunded);
									break;
								case "fda_approved":
									System.out.print("," + fdaApproved);
									break;
								}
								System.out.print("\n");
							}
							count += 1;
						}
					}
					else{
						if ((booleanSet & deleteMask) == deleteMask){
							System.out.println("Record already deleted");
						}
						else{
							booleanSet = (byte) (booleanSet | deleteMask);
							count += 1;
							randomAccessFile.seek(booleanSetPointer);
							randomAccessFile.write(booleanSet);
						}
					}
					break;
				}
			}
			randomAccessFile.close();

			if (deleteFlag && count > 0){
				System.out.println("\n" + count + " Record(s) deleted");
			}
		}
		catch(Exception e){
			
			e.printStackTrace();
			
		}
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
			if (out >= 1 && out <= 5 )
				break;
			else
				System.out.println("Please enter correct option");
		}

		return out;
	}
	
	private static void csvToBinary(String fileName){
		
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
			file = new File(fileName);
			
			reader = new CSVReader(new FileReader(file));
			
			randomAccessFile = new RandomAccessFile(TABLE_NAME + ".db", "rw");
			
			reader.readNext(); // Reading header
			String[] line = reader.readNext();
			
			while(line != null){
				writeDataToFile(randomAccessFile, line);
				
				line = reader.readNext();
			}
			
			/**Write the hashMaps to index files */
			
			writeAllMaps();
			
			/**End of write hashMaps*/
			
			reader.close();
			randomAccessFile.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
					
			
		}
	}

	private static void writeAllMaps() {
		writeMapsToFile(TABLE_NAME + indexFile[0], idMap);
		writeMapsToFile(TABLE_NAME + indexFile[1], companyMap);
		writeMapsToFile(TABLE_NAME + indexFile[2], drugIdMap);
		writeMapsToFile(TABLE_NAME + indexFile[3], trialsMap);
		writeMapsToFile(TABLE_NAME + indexFile[4], patientsMap);
		writeMapsToFile(TABLE_NAME + indexFile[5], dosageMgMap);
		writeMapsToFile(TABLE_NAME + indexFile[6], readingMap);
		writeMapsToFile(TABLE_NAME + indexFile[7], doubleBlindMap);
		writeMapsToFile(TABLE_NAME + indexFile[8], controlledStudyMap);
		writeMapsToFile(TABLE_NAME + indexFile[9], govtFundedMap);
		writeMapsToFile(TABLE_NAME + indexFile[10], fdaApprovedMap);
	}

	private static void writeDataToFile(RandomAccessFile randomAccessFile, String[] line) throws IOException {
		
		long filePointer;
		
		int id = Integer.parseInt((line[0]).trim());
		String company = line[1].trim();
		String drugId = line[2].trim();
		int trials =  Integer.parseInt((line[3]).trim());
		int patients =  Integer.parseInt((line[4]).trim());
		int dosageMg =  Integer.parseInt((line[5]).trim());
		float reading = Float.parseFloat((line[6]).trim());
		boolean doubleBlind = Boolean.parseBoolean((line[7]).trim());
		boolean controlledStudy = Boolean.parseBoolean((line[8]).trim());
		boolean govtFunded = Boolean.parseBoolean((line[9]).trim());
		boolean fdaApproved = Boolean.parseBoolean((line[10]).trim());
		@SuppressWarnings("unused")
		boolean deleted = false;
		
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
		
		company = company.toLowerCase();
		if (companyMap.containsKey(company)){
			companyMap.put(company, companyMap.get(company) + "," + String.valueOf(filePointer));
		}
		else
			companyMap.put(company, String.valueOf(filePointer));
		
		drugId = drugId.toLowerCase();
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
	
	/**Used to read & print the whole binary file 
	 * Commented as this part is not used 
	private static void readBinaryFile(){
		RandomAccessFile randomAccessFile = null;
		try{
			randomAccessFile = new RandomAccessFile(TABLE_NAME + ".db", "r");
			
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
						+ "," + reading + "," + doubleBlind + "," + controlledStudy + "," + govtFunded + "," + fdaApproved);
				
			}
			//randomAccessFile.close();
		}
		catch(Exception e){
			
			e.printStackTrace();
			
		}
	}
	*/
	
	private static Object readHashMap(String fileName, Object objMap){
		File file = null;
		FileInputStream fileInStream = null;
		ObjectInputStream objInStream = null;
		
		try{
			file = new File(fileName);
			fileInStream = new FileInputStream(file);
			objInStream = new ObjectInputStream(fileInStream);
			
			objMap = objInStream.readObject();
			
			objInStream.close();
			fileInStream.close();
		
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return objMap;
	}
	
	@SuppressWarnings("unchecked")
	private static void init(){
		try{
			File f = null;
			Boolean bool = new File(TABLE_NAME + ".db").exists();
			
			//populate column list from indexFile list
			for (int i = 0; i < 11; i++){
				String[] temp = (indexFile[i]).split(Pattern.quote("."));
				columnList.add(temp[1]);
			}
			
			//Check all files exists
			if (bool){
				for (int i = 0; i < 11; i++){
					f = new File(TABLE_NAME + indexFile[i]);
					bool = f.exists();
					if (bool == false)
						break;
				}
			}
		
			if (bool){
				Object objMap = new HashMap<>();
				idMap = (HashMap<Integer, Long>) readHashMap(TABLE_NAME + indexFile[0], objMap);
				companyMap = (HashMap<String, String>) readHashMap(TABLE_NAME + indexFile[1], objMap);
				drugIdMap = (HashMap<String, String>) readHashMap(TABLE_NAME + indexFile[2], objMap);
				trialsMap = (HashMap<Integer, String>) readHashMap(TABLE_NAME + indexFile[3], objMap);
				patientsMap = (HashMap<Integer, String>) readHashMap(TABLE_NAME + indexFile[4], objMap);
				dosageMgMap = (HashMap<Integer, String>) readHashMap(TABLE_NAME + indexFile[5], objMap);
				readingMap = (HashMap<Float, String>) readHashMap(TABLE_NAME + indexFile[6], objMap);
				doubleBlindMap = (HashMap<Boolean, String>) readHashMap(TABLE_NAME + indexFile[7], objMap);
				controlledStudyMap = (HashMap<Boolean, String>) readHashMap(TABLE_NAME + indexFile[8], objMap);
				govtFundedMap = (HashMap<Boolean, String>) readHashMap(TABLE_NAME + indexFile[9], objMap);
				fdaApprovedMap = (HashMap<Boolean, String>) readHashMap(TABLE_NAME + indexFile[10], objMap);
			}
			else
				System.out.println("Default files not found (PHARMA_TRIALS_1000). Please import database.");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	private static String getFullQuery(BufferedReader br, String type){
		
		String query = "", 
				fullQuery = "";
		boolean end = false;
		try{
			System.out.println("Please type your " + type + " query below:");
			do{
				System.out.print("prompt > ");
				query = br.readLine();
				if (query.indexOf(';') != -1 )
					end = true;
				fullQuery += " " + query.trim();
				fullQuery = fullQuery.trim();
			}while(!end);
		}
		catch (Exception e){
			e.printStackTrace();
		}
		return fullQuery;
	}
}
