
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class adwords {

	public static void main(String[] args)
	// throws SQLException, IOException, InterruptedException
	{
		

		Date date = new Date();
		System.out.println("start time" + date.toString());
		CallableStatement token;
		CallableStatement processData;
		CallableStatement createData;
		ResultSet rset;
		//String outString = "";
		ResultSet processQuery;
		//ResultSet advertisersMatched;
		ArrayList<String> input = new ArrayList<String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader("system.in"));
			String line = "";
			String username;
			String password;
			int ch;
			while ((line = br.readLine()) != null) {
				ch = line.indexOf('=');
				input.add(line.substring(ch + 2));
			}

			username = input.get(0);
			password = input.get(1);
			//for (String s : input)
			//	System.out.println(s);
			List<Integer> list = new ArrayList<Integer>();
			for (int i = 2; i <= 7; i++)
				list.add(Integer.parseInt(input.get(i)));

			Connection conn = DriverManager.getConnection(
					"jdbc:oracle:thin:@oracle.cise.ufl.edu:1521:orcl",
					username, password);
			Statement stmt = conn.createStatement();
			// ArrayList<String> user = new ArrayList<String>();

			Process p1 = Runtime.getRuntime().exec(
					"sqlplus " + username + "@orcl/" + password
							+ " @adwords.sql");
			System.out.println("adwords.sql processed");
			p1.waitFor();
			 Process p2 = Runtime.getRuntime().exec("sqlldr "
			 +username+"@orcl/"+password+ " control=loadq.ctl");
			p2.waitFor();
			 Process p3 =
			 Runtime.getRuntime().exec("sqlldr "+username+"@orcl/"+password+" control=load.ctl");
			 p3.waitFor();
			 Process p4 =
			 Runtime.getRuntime().exec("sqlldr "+username+"@orcl/"+password+" control=loadk.ctl");
			 p4.waitFor();																			

			processQuery = stmt
					.executeQuery("CREATE VIEW AdvertisersKeywordCount (AdvertiserId, KeywordCount) AS"
							+ " SELECT A.AdvertiserId, count(*) FROM Advertisers A, Keywords K WHERE A.AdvertiserId = K.AdvertiserId"
							+ " GROUP BY A.AdvertiserId");

	
						processData = conn
								.prepareCall("{call processAds(?,?,?,?,?,?)}");
						processData.setInt(1, list.get(0));
						processData.setInt(2, list.get(1));
						processData.setInt(3, list.get(2));
						processData.setInt(4, list.get(3));
						processData.setInt(5, list.get(4));
						processData.setInt(6, list.get(5));
						processData.execute();
						processData.close();

					
				
			
			writeData(conn);
			conn.commit();
			conn.close();
			
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void writeData(Connection conn) throws IOException {

		FileOutputStream fopG = null;
		BufferedWriter writerG = null;
		FileOutputStream fopG2 = null;
		BufferedWriter writerG2 = null;
		FileOutputStream fopB = null;
		BufferedWriter writerB = null;
		FileOutputStream fopB2 = null;
		BufferedWriter writerB2 = null;
		FileOutputStream fopP = null;
		BufferedWriter writerP = null;
		FileOutputStream fopP2 = null;
		BufferedWriter writerP2 = null;
		Date date;
		

		try {

			fopG = new FileOutputStream("system.out.1", false); 
			fopG2 = new FileOutputStream("system.out.2", false);
			fopB = new FileOutputStream("system.out.3", false);
			fopB2 = new FileOutputStream("system.out.4", false);
			fopP = new FileOutputStream("system.out.5", false);
			fopP2 = new FileOutputStream("system.out.6", false);

			writerG = new BufferedWriter(new OutputStreamWriter(fopG));
			writerG2 = new BufferedWriter(new OutputStreamWriter(fopG2));
			writerB = new BufferedWriter(new OutputStreamWriter(fopB));
			writerB2 = new BufferedWriter(new OutputStreamWriter(fopB2));
			writerP = new BufferedWriter(new OutputStreamWriter(fopP));
			writerP2 = new BufferedWriter(new OutputStreamWriter(fopP2));

			// System.out.println("abc");
			Statement selectG = conn.createStatement();
			Statement selectG2 = conn.createStatement();
			Statement selectB = conn.createStatement();
			Statement selectB2 = conn.createStatement();
			Statement selectP = conn.createStatement();
			Statement selectP2 = conn.createStatement();
			ResultSet resultG = selectG
					.executeQuery("select * from greedyOutput order by qid,rank");
			ResultSet resultG2 = selectG2
					.executeQuery("select * from greedyOutput2 order by qid,rank");
			ResultSet resultB = selectB
					.executeQuery("select * from balanceOutput order by qid,rank");
			ResultSet resultB2 = selectB2
					.executeQuery("select * from balanceOutput2 order by qid,rank");
			ResultSet resultP = selectP
					.executeQuery("select * from psiOutput order by qid,rank");
			ResultSet resultP2 = selectP2
					.executeQuery("select * from psiOutput2 order by qid,rank");

			ResultSetMetaData rsmdG = resultG.getMetaData();

			int comma_count = 0;
			int columnCount = rsmdG.getColumnCount();
			
			while (resultG.next()) {
				comma_count = 0;
				StringBuilder row = new StringBuilder();
				for (int i = 1; i <= columnCount; i++) {
					if (comma_count < 4) {
						row.append(resultG.getObject(i) + ", ");
						comma_count = comma_count + 1;
					} else
						row.append(resultG.getObject(i) + " ");

				}
				
				writerG.write(row.toString());
				writerG.newLine();
			}

			while (resultG2.next()) {
				comma_count = 0;
				StringBuilder row = new StringBuilder();
				for (int i = 1; i <= columnCount; i++) {
					if (comma_count < 4) {
						row.append(resultG2.getObject(i) + ", ");
						comma_count = comma_count + 1;
					} else
						row.append(resultG2.getObject(i) + " ");
				}
				
				writerG2.write(row.toString());
				writerG2.newLine();
			}

			while (resultB.next()) {
				comma_count = 0;
				StringBuilder row = new StringBuilder();
				for (int i = 1; i <= columnCount; i++) {
					if (comma_count < 4) {
						row.append(resultB.getObject(i) + ", ");
						comma_count = comma_count + 1;
					} else
						row.append(resultB.getObject(i) + " ");
				}
				
				writerB.write(row.toString());
				writerB.newLine();
			}

			while (resultB2.next()) {
				comma_count = 0;
				StringBuilder row = new StringBuilder();
				for (int i = 1; i <= columnCount; i++) {
					if (comma_count < 4) {
						row.append(resultB2.getObject(i) + ", ");
						comma_count = comma_count + 1;
					} else
						row.append(resultB2.getObject(i) + " ");
				}
				
				writerB2.write(row.toString());
				writerB2.newLine();
			}

			while (resultP.next()) {
				comma_count = 0;
				StringBuilder row = new StringBuilder();
				for (int i = 1; i <= columnCount; i++) {
					if (comma_count < 4) {
						row.append(resultP.getObject(i) + ", ");
						comma_count = comma_count + 1;
					} else
						row.append(resultP.getObject(i) + " ");
				}
				
				writerP.write(row.toString());
				writerP.newLine();
			}

			while (resultP2.next()) {
				comma_count = 0;
				StringBuilder row = new StringBuilder();
				for (int i = 1; i <= columnCount; i++) {
					if (comma_count < 4) {
						row.append(resultP2.getObject(i) + ", ");
						comma_count = comma_count + 1;
					} else
						row.append(resultP2.getObject(i) + " ");
				}
				
				writerP2.write(row.toString());
				writerP2.newLine();
			}
		}// try block
		catch (Exception e) {
			System.err.println("Problem writing to the file temp.txt");
			e.printStackTrace();
		}
		/*
		 * finally { try { if (fopG != null) fop.close(); if (writer != null)
		 * writer.close(); } catch (IOException e) { e.printStackTrace(); } }
		 */
		writerG.close();
		writerG2.close();
		writerB.close();
		writerB2.close();
		writerP.close();
		writerP2.close();

		
		date = new Date();
		// display time and date using toString()
		System.out.println("end time" + date.toString());

	}

}

