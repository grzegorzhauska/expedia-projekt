package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Grouping {

	Connection conn;

	public static void main(String[] args) {

		// new Grouping().start();
		new Grouping().numeric();
	}

	private void start() {
		initConnection();
		String[] attrs = { "site_id", "visitor_location_country_id",
				"prop_country_id", "prop_id", "prop_starrating",
				"prop_brand_bool", "promotion_flag", "srch_destination_id",
				"srch_saturday_night_bool", "comp1_rate", "comp1_inv",
				"comp2_rate", "comp2_inv", "comp3_rate", "comp3_inv",
				"comp4_rate", "comp4_inv", "comp5_rate", "comp5_inv" };

		try {
			for (String attr : attrs) {
				long startTime = System.currentTimeMillis();
				System.out.print(attr + "\t");
				PreparedStatement stmt = conn.prepareStatement("SELECT " + attr
						+ " FROM train_set GROUP BY " + attr);
				ResultSet rs = stmt.executeQuery();
				long time = System.currentTimeMillis() - startTime;
				int i = 0;
				while (rs.next()) {
					i++;
				}
				System.out.print(i + "\t");
				System.out.println(time / 1000 + "s");

			}

			int count = 0;
			for (int j = 0; j < attrs.length; j++) {
				for (int k = j + 1; k < attrs.length; k++) {
					System.out.print(attrs[j] + "," + attrs[k] + "\t");
					count++;
					long startTime = System.currentTimeMillis();
					PreparedStatement stmt = conn.prepareStatement("SELECT "
							+ attrs[j] + "," + attrs[k]
							+ " FROM train_set GROUP BY " + attrs[j] + ","
							+ attrs[k]);
					ResultSet rs = stmt.executeQuery();
					long time = System.currentTimeMillis() - startTime;
					int i = 0;
					while (rs.next()) {
						i++;
					}
					System.out.print(i + "\t");
					System.out.println(time + "s");
				}

			}
			System.out.println(count);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * ResultSetMetaData rsmd = rs.getMetaData(); int NumOfCol =
	 * rsmd.getColumnCount(); for(int i=1;i<=NumOfCol;i++) {
	 * System.out.println("Name of ["+i+"] Column data type is ="
	 * +rsmd.getColumnTypeName(i)); }
	 */

	private void numeric() {
		initConnection();
		String[] attrsI = { "srch_length_of_stay", "srch_booking_window",
				"srch_adults_count", "srch_children_count", "srch_room_count" };
		String[] attrsF = { "visitor_hist_starrating", "visitor_hist_adr_usd",
				"prop_review_score", "prop_location_score1",
				"prop_location_score2", "prop_log_historical_price",
				"gross_bookings_usd", //"srch_query_affinity_score", - daje error8
				"orig_destination_distance", "comp1_rate_percent_diff",
				"comp2_rate_percent_diff", "comp3_rate_percent_diff",
				"comp4_rate_percent_diff", "comp5_rate_percent_diff",
				"comp6_rate_percent_diff", "comp7_rate_percent_diff",
				"comp8_rate_percent_diff" };
		// List<List<Integer>> rangesI = new ArrayList<List<Integer>>();

		try {
			PreparedStatement stmt = conn
					.prepareStatement("SELECT COUNT(*) FROM train_set");
			ResultSet rsC = stmt.executeQuery();
			int instanceCount = 0;
			if (rsC.next()) {
				instanceCount = rsC.getInt(1);
			} else {
				System.out.println("error kurwa5");
				System.exit(5);
			}
			rsC.close();

			Integer[][] rangesI = new Integer[attrsI.length][4];
			

			System.out.println("dyskretyzacjaF rozpoczeta...");
			Float[][] rangesF = new Float[attrsF.length][4];
			for (int a = 0; a < attrsF.length; a++) {
				
				// System.out.println(attrsI[a]);
				stmt = conn.prepareStatement("SELECT " + attrsF[a]
						+ " FROM train_set ORDER BY " + attrsF[a] + " ASC ");
				ResultSet rs = stmt.executeQuery(); //tutaj sie wywala java.lang.OutOfMemoryError: Java heap space
											//at com.mysql.jdbc.MysqlIO.nextRow(MysqlIO.java:1370) oststnio 7-8GBramu zjadlo
				int parts = 5;

				// rangesI[a][0] = rs.getInt(1);
				rs.absolute(instanceCount / parts);
				rangesF[a][0] = rs.getFloat(1);
				rs.absolute(2 * (instanceCount / parts));
				rangesF[a][1] = rs.getFloat(1);
				while (rangesF[a][0] >= rangesF[a][1]) {
					if (!rs.next()) {
						System.out.println("error kurwa8");
						System.exit(8);
					}
					rangesF[a][1] = rs.getFloat(1);
				}
				rs.absolute(3 * (instanceCount / parts));
				rangesF[a][2] = rs.getFloat(1);
				while (rangesF[a][1] >= rangesF[a][2]) {
					if (!rs.next()) {
						System.out.println("error kurwa9");
						System.exit(9);
					}
					rangesF[a][2] = rs.getFloat(1);
				}
				rs.absolute(4 * (instanceCount / parts));
				rangesF[a][3] = rs.getFloat(1);
				while (rangesF[a][2] >= rangesF[a][3]) {
					if (!rs.next()) {
						System.out.println("error kurwa10");
						System.exit(10);
					}
					rangesF[a][3] = rs.getFloat(1);
				}

				System.out.println(attrsF[a] + " 0: " + rangesF[a][0]);
				System.out.println(attrsF[a] + " 1: " + rangesF[a][1]);
				System.out.println(attrsF[a] + " 2: " + rangesF[a][2]);
				System.out.println(attrsF[a] + " 3: " + rangesF[a][3]);

				rs.close();
			}

			save("data/dyskretyzacjaF.txt", rangesF);

			System.out.println("dyskretyzacjaF skonczona!");

			System.exit(0);

/*			System.out.println("dyskretyzacjaI rozpoczeta...");
			
			for (int a = 0; a < attrsI.length; a++) {

				// System.out.println(attrsI[a]);
				stmt = conn.prepareStatement("SELECT " + attrsI[a]
						+ " FROM train_set ORDER BY " + attrsI[a] + " ASC ");
				rs = stmt.executeQuery();
				int parts = 5;

				// rangesI[a][0] = rs.getInt(1);
				rs.absolute(instanceCount / parts);
				rangesI[a][0] = rs.getInt(1);
				rs.absolute(2 * (instanceCount / parts));
				rangesI[a][1] = rs.getInt(1);
				while (rangesI[a][0] >= rangesI[a][1]) {
					if (!rs.next()) {
						System.out.println("error kurwa1");
						System.exit(1);
					}
					rangesI[a][1] = rs.getInt(1);
				}
				rs.absolute(3 * (instanceCount / parts));
				rangesI[a][2] = rs.getInt(1);
				while (rangesI[a][1] >= rangesI[a][2]) {
					if (!rs.next()) {
						System.out.println("error kurwa2");
						System.exit(2);
					}
					rangesI[a][2] = rs.getInt(1);
				}
				rs.absolute(4 * (instanceCount / parts));
				rangesI[a][3] = rs.getInt(1);
				while (rangesI[a][2] >= rangesI[a][3]) {
					if (!rs.next()) {
						System.out.println("error kurwa3");
						System.exit(3);
					}
					rangesI[a][3] = rs.getInt(1);
				}

				
				 // System.out.println(attrsI[a] + " 0: " + rangesI[a][0]);
				 // System.out.println(attrsI[a] + " 1: " + rangesI[a][1]);
				 // System.out.println(attrsI[a] + " 2: " + rangesI[a][2]);
				 // System.out.println(attrsI[a] + " 3: " + rangesI[a][3]);
				 
			}

			save("data/dyskretyzacjaI.txt", rangesI);

			System.out.println("dyskretyzacjaI skonczona!");
*/
//			System.exit(0);
			
			
			
			rangesI = load("data/dyskretyzacjaI.txt");
			
			//for(int i = 0; i < attrsI.length;i++){
			//	for(int j=0; j < 4;j++){
			//		System.out.print(rangesI[i][j] + " ");
			//	}
			//	System.out.println();
			//}
			
			//System.exit(0);
			
			System.out.println("pojedyncze atrybutyI: ");
			for (int a = 0; a < attrsI.length; a++) {
				stmt = conn
						.prepareStatement("select t.ran as sr, count(*) as no"
								+ " from ( " + " select " // + attrsI[a] +  " , " + srch_d
								+ " case " + " when "
								+ attrsI[a]
								+ " <= "
								+ rangesI[a][0]
								+ " then '<"
								+ rangesI[a][0]
								+ "' "
								+ " when "
								+ attrsI[a]
								+ " > "
								+ rangesI[a][0]
								+ " and "
								+ attrsI[a]
								+ " <= "
								+ rangesI[a][1]
								+ " then '"
								+ rangesI[a][0]
								+ "-"
								+ rangesI[a][1]
								+ "' "
								+ " when "
								+ attrsI[a]
								+ " > "
								+ rangesI[a][1]
								+ " and "
								+ attrsI[a]
								+ " <= "
								+ rangesI[a][2]
								+ " then '"
								+ rangesI[a][1]
								+ "-"
								+ rangesI[a][2]
								+ "' "
								+ " when "
								+ attrsI[a]
								+ " > "
								+ rangesI[a][2]
								+ " and "
								+ attrsI[a]
								+ " <= "
								+ rangesI[a][3]
								+ " then '"
								+ rangesI[a][2]
								+ "-"
								+ rangesI[a][3]
								+ "' "
								+ " when "
								+ attrsI[a]
								+ " > "
								+ rangesI[a][3]
								+ " then '"
								+ " >"
								+ rangesI[a][3]
								+ "' "
								+ " else '200-999'  "
								+ " end as ran "
								+ " from train_set) t group by t.ran ");
				long startTime = System.currentTimeMillis();
				ResultSet rs = stmt.executeQuery();
				long time = System.currentTimeMillis() - startTime;

				System.out.println(attrsI[a] + " " + time / 1000 + "s");

				/*while (rs.next()) {
					System.out.println("sr: " + rs.getString("sr") + " no: "
							+ rs.getInt("no"));
				}*/

			}
			System.out.println("koniec pojedynchych atrybutowI: ");

			System.out.println("pary atrybutowI: ");
			for (int a = 0; a < attrsI.length; a++) {
				for (int b = a + 1; b < attrsI.length; b++) {

					stmt = conn
							.prepareStatement("select t.ran as sr, t.ran2 as sr2, count(*) as no"
									+ " from ( " + " select "
									// + attrsI[a]
									// + " , " // srch_d
									+ " case " + " when "
									+ attrsI[a]
									+ " <= "
									+ rangesI[a][0]
									+ " then '<"
									+ rangesI[a][0]
									+ "' "
									+ " when "
									+ attrsI[a]
									+ " > "
									+ rangesI[a][0]
									+ " and "
									+ attrsI[a]
									+ " <= "
									+ rangesI[a][1]
									+ " then '"
									+ rangesI[a][0]
									+ "-"
									+ rangesI[a][1]
									+ "' "
									+ " when "
									+ attrsI[a]
									+ " > "
									+ rangesI[a][1]
									+ " and "
									+ attrsI[a]
									+ " <= "
									+ rangesI[a][2]
									+ " then '"
									+ rangesI[a][1]
									+ "-"
									+ rangesI[a][2]
									+ "' "
									+ " when "
									+ attrsI[a]
									+ " > "
									+ rangesI[a][2]
									+ " and "
									+ attrsI[a]
									+ " <= "
									+ rangesI[a][3]
									+ " then '"
									+ rangesI[a][2]
									+ "-"
									+ rangesI[a][3]
									+ "' "
									+ " when "
									+ attrsI[a]
									+ " > "
									+ rangesI[a][3]
									+ " then '"
									+ " >"
									+ rangesI[a][3]
									+ "' "
									/* + " else '200-999'  " */
									+ " end as ran "
									+ " , "
									+ " case "
									+ " when "
									+ attrsI[b]
									+ " <= "
									+ rangesI[b][0]
									+ " then '<"
									+ rangesI[b][0]
									+ "' "
									+ " when "
									+ attrsI[b]
									+ " > "
									+ rangesI[b][0]
									+ " and "
									+ attrsI[b]
									+ " <= "
									+ rangesI[b][1]
									+ " then '"
									+ rangesI[b][0]
									+ "-"
									+ rangesI[b][1]
									+ "' "
									+ " when "
									+ attrsI[b]
									+ " > "
									+ rangesI[b][1]
									+ " and "
									+ attrsI[b]
									+ " <= "
									+ rangesI[b][2]
									+ " then '"
									+ rangesI[b][1]
									+ "-"
									+ rangesI[b][2]
									+ "' "
									+ " when "
									+ attrsI[b]
									+ " > "
									+ rangesI[b][2]
									+ " and "
									+ attrsI[b]
									+ " <= "
									+ rangesI[b][3]
									+ " then '"
									+ rangesI[b][2]
									+ "-"
									+ rangesI[b][3]
									+ "' "
									+ " when "
									+ attrsI[b]
									+ " > "
									+ rangesI[b][3]
									+ " then '"
									+ " >"
									+ rangesI[b][3]
									+ "' "
									+ " end as ran2 "
									+ " from train_set) t group by t.ran, t.ran2 ");
					long startTime = System.currentTimeMillis();
					ResultSet rs = stmt.executeQuery();
					long time = System.currentTimeMillis() - startTime;

					System.out.println(attrsI[a] + "," + attrsI[b] + " "
							+ time / 1000 + "s");

					/*while (rs.next()) {
						System.out.println("sr: " + rs.getString("sr")
								+ " sr2: " + rs.getString("sr2") + " no: "
								+ rs.getInt("no"));
					}*/
				}
			}
			System.out.println("koniec par atrybutowI: ");
/*
			System.out.println("pary atrybutowF: ");
			for (int a = 0; a < attrsF.length; a++) {
				for (int b = a + 1; b < attrsF.length; b++) {

					stmt = conn
							.prepareStatement("select t.ran as sr, t.ran2 as sr2, count(*) as no"
									+ " from ( " + " select "
									// + attrsI[a]
									// + " , " // srch_d
									+ " case " + " when "
									+ attrsF[a]
									+ " <= "
									+ rangesF[a][0]
									+ " then '<"
									+ rangesF[a][0]
									+ "' "
									+ " when "
									+ attrsF[a]
									+ " > "
									+ rangesF[a][0]
									+ " and "
									+ attrsF[a]
									+ " <= "
									+ rangesF[a][1]
									+ " then '"
									+ rangesF[a][0]
									+ "-"
									+ rangesF[a][1]
									+ "' "
									+ " when "
									+ attrsF[a]
									+ " > "
									+ rangesF[a][1]
									+ " and "
									+ attrsF[a]
									+ " <= "
									+ rangesF[a][2]
									+ " then '"
									+ rangesF[a][1]
									+ "-"
									+ rangesF[a][2]
									+ "' "
									+ " when "
									+ attrsF[a]
									+ " > "
									+ rangesF[a][2]
									+ " and "
									+ attrsF[a]
									+ " <= "
									+ rangesF[a][3]
									+ " then '"
									+ rangesF[a][2]
									+ "-"
									+ rangesF[a][3]
									+ "' "
									+ " when "
									+ attrsF[a]
									+ " > "
									+ rangesF[a][3]
									+ " then '"
									+ " >"
									+ rangesF[a][3]
									+ "' "
									// + " else '200-999'  " 
									+ " end as ran "
									+ " , "
									+ " case "
									+ " when "
									+ attrsF[b]
									+ " <= "
									+ rangesF[b][0]
									+ " then '<"
									+ rangesF[b][0]
									+ "' "
									+ " when "
									+ attrsF[b]
									+ " > "
									+ rangesF[b][0]
									+ " and "
									+ attrsF[b]
									+ " <= "
									+ rangesF[b][1]
									+ " then '"
									+ rangesF[b][0]
									+ "-"
									+ rangesF[b][1]
									+ "' "
									+ " when "
									+ attrsF[b]
									+ " > "
									+ rangesF[b][1]
									+ " and "
									+ attrsF[b]
									+ " <= "
									+ rangesF[b][2]
									+ " then '"
									+ rangesF[b][1]
									+ "-"
									+ rangesF[b][2]
									+ "' "
									+ " when "
									+ attrsF[b]
									+ " > "
									+ rangesF[b][2]
									+ " and "
									+ attrsF[b]
									+ " <= "
									+ rangesF[b][3]
									+ " then '"
									+ rangesF[b][2]
									+ "-"
									+ rangesF[b][3]
									+ "' "
									+ " when "
									+ attrsF[b]
									+ " > "
									+ rangesF[b][3]
									+ " then '"
									+ " >"
									+ rangesF[b][3]
									+ "' "
									+ " end as ran2 "
									+ " from train_set) t group by t.ran, t.ran2 ");
					long startTime = System.currentTimeMillis();
					rs = stmt.executeQuery();
					long time = System.currentTimeMillis() - startTime;

					System.out.println(attrsI[a] + "," + attrsI[b] + "\t"
							+ time / 1000 + "s");

					//while (rs.next()) {
					//	System.out.println("sr: " + rs.getString("sr")
					//			+ " sr2: " + rs.getString("sr2") + " no: "
					//			+ rs.getInt("no"));
					//}
				}
			}

			System.out.println("koniec par atrybutowF: ");
*/
			/*
			 * int count = 0; for (int j = 0; j < attrs.length; j++) { for (int
			 * k = j + 1; k < attrs.length; k++) { System.out.print(attrs[j] +
			 * "," + attrs[k] + "\t"); count++; long startTime =
			 * System.currentTimeMillis(); PreparedStatement stmt =
			 * conn.prepareStatement("SELECT " + attrs[j] + "," + attrs[k] +
			 * " FROM train_set GROUP BY " + attrs[j] + "," + attrs[k]);
			 * ResultSet rs = stmt.executeQuery(); long time =
			 * System.currentTimeMillis() - startTime; int i = 0; while
			 * (rs.next()) { i++; } System.out.print(i + "\t");
			 * System.out.println(time + "s"); }
			 * 
			 * } System.out.println(count);
			 */
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void save(String fileName, Float[][] x) {

		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(fileName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			for (int i = 0; i < 5; i++) {
				for (int j = 0; j < 4; j++) {

					writer.write(x[i][j].toString());

					writer.write(" ");
				}
				writer.newLine();

			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("error kurwa11");
			System.exit(11);

		}
	}

	public void save(String fileName, Integer[][] x) {

		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(fileName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			for (int i = 0; i < 5; i++) {
				for (int j = 0; j < 4; j++) {

					writer.write(x[i][j].toString());

					writer.write(" ");
				}
				writer.newLine();

			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("error kurwa6");
			System.exit(6);

		}
	}

	public Integer[][] load(String fileName) {

		Integer[][] x = new Integer[5][4];

		Scanner scanner = null;
		try {
			scanner = new Scanner(new File(fileName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("error kurwa12");
			System.exit(12);
		}

		for (int i = 0; i < 5; i++) {
			for (int j = 0; j < 4; j++) {
				if (scanner.hasNextInt()) {
					x[i][j] = scanner.nextInt();
				} else {
					System.out.println("error kurwa7");
					System.exit(7);
				}
			}
		}
		scanner.close();

		return x;
	}

	private void initConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/expedia", "root", "");
			conn.setAutoCommit(false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
