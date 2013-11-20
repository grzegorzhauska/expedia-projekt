package main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Grouping {

	Connection conn;

	public static void main(String[] args) {

		new Grouping().start();

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
				System.out.println(attr);
				PreparedStatement stmt = conn.prepareStatement("SELECT " + attr
						+ " FROM train_set GROUP BY " + attr);
				ResultSet rs = stmt.executeQuery();
				long time = System.currentTimeMillis() - startTime;
				int i = 0;
				while (rs.next()) {
					i++;
				}
				System.out.println("Liczba rekordów: " + i);
				System.out.println("Czas: " + time);

			}

			int count = 0;
			for (int j = 0; j < attrs.length; j++) {
				for (int k = j + 1; k < attrs.length; k++) {
					System.out.println(attrs[j] + " " + attrs[k]);
					count++;
					long startTime = System.currentTimeMillis();
					PreparedStatement stmt = conn.prepareStatement("SELECT "
							+ attrs[j] + "," + attrs[k] + " FROM train_set GROUP BY " + attrs[j] + "," + attrs[k]);
					ResultSet rs = stmt.executeQuery();
					long time = System.currentTimeMillis() - startTime;
					int i = 0;
					while (rs.next()) {
						i++;
					}
					System.out.println("Liczba rekordów: " + i);
					System.out.println("Czas: " + time);
				}

			}
			System.out.println(count);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void initConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/expedia", "root", "qazwsx");
			conn.setAutoCommit(false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
