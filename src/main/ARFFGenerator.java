package main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import main.KNN.CustomRowNominal;
import main.KNN.CustomRowNumeric;

public class ARFFGenerator {

	CustomRowNominal[] allRows = new CustomRowNominal[9917530];
	CustomRowNumeric[] allRowsNumeric = new CustomRowNumeric[9917530];
	float[] means = new float[19];
	int[] count = new int[19];
	float[] stdvt = new float[19];
	
	Set<Integer> booked = new HashSet<Integer>();
	Set<Integer> nonbooked = new HashSet<Integer>();

	Connection conn;

	class CustomRowNumeric implements Comparable<CustomRowNumeric> {
		float visitor_hist_starrating;
		boolean visitor_hist_starrating_b;
		float visitor_hist_adr_usd;
		boolean visitor_hist_adr_usd_b;
		float prop_review_score;
		boolean prop_review_score_b;
		float prop_location_score1;
		boolean prop_location_score1_b;
		float prop_location_score2;
		boolean prop_location_score2_b;
		float prop_log_historical_price;
		boolean prop_log_historical_price_b;
		float price_usd;
		boolean price_usd_b;
		int srch_length_of_stay;
		boolean srch_length_of_stay_b;
		int srch_booking_window;
		boolean srch_booking_window_b;

		int srch_adults_count;
		boolean srch_adults_count_b;
		int srch_children_count;
		boolean srch_children_count_b;
		int srch_room_count;
		boolean srch_room_count_b;
		float srch_query_affinity_score;
		boolean srch_query_affinity_score_b;
		float orig_destination_distance;
		boolean orig_destination_distance_b;
		float comp1_rate_percent_diff;
		boolean comp1_rate_percent_diff_b;
		float comp2_rate_percent_diff;
		boolean comp2_rate_percent_diff_b;
		float comp3_rate_percent_diff;
		boolean comp3_rate_percent_diff_b;
		float comp4_rate_percent_diff;
		boolean comp4_rate_percent_diff_b;
		float comp5_rate_percent_diff;
		boolean comp5_rate_percent_diff_b;

		float dist;

		boolean booking_bool;

		public CustomRowNumeric(ResultSet rs) {
			super();
			try {
				if (rs.getObject("visitor_hist_starrating") == null)
					this.visitor_hist_starrating_b = true;
				this.visitor_hist_starrating = rs
						.getFloat("visitor_hist_starrating");
				if (rs.getObject("visitor_hist_adr_usd") == null)
					this.visitor_hist_adr_usd_b = true;
				this.visitor_hist_adr_usd = rs.getFloat("visitor_hist_adr_usd");
				if (rs.getObject("prop_review_score") == null)
					this.prop_review_score_b = true;
				this.prop_review_score = rs.getFloat("prop_review_score");
				if (rs.getObject("prop_location_score1") == null)
					this.prop_location_score1_b = true;
				this.prop_location_score1 = rs.getFloat("prop_location_score1");
				if (rs.getObject("prop_location_score2") == null)
					this.prop_location_score2_b = true;
				this.prop_location_score2 = rs.getFloat("prop_location_score2");
				if (rs.getObject("prop_log_historical_price") == null)
					this.prop_log_historical_price_b = true;
				this.prop_log_historical_price = rs
						.getFloat("prop_log_historical_price");
				if (rs.getObject("price_usd") == null)
					this.price_usd_b = true;
				this.price_usd = rs.getFloat("price_usd");
				if (rs.getObject("srch_length_of_stay") == null)
					this.srch_length_of_stay_b = true;
				this.srch_length_of_stay = rs.getInt("srch_length_of_stay");
				if (rs.getObject("srch_booking_window") == null)
					this.srch_booking_window_b = true;
				this.srch_booking_window = rs.getInt("srch_booking_window");
				if (rs.getObject("srch_adults_count") == null)
					this.srch_adults_count_b = true;
				this.srch_adults_count = rs.getInt("srch_adults_count");
				if (rs.getObject("srch_children_count") == null)
					this.srch_children_count_b = true;
				this.srch_children_count = rs.getInt("srch_children_count");
				if (rs.getObject("srch_room_count") == null)
					this.srch_room_count_b = true;
				this.srch_room_count = rs.getInt("srch_room_count");
				if (rs.getObject("srch_query_affinity_score") == null)
					this.srch_query_affinity_score_b = true;
				this.srch_query_affinity_score = rs
						.getFloat("srch_query_affinity_score");
				if (rs.getObject("orig_destination_distance") == null)
					this.orig_destination_distance_b = true;
				this.orig_destination_distance = rs
						.getFloat("orig_destination_distance");
				if (rs.getObject("comp1_rate_percent_diff") == null)
					this.comp1_rate_percent_diff_b = true;
				this.comp1_rate_percent_diff = rs
						.getFloat("comp1_rate_percent_diff");
				if (rs.getObject("comp2_rate_percent_diff") == null)
					this.comp2_rate_percent_diff_b = true;
				this.comp2_rate_percent_diff = rs
						.getFloat("comp2_rate_percent_diff");
				if (rs.getObject("comp3_rate_percent_diff") == null)
					this.comp3_rate_percent_diff_b = true;
				this.comp3_rate_percent_diff = rs
						.getFloat("comp3_rate_percent_diff");
				if (rs.getObject("comp4_rate_percent_diff") == null)
					this.comp4_rate_percent_diff_b = true;
				this.comp4_rate_percent_diff = rs
						.getFloat("comp4_rate_percent_diff");
				if (rs.getObject("comp5_rate_percent_diff") == null)
					this.comp5_rate_percent_diff_b = true;
				this.comp5_rate_percent_diff = rs
						.getFloat("comp5_rate_percent_diff");
				this.booking_bool = rs.getBoolean("booking_bool");

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public CustomRowNumeric(CustomRowNumeric cr) {
			this.visitor_hist_starrating = cr.visitor_hist_starrating;
			this.visitor_hist_starrating_b = cr.visitor_hist_starrating_b;
			this.visitor_hist_adr_usd = cr.visitor_hist_adr_usd;
			this.visitor_hist_adr_usd_b = cr.visitor_hist_adr_usd_b;
			this.prop_review_score = cr.prop_review_score;
			this.prop_review_score_b = cr.prop_review_score_b;
			this.prop_location_score1 = cr.prop_location_score1;
			this.prop_location_score1_b = cr.prop_location_score1_b;
			this.prop_location_score2 = cr.prop_location_score2;
			this.prop_location_score2_b = cr.prop_location_score2_b;
			this.prop_log_historical_price = cr.prop_log_historical_price;
			this.prop_log_historical_price_b = cr.prop_log_historical_price_b;
			this.price_usd = cr.price_usd;
			this.price_usd_b = cr.price_usd_b;
			this.srch_length_of_stay = cr.srch_length_of_stay;
			this.srch_length_of_stay_b = cr.srch_length_of_stay_b;
			this.srch_booking_window = cr.srch_booking_window;
			this.srch_booking_window_b = cr.srch_booking_window_b;
			this.srch_adults_count = cr.srch_adults_count;
			this.srch_adults_count_b = cr.srch_adults_count_b;
			this.srch_children_count = cr.srch_children_count;
			this.srch_children_count_b = cr.srch_children_count_b;
			this.srch_room_count = cr.srch_room_count;
			this.srch_room_count_b = cr.srch_room_count_b;
			this.srch_query_affinity_score = cr.srch_query_affinity_score;
			this.srch_query_affinity_score_b = cr.srch_query_affinity_score_b;
			this.orig_destination_distance = cr.orig_destination_distance;
			this.orig_destination_distance_b = cr.orig_destination_distance_b;
			this.comp1_rate_percent_diff = cr.comp1_rate_percent_diff;
			this.comp1_rate_percent_diff_b = cr.comp1_rate_percent_diff_b;
			this.comp2_rate_percent_diff = cr.comp2_rate_percent_diff;
			this.comp2_rate_percent_diff_b = cr.comp2_rate_percent_diff_b;
			this.comp3_rate_percent_diff = cr.comp3_rate_percent_diff;
			this.comp3_rate_percent_diff_b = cr.comp3_rate_percent_diff_b;
			this.comp4_rate_percent_diff = cr.comp4_rate_percent_diff;
			this.comp4_rate_percent_diff_b = cr.comp4_rate_percent_diff_b;
			this.comp5_rate_percent_diff = cr.comp5_rate_percent_diff;
			this.comp5_rate_percent_diff_b = cr.comp5_rate_percent_diff_b;

			this.booking_bool = cr.booking_bool;
		}

		public void setDist(float dist) {
			this.dist = dist;
		}

		@Override
		public int compareTo(CustomRowNumeric o) {
			// TODO Auto-generated method stub
			return new Float(this.dist).compareTo(o.dist);
		}

	}

	class CustomRowNominal implements Comparable<CustomRowNominal> {
		int site_id;
		int visitor_location_country_id;
		int prop_country_id;
		int prop_id;
		int prop_starrating;
		int prop_brand_bool;
		int promotion_flag;
		int srch_destination_id;
		int srch_saturday_night_bool;

		int comp1_rate;
		int comp1_inv;
		int comp2_rate;
		int comp2_inv;
		int comp3_rate;
		int comp3_inv;
		int comp4_rate;
		int comp4_inv;
		int comp5_rate;
		int comp5_inv;

		float dist;

		boolean booking_bool;

		public CustomRowNominal(ResultSet rs) {
			super();
			try {
				this.site_id = rs.getInt("site_id");
				this.visitor_location_country_id = rs
						.getInt("visitor_location_country_id");
				this.prop_country_id = rs.getInt("prop_country_id");
				this.prop_id = rs.getInt("prop_id");
				this.prop_starrating = rs.getInt("prop_starrating");
				this.prop_brand_bool = rs.getInt("prop_brand_bool");
				this.promotion_flag = rs.getInt("promotion_flag");
				this.srch_destination_id = rs.getInt("srch_destination_id");
				this.srch_saturday_night_bool = rs
						.getInt("srch_saturday_night_bool");
				this.comp1_rate = rs.getInt("comp1_rate");
				this.comp1_inv = rs.getInt("comp1_inv");
				this.comp2_rate = rs.getInt("comp2_rate");
				this.comp2_inv = rs.getInt("comp2_inv");
				this.comp3_rate = rs.getInt("comp3_rate");
				this.comp3_inv = rs.getInt("comp3_inv");
				this.comp4_rate = rs.getInt("comp4_rate");
				this.comp4_inv = rs.getInt("comp4_inv");
				this.comp5_rate = rs.getInt("comp5_rate");
				this.comp5_inv = rs.getInt("comp5_inv");
				this.booking_bool = rs.getBoolean("booking_bool");

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public CustomRowNominal(CustomRowNominal cr) {
			this.site_id = cr.site_id;
			this.visitor_location_country_id = cr.visitor_location_country_id;
			this.prop_country_id = cr.prop_country_id;
			this.prop_id = cr.prop_id;
			this.prop_starrating = cr.prop_starrating;
			this.prop_brand_bool = cr.prop_brand_bool;
			this.promotion_flag = cr.promotion_flag;
			this.srch_destination_id = cr.srch_destination_id;
			this.srch_saturday_night_bool = cr.srch_saturday_night_bool;
			this.comp1_rate = cr.comp1_rate;
			this.comp1_inv = cr.comp1_inv;
			this.comp2_rate = cr.comp2_rate;
			this.comp2_inv = cr.comp2_inv;
			this.comp3_rate = cr.comp3_rate;
			this.comp3_inv = cr.comp3_inv;
			this.comp4_rate = cr.comp4_rate;
			this.comp4_inv = cr.comp4_inv;
			this.comp5_rate = cr.comp5_rate;
			this.comp5_inv = cr.comp5_inv;
			this.booking_bool = cr.booking_bool;
		}

		public void setDist(float dist) {
			this.dist = dist;
		}

		@Override
		public int compareTo(CustomRowNominal o) {
			// TODO Auto-generated method stub
			return new Float(this.dist).compareTo(o.dist);
		}

	}

	public static void main(String[] args) {
		new ARFFGenerator().start();
	}

	private void start() {
		initConnection();
//		File f = new File();
		PrintWriter writer;
		try {
			writer = new PrintWriter("C:\\Users\\Karol\\test.arff", "UTF-8");
			writer.println("@RELATION expedia");
			writer.println("");
			writer.println("@ATTRIBUTE prop_location_score1 NUMERIC");
			writer.println("@ATTRIBUTE prop_location_score2 NUMERIC");
			writer.println("@ATTRIBUTE prop_review_score NUMERIC");
			writer.println("@ATTRIBUTE class {0,1}");
			writer.println("");
			writer.println("@DATA");
			try {
				conn.setAutoCommit(false);

				String q = "SELECT srch_id, booking_bool, visitor_hist_starrating, visitor_hist_adr_usd ,prop_review_score, prop_location_score1,prop_location_score2, prop_log_historical_price, price_usd,"
						+ " srch_length_of_stay, srch_booking_window, srch_adults_count, srch_children_count, srch_room_count, srch_query_affinity_score,orig_destination_distance ,comp1_rate_percent_diff,comp2_rate_percent_diff ,comp3_rate_percent_diff,comp4_inv,comp4_rate_percent_diff,comp5_rate_percent_diff FROM train_set";
				Statement stmt = conn.createStatement(
						java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY);
				stmt.setFetchSize(Integer.MIN_VALUE);
				if (stmt.execute(q)) {
					ResultSet rs = stmt.getResultSet();
					int i = 0;
					while (rs.next()) {
						if(rs.getInt("booking_bool") == 1 && booked.size() < 100 && !booked.contains(rs.getInt("srch_id"))) {
							booked.add(rs.getInt("srch_id"));
							writer.println(rs.getDouble("prop_location_score1") + "," + rs.getDouble("prop_location_score2") + "," + rs.getDouble("prop_review_score") + "," + rs.getInt("booking_bool"));
						} else if(rs.getInt("booking_bool") == 0 && nonbooked.size() < 100 && !nonbooked.contains(rs.getInt("srch_id"))) {
							nonbooked.add(rs.getInt("srch_id"));
							writer.println(rs.getDouble("prop_location_score1") + "," + rs.getDouble("prop_location_score2") + "," + rs.getDouble("prop_review_score") + "," + rs.getInt("booking_bool"));

						}
//						allRowsNumeric[i] = new CustomRowNumeric(rs);
						if(booked.size() >= 100 && nonbooked.size() >=100) {
							break;
						}
						i++;
						if (i % 100 == 0) {
							System.out.println(i);
						}
					}
				}

				stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			writer.close();
		} catch (FileNotFoundException | UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
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

	private void getAll() throws SQLException {
		conn.setAutoCommit(false);
		String q = "SELECT booking_bool, site_id, visitor_location_country_id ,prop_country_id,prop_id, prop_starrating, prop_brand_bool, promotion_flag, srch_destination_id, srch_saturday_night_bool, comp1_rate, comp1_inv, comp2_rate,comp2_inv ,comp3_rate,comp3_inv ,comp4_rate,comp4_inv,comp5_rate,comp5_inv FROM train_set";
		Statement stmt = conn.createStatement(
				java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		if (stmt.execute(q)) {
			ResultSet rs = stmt.getResultSet();
			int i = 0;
			while (rs.next()) {
				allRows[i] = new CustomRowNominal(rs);
				i++;
				if (i % 100 == 0) {
					System.out.println(i);
				}
			}
		}

		stmt.close();

		// new java.util.Scanner(System.in).nextLine();

	}

	private void getAllNumeric() throws SQLException {

		conn.setAutoCommit(false);
		String q = "SELECT booking_bool, visitor_hist_starrating, visitor_hist_adr_usd ,prop_review_score, prop_location_score1,prop_location_score2, prop_log_historical_price, price_usd,"
				+ " srch_length_of_stay, srch_booking_window, srch_adults_count, srch_children_count, srch_room_count, srch_query_affinity_score,orig_destination_distance ,comp1_rate_percent_diff,comp2_rate_percent_diff ,comp3_rate_percent_diff,comp4_inv,comp4_rate_percent_diff,comp5_rate_percent_diff FROM train_set";
		Statement stmt = conn.createStatement(
				java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		if (stmt.execute(q)) {
			ResultSet rs = stmt.getResultSet();
			int i = 0;
			while (rs.next()) {
				allRowsNumeric[i] = new CustomRowNumeric(rs);
				i++;
				if (i % 100 == 0) {
					System.out.println(i);
				}
			}
		}

		stmt.close();

	}
}
