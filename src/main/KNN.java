package main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class KNN {

	private void countMeans() {
		for (CustomRowNumeric cr : allRowsNumeric) {
			if (!cr.visitor_hist_starrating_b) {
				means[0] += cr.visitor_hist_starrating;
				count[0]++;
			}
			if (!cr.visitor_hist_adr_usd_b) {
				means[1] += cr.visitor_hist_adr_usd;
				count[1]++;
			}
			if (!cr.prop_review_score_b) {
				means[2] += cr.prop_review_score;
				count[2]++;
			}
			if (!cr.prop_location_score1_b) {
				means[3] += cr.prop_location_score1;
				count[3]++;
			}
			if (!cr.prop_location_score2_b) {
				means[4] += cr.prop_location_score2;
				count[4]++;
			}
			if (!cr.prop_log_historical_price_b) {
				means[5] += cr.prop_log_historical_price;
				count[5]++;
			}
			if (!cr.price_usd_b) {
				means[6] += cr.price_usd;
				count[6]++;
			}
			if (!cr.srch_length_of_stay_b) {
				means[7] += cr.srch_length_of_stay;
				count[7]++;
			}
			if (!cr.srch_booking_window_b) {
				means[8] += cr.srch_booking_window;
				count[8]++;
			}
			if (!cr.srch_adults_count_b) {
				means[9] += cr.srch_adults_count;
				count[9]++;
			}
			if (!cr.srch_children_count_b) {
				means[10] += cr.srch_children_count;
				count[10]++;
			}
			if (!cr.srch_room_count_b) {
				means[11] += cr.srch_room_count;
				count[11]++;
			}
			if (!cr.srch_query_affinity_score_b) {
				means[12] += cr.srch_query_affinity_score;
				count[12]++;
			}
			if (!cr.orig_destination_distance_b) {
				means[13] += cr.orig_destination_distance;
				count[13]++;
			}

			if (!cr.comp1_rate_percent_diff_b) {
				means[14] += cr.comp1_rate_percent_diff;
				count[14]++;
			}
			if (!cr.comp2_rate_percent_diff_b) {
				means[15] += cr.comp2_rate_percent_diff;
				count[15]++;
			}
			if (!cr.comp3_rate_percent_diff_b) {
				means[16] += cr.comp3_rate_percent_diff;
				count[16]++;
			}
			if (!cr.comp4_rate_percent_diff_b) {
				means[17] += cr.comp4_rate_percent_diff;
				count[17]++;
			}
			if (!cr.comp5_rate_percent_diff_b) {
				means[18] += cr.comp5_rate_percent_diff;
				count[18]++;
			}
		}
		for (int i = 0; i < 19; i++) {
			means[i] /= count[i];

		}

		for (CustomRowNumeric cr : allRowsNumeric) {
			if (!cr.visitor_hist_starrating_b) {
				stdvt[0] += Math.pow(cr.visitor_hist_starrating - means[0], 2);
			} else {
				cr.visitor_hist_starrating = means[0];
			}
			if (!cr.visitor_hist_adr_usd_b) {
				stdvt[1] += Math.pow(cr.visitor_hist_adr_usd - means[1], 2);
			} else {
				cr.visitor_hist_adr_usd = means[1];
			}
			if (!cr.prop_review_score_b) {
				stdvt[2] += Math.pow(cr.prop_review_score - means[2], 2);
			} else {
				cr.prop_review_score = means[2];
			}
			if (!cr.prop_location_score1_b) {
				stdvt[3] += Math.pow(cr.prop_location_score1 - means[3], 2);
			} else {
				cr.prop_location_score1 = means[3];
			}
			if (!cr.prop_location_score2_b) {
				stdvt[4] += Math.pow(cr.prop_location_score2 - means[4], 2);
			} else {
				cr.prop_location_score2 = means[4];
			}
			if (!cr.prop_log_historical_price_b) {
				stdvt[5] += Math
						.pow(cr.prop_log_historical_price - means[5], 2);
			} else {
				cr.prop_log_historical_price = means[5];
			}
			if (!cr.price_usd_b) {
				stdvt[6] += Math.pow(cr.price_usd - means[6], 2);
			} else {
				cr.price_usd = means[6];
			}
			if (!cr.srch_length_of_stay_b) {
				stdvt[7] += Math.pow(cr.srch_length_of_stay - means[7], 2);
			} else {
				cr.srch_length_of_stay = (int) means[7];
			}
			if (!cr.srch_booking_window_b) {
				stdvt[8] += Math.pow(cr.srch_booking_window - means[8], 2);
			} else {
				cr.srch_booking_window = (int) means[8];
			}
			if (!cr.srch_adults_count_b) {
				stdvt[9] += Math.pow(cr.srch_adults_count - means[9], 2);
			} else {
				cr.srch_adults_count = (int) means[9];
			}
			if (!cr.srch_children_count_b) {
				stdvt[10] += Math.pow(cr.srch_children_count - means[10], 2);
			} else {
				cr.srch_children_count = (int) means[10];
			}
			if (!cr.srch_room_count_b) {
				stdvt[11] += Math.pow(cr.srch_room_count - means[11], 2);
			} else {
				cr.srch_room_count = (int) means[11];
			}
			if (!cr.srch_query_affinity_score_b) {
				stdvt[12] += Math.pow(cr.srch_query_affinity_score - means[12],
						2);
			} else {
				cr.srch_query_affinity_score = means[12];
			}
			if (!cr.orig_destination_distance_b) {
				stdvt[13] += Math.pow(cr.orig_destination_distance - means[13],
						2);
			} else {
				cr.orig_destination_distance = means[13];
			}

			if (!cr.comp1_rate_percent_diff_b) {
				stdvt[14] += Math.pow(cr.comp1_rate_percent_diff - means[14], 2);
			} else {
				cr.comp1_rate_percent_diff = means[14];
			}
			if (!cr.comp2_rate_percent_diff_b) {
				stdvt[15] += Math.pow(cr.comp2_rate_percent_diff - means[15], 2);
			} else {
				cr.comp2_rate_percent_diff = means[15];
			}
			if (!cr.comp3_rate_percent_diff_b) {
				stdvt[16] += Math.pow(cr.comp3_rate_percent_diff - means[16], 2);
			} else {
				cr.comp3_rate_percent_diff = means[16];
			}
			if (!cr.comp4_rate_percent_diff_b) {
				stdvt[17] += Math.pow(cr.comp4_rate_percent_diff - means[17], 2);
			} else {
				cr.comp4_rate_percent_diff = means[17];
			}
			if (!cr.comp5_rate_percent_diff_b) {
				stdvt[18] += Math.pow(cr.comp5_rate_percent_diff - means[18], 2);
			} else {
				cr.comp5_rate_percent_diff = means[18];
			}
		}

		for (int i = 0; i < 19; i++) {
			stdvt[i] /= count[i];
			stdvt[i] = (float) Math.sqrt(stdvt[i]);

		}
	}

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

	CustomRowNominal[] allRows = new CustomRowNominal[9917530];
	CustomRowNumeric[] allRowsNumeric = new CustomRowNumeric[9917530];
	float[] means = new float[19];
	int[] count = new int[19];
	float[] stdvt = new float[19];

	Connection conn;

	public static void main(String[] args) {
		new KNN().start();
	}

	private void start() {
		initConnection();
		try {
			 getAll();
			 findKNN(50);
//			getAllNumeric();
//			countMeans();
//			findKNNNumeric(50);
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void findKNNNumeric(int i) throws SQLException {
		String q = "SELECT booking_bool, visitor_hist_starrating, visitor_hist_adr_usd ,prop_review_score, prop_location_score1,prop_location_score2, prop_log_historical_price, price_usd,"
				+ " srch_length_of_stay, srch_booking_window, srch_adults_count, srch_children_count, srch_room_count, srch_query_affinity_score,orig_destination_distance ,comp1_rate_percent_diff,comp2_rate_percent_diff ,comp3_rate_percent_diff,comp4_inv,comp4_rate_percent_diff,comp5_rate_percent_diff FROM train_set ORDER BY RAND() LIMIT 100";
		Statement stmt = conn.createStatement();
		int count = 0;
		if (stmt.execute(q)) {
			long startTime = System.currentTimeMillis();
			ResultSet rs = stmt.getResultSet();
			while (rs.next()) {
				CustomRowNumeric customRowNumeric = new CustomRowNumeric(rs);
				ArrayList<CustomRowNumeric> set = findKNNNumericFor(i,
						customRowNumeric);
				int countBooked = 0;
				for (CustomRowNumeric cr : set) {
					if (cr.booking_bool)
						countBooked++;
				}
				double p = (double) countBooked / set.size();
				if ((p > 0.5 && customRowNumeric.booking_bool)
						|| (p <= 0.5 && !customRowNumeric.booking_bool)) {
					count++;
				}
				//System.out.println(count);
			}
			long time = System.currentTimeMillis() - startTime;
			System.out.println("Numeric KNN: " + time);
		}
		System.out.println(count);
	}

	private void findKNN(int i) throws SQLException {
		String q = "SELECT booking_bool, site_id, visitor_location_country_id ,prop_country_id,prop_id, prop_starrating, prop_brand_bool, promotion_flag, srch_destination_id, srch_saturday_night_bool, comp1_rate, comp1_inv, comp2_rate,comp2_inv ,comp3_rate,comp3_inv ,comp4_rate,comp4_inv,comp5_rate,comp5_inv FROM train_set ORDER BY RAND() LIMIT 100";
		Statement stmt = conn.createStatement();
		int count = 0;
		if (stmt.execute(q)) {
			long startTime = System.currentTimeMillis();
			ResultSet rs = stmt.getResultSet();
			while (rs.next()) {
				CustomRowNominal customRowNominal = new CustomRowNominal(rs);
				ArrayList<CustomRowNominal> set = findKNNFor(i,
						customRowNominal);
//				int countBooked = 0;
//				for (CustomRowNominal cr : set) {
//					if (cr.booking_bool)
//						countBooked++;
//				}
//				double p = (double) countBooked / set.size();
//				if ((p > 0.5 && customRowNominal.booking_bool)
//						|| (p <= 0.5 && !customRowNominal.booking_bool)) {
//					count++;
//				}
//				System.out.println(count);
			}
			long time = System.currentTimeMillis() - startTime;
			System.out.println("KNN for nominal: " + time);
			
		}
		System.out.println(count);
	}

	private ArrayList<CustomRowNumeric> findKNNNumericFor(int i,
			CustomRowNumeric customRowNumeric) {
		ArrayList<CustomRowNumeric> list = new ArrayList<CustomRowNumeric>();
		for (CustomRowNumeric row : allRowsNumeric) {
			float dist = calculateDistanceNumeric(customRowNumeric, row);
			if (list.size() >= i) {

				CustomRowNumeric crTemp = getMaximumNumeric(list);
				if (crTemp == null) {
					int a = 1;
				}
				if (dist < crTemp.dist) {
					list.remove(crTemp);
					CustomRowNumeric crClone = new CustomRowNumeric(row);
					crClone.setDist(dist);
					list.add(crClone);
				}

			} else {
				CustomRowNumeric crClone = new CustomRowNumeric(row);
				crClone.setDist(dist);
				list.add(crClone);
			}
		}

		return list;
	}

	private CustomRowNumeric getMaximumNumeric(ArrayList<CustomRowNumeric> list) {
		CustomRowNumeric maximum = null;
		float maxDist = - Float.MAX_VALUE;
		for (CustomRowNumeric cr : list) {
			if (cr.dist > maxDist) {
				maxDist = cr.dist;
				maximum = cr;
			}
		}
		if(maximum==null) return list.get(0);
		return maximum;
	}

	private float calculateDistanceNumeric(CustomRowNumeric customRowNumeric,
			CustomRowNumeric row) {
		float dist = 0;

		customRowNumeric.visitor_hist_starrating =  (customRowNumeric.visitor_hist_starrating - means[0])/stdvt[0];
		row.visitor_hist_starrating = (row.visitor_hist_starrating - means[0])/stdvt[0];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.visitor_hist_adr_usd = (customRowNumeric.visitor_hist_adr_usd - means[1])/stdvt[1];
		row.visitor_hist_adr_usd = (row.visitor_hist_adr_usd - means[1])/stdvt[1];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.prop_review_score = (customRowNumeric.prop_review_score - means[2])/stdvt[2];
		row.prop_review_score = (row.prop_review_score - means[2])/stdvt[2];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.prop_location_score1 = (customRowNumeric.prop_location_score1 - means[3])/stdvt[3];
		row.prop_location_score1 = (row.prop_location_score1 - means[3])/stdvt[3];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.prop_location_score2 = (customRowNumeric.prop_location_score2 - means[4])/stdvt[4];
		row.prop_location_score2 = (row.prop_location_score2 - means[4])/stdvt[4];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.prop_log_historical_price = (customRowNumeric.prop_log_historical_price - means[5])/stdvt[5];
		row.prop_log_historical_price = (row.prop_log_historical_price - means[5])/stdvt[5];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.price_usd = (customRowNumeric.price_usd - means[6])/stdvt[6];
		row.price_usd = (row.price_usd - means[6])/stdvt[6];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.srch_length_of_stay = (int) ((customRowNumeric.srch_length_of_stay - means[7])/stdvt[7]);
		row.srch_length_of_stay = (int) ((row.srch_length_of_stay - means[7])/stdvt[7]);
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.srch_booking_window = (int) ((customRowNumeric.srch_booking_window - means[8])/stdvt[8]);
		row.srch_booking_window = (int) ((row.srch_booking_window - means[8])/stdvt[8]);
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.srch_adults_count = (int) ((customRowNumeric.srch_adults_count - means[9])/stdvt[9]);
		row.srch_adults_count = (int) ((row.srch_adults_count - means[9])/stdvt[9]);
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.srch_children_count = (int) ((customRowNumeric.srch_children_count - means[10])/stdvt[10]);
		row.srch_children_count = (int) ((row.srch_children_count - means[10])/stdvt[10]);
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.srch_room_count = (int) ((customRowNumeric.srch_room_count - means[11])/stdvt[11]);
		row.srch_room_count = (int) ((row.srch_room_count - means[11])/stdvt[11]);
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.srch_query_affinity_score = (customRowNumeric.srch_query_affinity_score - means[12])/stdvt[12];
		row.srch_query_affinity_score = (row.srch_query_affinity_score - means[12])/stdvt[12];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.orig_destination_distance = (customRowNumeric.orig_destination_distance - means[13])/stdvt[13];
		row.orig_destination_distance = (row.orig_destination_distance - means[13])/stdvt[13];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.comp1_rate_percent_diff = (customRowNumeric.comp1_rate_percent_diff - means[14])/stdvt[14];
		row.comp1_rate_percent_diff = (row.comp1_rate_percent_diff - means[14])/stdvt[14];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.comp2_rate_percent_diff = (customRowNumeric.comp2_rate_percent_diff - means[15])/stdvt[15];
		row.comp2_rate_percent_diff = (row.comp2_rate_percent_diff - means[15])/stdvt[15];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.comp3_rate_percent_diff = (customRowNumeric.comp3_rate_percent_diff - means[16])/stdvt[16];
		row.comp3_rate_percent_diff = (row.comp3_rate_percent_diff - means[16])/stdvt[16];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.comp4_rate_percent_diff = (customRowNumeric.comp4_rate_percent_diff - means[17])/stdvt[17];
		row.comp4_rate_percent_diff = (row.comp4_rate_percent_diff - means[17])/stdvt[17];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		customRowNumeric.comp5_rate_percent_diff = (customRowNumeric.comp5_rate_percent_diff - means[18])/stdvt[18];
		row.comp5_rate_percent_diff = (row.comp5_rate_percent_diff - means[18])/stdvt[18];
		dist += Math.pow(customRowNumeric.visitor_hist_starrating - row.visitor_hist_starrating,2);
		dist /= 19;

		return Float.MAX_VALUE - dist;
	}

	private ArrayList<CustomRowNominal> findKNNFor(int i,
			CustomRowNominal customRowNominal) {

		ArrayList<CustomRowNominal> list = new ArrayList<CustomRowNominal>();
		for (CustomRowNominal row : allRows) {
			float dist = calculateDistance(customRowNominal, row);
			if (list.size() >= i) {

				CustomRowNominal crTemp = getMaximum(list);
				if (crTemp == null) {
					int a = 1;
				}
				if (dist < crTemp.dist) {
					list.remove(crTemp);
					CustomRowNominal crClone = new CustomRowNominal(row);
					crClone.setDist(dist);
					list.add(crClone);
				}

			} else {
				CustomRowNominal crClone = new CustomRowNominal(row);
				crClone.setDist(dist);
				list.add(crClone);
			}
		}

		// System.out.println(getMaximum(list).dist);
		return list;

	}

	private CustomRowNominal getMaximum(ArrayList<CustomRowNominal> list) {
		CustomRowNominal maximum = null;
		float maxDist = - Float.MAX_VALUE;
		for (CustomRowNominal cr : list) {
			if (cr.dist > maxDist) {
				maxDist = cr.dist;
				maximum = cr;
			}
		}
		if(maximum==null) return list.get(0);
		return maximum;
	}

	private float calculateDistance(CustomRowNominal customRowNominal,
			CustomRowNominal row) {
		float common = 0;
		float sum = 0;

		if (customRowNominal.site_id == row.site_id) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.comp1_inv == row.comp1_inv) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.comp1_rate == row.comp1_rate) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.comp2_inv == row.comp2_inv) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.comp2_rate == row.comp2_rate) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.comp3_inv == row.comp3_inv) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.comp3_rate == row.comp3_rate) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.comp4_inv == row.comp4_inv) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.comp4_rate == row.comp4_rate) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.comp5_inv == row.comp5_inv) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.comp5_rate == row.comp5_rate) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.promotion_flag == row.promotion_flag) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.prop_brand_bool == row.prop_brand_bool) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.prop_country_id == row.prop_country_id) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.prop_id == row.prop_id) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.prop_starrating == row.prop_starrating) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.srch_destination_id == row.srch_destination_id) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.srch_saturday_night_bool == row.srch_saturday_night_bool) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		if (customRowNominal.visitor_location_country_id == row.visitor_location_country_id) {
			common++;
			sum++;
		} else {
			sum += 2;
		}

		return (float) (1.0 - (common / sum));

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
