package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import weka.classifiers.Classifier;
import weka.classifiers.functions.Logistic;
import weka.core.Instance;
import weka.core.Instances;
import main.KNN.CustomRowNominal;
import main.KNN.CustomRowNumeric;

public class ARFFGenerator {

	String path = "C:\\Users\\Karol\\train.arff";
	String pathTest = "C:\\Users\\Karol\\test1.arff";

	String result = "C:\\Users\\Karol\\result.csv";
	String[] testPaths = { "C:\\Users\\Karol\\test1.arff",
			"C:\\Users\\Karol\\test2.arff", "C:\\Users\\Karol\\test3.arff",
			"C:\\Users\\Karol\\test4.arff", "C:\\Users\\Karol\\test5.arff",
			"C:\\Users\\Karol\\test6.arff", "C:\\Users\\Karol\\test7.arff" };

	class Tuple {
		private String key;
		private double value;

		Tuple(String key, double value) {
			this.key = key;
			this.value = value;
		}

		public String getKey() {
			return key;
		}

		public double getValue() {
			return value;
		}
	}

	Set<Integer> booked = new HashSet<Integer>();
	Set<Integer> nonbooked = new HashSet<Integer>();

	Connection conn;

	public static void main(String[] args) {
		new ARFFGenerator().start();
	}

	private void start() {
		initConnection();
		// File f = new File();
		PrintWriter writer;

		try {
//			buildTrainFile();

//			buildTestFile(pathTest);
			testInstances();

		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	private void buildTrainFile() throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(path, "UTF-8");
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

			String q = "SELECT srch_id, prop_id, booking_bool, visitor_hist_starrating, visitor_hist_adr_usd ,prop_review_score, prop_location_score1,prop_location_score2, prop_log_historical_price, price_usd,"
					+ " srch_length_of_stay, srch_booking_window, srch_adults_count, srch_children_count, srch_room_count, srch_query_affinity_score,orig_destination_distance ,comp1_rate_percent_diff,comp2_rate_percent_diff ,comp3_rate_percent_diff,comp4_inv,comp4_rate_percent_diff,comp5_rate_percent_diff FROM train_set";
			Statement stmt = conn.createStatement(
					java.sql.ResultSet.TYPE_FORWARD_ONLY,
					java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
			if (stmt.execute(q)) {
				ResultSet rs = stmt.getResultSet();
				int i = 0;
				while (rs.next()) {
					if (rs.getInt("booking_bool") == 1
							&& !booked.contains(rs.getInt("srch_id"))) {
						booked.add(rs.getInt("srch_id"));
						writer.println(rs.getDouble("prop_location_score1")
								+ "," + rs.getDouble("prop_location_score2")
								+ "," + rs.getDouble("prop_review_score") + ","
								+ rs.getInt("booking_bool"));
					} else if (rs.getInt("booking_bool") == 0
							&& !nonbooked.contains(rs.getInt("srch_id"))) {
						nonbooked.add(rs.getInt("srch_id"));
						writer.println(rs.getDouble("prop_location_score1")
								+ "," + rs.getDouble("prop_location_score2")
								+ "," + rs.getDouble("prop_review_score") + ","
								+ rs.getInt("booking_bool"));

					}
					// allRowsNumeric[i] = new CustomRowNumeric(rs);
					// if(booked.size() >= 100 && nonbooked.size() >=100) {
					// break;
					// }
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

	}

	private void testInstances() throws Exception {

		PrintWriter writer2 = new PrintWriter(new FileOutputStream(new File(
				result), true));
		writer2.println("SearchId,PropertyId");
		writer2.close();

		BufferedReader readerTrain = new BufferedReader(new FileReader(path));
		BufferedReader[] readerTest = new BufferedReader[testPaths.length];
		for (int i = 0; i < readerTest.length; i++) {
			readerTest[i] = new BufferedReader(new FileReader(testPaths[i]));
		}
		BufferedReader[] readerTest2 = new BufferedReader[testPaths.length];
		for (int i = 0; i < readerTest.length; i++) {
			readerTest2[i] = new BufferedReader(new FileReader(testPaths[i]));
		}

		Classifier classifier = new Logistic();

		Instances instances = new Instances(readerTrain);
		if (instances.classIndex() == -1)
			instances.setClassIndex(instances.numAttributes() - 1);
		classifier.buildClassifier(instances);
		System.out.println("trainset");

		for (int j = 0; j < readerTest.length; j++) {
			Instances testSet = new Instances(readerTest[j]);
			System.out.println("testset1");
			Instances testSet2 = new Instances(readerTest2[j]);
			System.out.println("trainset2");
			testSet.deleteStringAttributes();
			System.out.println("deleted");
			TreeMap<Integer, ArrayList<Tuple>> map = new TreeMap<Integer, ArrayList<Tuple>>();

			int good = 0;
			for (int i = 0; i < testSet.numInstances(); i++) {
				Instance instance = testSet.instance(i);
				// double prediction =
				// classifier.classifyInstance(instance);
				double[] dist = classifier.distributionForInstance(instance);
				String[] idStr = testSet2.instance(i).stringValue(0).split(" ");
				int srch_id = Integer.parseInt(idStr[0]);
				int prop_id = Integer.parseInt(idStr[1]);
				if (map.containsKey(srch_id)) {
					map.get(srch_id).add(
							new Tuple(Integer.toString(prop_id), dist[1]));
				} else {
					ArrayList<Tuple> list = new ArrayList<Tuple>();
					list.add(new Tuple(Integer.toString(prop_id), dist[1]));
					map.put(srch_id, list);
				}
				if (i % 100 == 0)
					System.out.println(i);
				// double truth = instance.classValue();

				// System.out.println();

			}
			PrintWriter writer = new PrintWriter(new FileOutputStream(new File(
					result), true));

			System.out.println("RESULT");
			for (Integer key : map.keySet()) {
				ArrayList<Tuple> list = map.get(key);
				Collections.sort(list, new Comparator<Tuple>() {
					@Override
					public int compare(Tuple o1, Tuple o2) {
						// TODO Auto-generated method stub
						return new Double(o2.getValue()).compareTo(o1
								.getValue());
					}
				});
				for (Tuple tuple : list) {
					System.out.println(key + " " + tuple.getKey() + " "
							+ tuple.getValue());
					writer.println(key + "," + tuple.getKey());
				}
			}
			writer.close();

		}

	}

	private void buildTestFile(String pathTest) throws FileNotFoundException,
			UnsupportedEncodingException {
		int limit = 0;
		for (String path : testPaths) {

			PrintWriter writer = new PrintWriter(path, "UTF-8");
			writer.println("@RELATION expediaTest");
			writer.println("");
			writer.println("@ATTRIBUTE srch_id_prop_id string");
			writer.println("@ATTRIBUTE prop_location_score1 NUMERIC");
			writer.println("@ATTRIBUTE prop_location_score2 NUMERIC");
			writer.println("@ATTRIBUTE prop_review_score NUMERIC");
			writer.println("@ATTRIBUTE class {0,1}");
			writer.println("");
			writer.println("@DATA");
			try {
				conn.setAutoCommit(false);

				String q = "SELECT srch_id, prop_id, visitor_hist_starrating, visitor_hist_adr_usd ,prop_review_score, prop_location_score1,prop_location_score2, prop_log_historical_price, price_usd,"
						+ " srch_length_of_stay, srch_booking_window, srch_adults_count, srch_children_count, srch_room_count, srch_query_affinity_score,orig_destination_distance ,comp1_rate_percent_diff,comp2_rate_percent_diff ,comp3_rate_percent_diff,comp4_inv,comp4_rate_percent_diff,comp5_rate_percent_diff FROM test_set LIMIT "
						+ limit * 1000000 + ", " + 1000000;
				Statement stmt = conn.createStatement(
						java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY);
				stmt.setFetchSize(Integer.MIN_VALUE);
				if (stmt.execute(q)) {
					ResultSet rs = stmt.getResultSet();
					int i = 0;
					while (rs.next()) {
						writer.println("\"" + rs.getInt("srch_id") + " "
								+ rs.getInt("prop_id") + "\","
								+ rs.getDouble("prop_location_score1") + ","
								+ rs.getDouble("prop_location_score2") + ","
								+ rs.getDouble("prop_review_score") + ",0");
						// allRowsNumeric[i] = new CustomRowNumeric(rs);
						// if(booked.size() >= 100 && nonbooked.size() >=100) {
						// break;
						// }
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
			limit++;
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
