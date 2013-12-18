package main;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import main.KNN.CustomRowNumeric;
import weka.classifiers.Classifier;
import weka.classifiers.functions.Logistic;
import weka.classifiers.functions.SimpleLogistic;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ConverterUtils;

public class NaiveClassifier {

	private Connection conn;
	private HashMap<Integer, Instance> booked;
	private HashMap<Integer, Double> class_booked;
	private HashMap<Integer, Instance> nonbooked;
	private HashMap<Integer, Double> class_nonbooked;

	public static void main(String[] args) {
		new NaiveClassifier().start();
	}

	void start() {
		initConnection();
		try {
			conn.setAutoCommit(false);

			booked = new HashMap<Integer, Instance>();
			nonbooked = new HashMap<Integer, Instance>();
			class_booked = new HashMap<Integer, Double>();
			class_nonbooked = new HashMap<Integer, Double>();
			
			String q = "SELECT booking_bool, click_bool, srch_id, prop_id, prop_review_score, prop_location_score1, prop_location_score2 FROM train_set";
			Statement stmt = conn.createStatement(
					java.sql.ResultSet.TYPE_FORWARD_ONLY,
					java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
			if (stmt.execute(q)) {
				ResultSet rs = stmt.getResultSet();
				int i = 0;
				while (rs.next()) {
					// allRowsNumeric[i] = new CustomRowNumeric(rs);
					boolean booking_bool = rs.getBoolean("booking_bool");
					double booking_bool_class = 0;
					if (booking_bool) {
						booking_bool_class = 1.0;
					}
					int srch_id = rs.getInt("srch_id");
					double prop_review_score = rs
							.getDouble("prop_review_score");
					double prop_location_score1 = rs
							.getDouble("prop_location_score1");
					double prop_location_score2 = rs
							.getDouble("prop_location_score2");
					Instance instance = new Instance(4);
					
					instance.setValue(0, prop_review_score);
					instance.setValue(1, prop_location_score1);
					instance.setValue(2, prop_location_score2);

					if (booking_bool) {
						if (!booked.containsKey(srch_id)) {
							
							booked.put(srch_id, instance);
						}
					} else {
						if (!nonbooked.containsKey(srch_id)) {

							nonbooked.put(srch_id, instance);
						}
					}
					i++;
					if (i % 100 == 0) {
						System.out.println(i);
					}
				}
			}
			
			Instances i = new Instances(new FileReader(new File())) ;

			
			Instances instances1 = (Instances) booked.values();
			
			for(int i = 0; i < instances1.numInstances(); i++) {
				instances1.instance(i).setClassValue(1.0);
			}
			
			Instances instances2 = (Instances) nonbooked.values();
			
			for(int i = 0; i < instances2.numInstances(); i++) {
				instances2.instance(i).setClassValue(0.0);
			}
			
			Instances instances = Instances.mergeInstances(instances1, instances2);
			
			Classifier classifier = new SimpleLogistic();
			System.out.println("Start building classifier");
			try {
				classifier.buildClassifier(instances);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Classifier built");
			

			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void getAllNumeric() throws SQLException {

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
