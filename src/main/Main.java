package main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeSet;

public class Main {

	class CustomRow {
		public int searchId;
		public int propId;
		boolean bookingBool;
		int country;
		float starrating;
		float usd;

		 
		public CustomRow(ResultSet rs) {
			super();
			try {
				this.searchId = rs.getInt("srch_id");
			
			this.propId = rs.getInt("prop_id");
			this.bookingBool = rs.getBoolean("booking_bool");
			this.country = rs.getInt("visitor_location_country_id");
			this.starrating = rs.getFloat("visitor_hist_starrating");
			this.usd = rs.getFloat("visitor_hist_adr_usd");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private Connection conn;
	private ArrayList<CustomRow> all = new ArrayList<CustomRow>();

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new Main().start();

	}

	private float distance(CustomRow cs, CustomRow cs1) {
		// TODO Auto-generated method stub
		float distance = 0;
		if (cs.country != cs1.country)
			distance++;

		distance += Math.abs(cs.usd - cs1.usd) / 100;
		distance += Math.abs(cs.starrating - cs1.starrating);

		if (cs.usd == 0 || cs1.usd == 0)
			distance += 5;
		if (cs.starrating == 0 || cs1.starrating == 0)
			distance += 5;

		return distance;

	}
	
	class Tuple<CustomRow, Float> { 
	    public final CustomRow x; 
	    public final Float y; 
	    public Tuple(CustomRow x, Float y) { 
	      this.x = x; 
	      this.y = y; 
	    } 
	  }

	

	private void findNear(CustomRow cs) {
		// TODO Auto-generated method stub
		TreeSet<Tuple<CustomRow, Float>> knn = new TreeSet<Tuple<CustomRow, Float>>(new Comparator<Tuple<CustomRow, Float>>() {
			@Override
			public int compare(Tuple<CustomRow, java.lang.Float> o1,
					Tuple<CustomRow, java.lang.Float> o2) {
				// TODO Auto-generated method stub
				return o1.y > o2.y ? 1 : o1.y == o2.y ? 0 : -1;
			}
		});
		
		for(CustomRow cs2 : all){
			if(knn.size() >= 50){
//				CustomRow cs
			}
		}
		
		
	}

	private void start() {
		// TODO Auto-generated method stub

		String query = "select srch_id, prop_id, visitor_location_country_id, visitor_hist_starrating, visitor_hist_adr_usd, booking_bool from train_set order by rand() limit 10000";
		String selectAll = "select srch_id, prop_id, visitor_location_country_id, visitor_hist_starrating, visitor_hist_adr_usd, booking_bool from train_set";

		ResultSet rs;

		System.out.println("Start loading data...");
		  try {
		   PreparedStatement p = conn.prepareStatement(selectAll);
		   rs = p.executeQuery();
		   int  i=0;
		   while (rs.next()) {
		    all.add(new CustomRow(rs));
		    i++;
		    if(i % 10000 == 0) System.out.println(i);
		    
		   }
		  } catch (SQLException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
		  }
		  System.out.println("Data loading finished");
		/*
		 * String query = "select * from testset order by rand() limit 10000";
		 * String selectAll = "select  from testset";
		 */

		try {
			PreparedStatement p = conn.prepareStatement(query);
			rs = p.executeQuery();

			while (rs.next()) {
				findNear(new CustomRow(rs));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
