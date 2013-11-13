package main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Main {

	class CustomRow {
		public int searchId;
		public int propId;
		boolean bookingBool;
		int country;
		float starrating;
		float usd;
		public CustomRow(int searchId, int propId, boolean bookingBool,
				int country, float starrating, float usd) {
			super();
			this.searchId = searchId;
			this.propId = propId;
			this.bookingBool = bookingBool;
			this.country = country;
			this.starrating = starrating;
			this.usd = usd;
		}
		
	}
	
	private Connection conn;

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

		try {
			

			if (cs.country != cs1.country) distance++;
			
			distance+= Math.abs(usd-usd1)/100;
			distance+= Math.abs(starrating-starrating1);
			
			if(usd==0 || usd1==0) distance+=5;
			if(starrating==0 || starrating1==0) distance+=5;
			
			return distance;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 10000;
		

	}

	private void findNear(ResultSet rs) {
		// TODO Auto-generated method stub
		List
	}

	private void start() {
		// TODO Auto-generated method stub
		String query = "select * from testset order by rand() limit 10000";
		String selectAll = "select  from testset";
		ResultSet rs;

		try {
			PreparedStatement p = conn.prepareStatement(query);
			rs = p.executeQuery();

			while (rs.next()) {
				findNear(rs);
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
