package jdbc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
	 
	class jdbcTest {
	 
	    private static final String url = "jdbc:mysql://localhost:3306/";
	    private static final String user = "root";
	    private static final String password = "";
	 
	    public static void main(String args[]) {
	        try {
	        	File newFile = new File("hadoop1.txt");
	        	FileWriter fw = new FileWriter(newFile);
	        	BufferedWriter out = new BufferedWriter(fw);
	            Class.forName("com.mysql.jdbc.Driver").newInstance();
	            Connection con = DriverManager.getConnection(url, user, password);
	            Statement stt = con.createStatement();
	            stt.execute("USE wordpress");
	            ResultSet res = stt.executeQuery("SELECT b.phone_name,comment_content FROM wp_comments a ,phone_index b WHERE a.comment_post_ID = b.comment_post_ID");  
	            while (res.next())
	               {
	            	out.write(res.getString("phone_name")+"\t"+res.getString("comment_content").replace("\n","").replace("\r",""));
	            	out.newLine();
	               }
	            System.out.println("File is retrieved"); 
	            out.close();
	            res.close();  
	            stt.close();
	            con.close();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	    }
	}