package com.sample.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class MyfirstHBaseTable {

	public static final String TABLE = "JR_TEST"; // Table names are
													// case-sensitive

	public static void main(String[] args) throws IOException {
		Configuration hconfig = HBaseConfiguration.create(new Configuration());

		HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(TABLE));
		htable.addFamily(new HColumnDescriptor("personal")); //Add column family
		htable.addFamily(new HColumnDescriptor("professional"));

		System.out.println("Connecting...");
		HBaseAdmin hbase_admin = new HBaseAdmin(hconfig);
		
		System.out.println("Creating Table...");
		
		//create 'JR_TEST', 'personal', 'professional'
		hbase_admin.createTable(htable);
		
		//Getting all the list of tables using HBaseAdmin object
		System.out.println("Listing Tables and corresponding Column Families");
		HTableDescriptor[] tableDescriptor =hbase_admin.listTables();
		
		for (HTableDescriptor tblDescriptor : tableDescriptor) {
			System.out.println(tblDescriptor.getNameAsString());
			System.out.println(tblDescriptor.getColumnFamilies().length);
			//Listing column families of a table
			for (HColumnDescriptor colDescriptor : tblDescriptor.getColumnFamilies()) {
				System.out.println(colDescriptor.getNameAsString());
			}
		}
		
		//Insert
		System.out.println("Populating Table");
		HTable hTable = new HTable(hconfig, TABLE);
		
		Put p = new Put(Bytes.toBytes("101"));
		//put 'JR_TEST', '101', 'personal:name', 'jyoti'
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("name"),Bytes.toBytes("jyoti"));
		//put 'JR_TEST', '101', 'personal:city', 'pune'
		p.add(Bytes.toBytes("personal"), Bytes.toBytes("city"),Bytes.toBytes("pune"));
		
		//put 'JR_TEST', '101', 'professional:dept', 'engineering'
		p.add(Bytes.toBytes("professional"), Bytes.toBytes("dept"),Bytes.toBytes("engineering"));
		
		hTable.put(p);
		
		
		//Read
		//get 'JR_TEST', '101'
		//or get 'JR_TEST', ‘101’, {COLUMN => ‘personal:city’}
		System.out.println("Reading city column of record 101");
		Get get = new Get(Bytes.toBytes("101"));
		//get.addFamily(Bytes.toBytes("personal"));
		get.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"));
		
		Result result = hTable.get(get);
		byte [] value = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city"));
		String city = Bytes.toString(value);
		System.out.println("for 101 : city : " + city);

		
		
		//Update
		System.out.println("Updating record 101 : city -> Bangalore");
		//put ‘JR_TEST’,’101’,'personal:city',’Bangalore’
		Put pt = new Put(Bytes.toBytes("101"));
		pt.add(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("Bangalore"));
		hTable.put(pt);
		
		//Delete
		System.out.println("Deleting record 101 ...");
		//delete ‘JR_TEST’, ‘101’, ‘personal:city’, ‘<time stamp>’ 
		//deleteall ’JR_TEST’,’101’
		Delete delete = new Delete(Bytes.toBytes("101"));
		//delete.deleteColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
		hTable.delete(delete);
		
		hTable.close();
		
		System.out.println("Done!");
		hbase_admin.close();
	}

}