package assignment182;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class TableCreator 
{
	public static void main( String[] args ) throws Exception
	{
		Configuration conf = HBaseConfiguration.create();
		
		HBaseAdmin admin = new HBaseAdmin( conf );
		
		createTable( "customer" , new String[]{"details"}, admin );
		
	}	

	
	@SuppressWarnings("deprecation")
	public static void createTable( String strTableName, String[] aColumnFamilies, HBaseAdmin admin ) throws IOException
	{
		
		HTableDescriptor tableDescriptor = new HTableDescriptor( Bytes.toBytes( strTableName ) );
		
		//Add column families
		for ( int i = 0; i < aColumnFamilies.length; i++ ) 
		{
			tableDescriptor.addFamily( new HColumnDescriptor( aColumnFamilies[ i ] ) );
		}
		
		//If table already exists, delete it
		if ( admin.tableExists( strTableName ) ) 
		{
			System.out.println( "Table " +strTableName+ " already existed." );
			
			admin.disableTable( strTableName );
			
			admin.deleteTable( strTableName );
			
			System.out.println( "Already existing table disabled and deleted" );
		}
	
		//Create Table
		admin.createTable( tableDescriptor );
		
		System.out.println( "Table created." );
		
		
	}

}
