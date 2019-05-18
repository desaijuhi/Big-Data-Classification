import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import java.math.BigDecimal;
public class SUSYConvert
{


    public void processFile( String name )
    {
	try
	{
	    File file = new File( name );
	    BufferedWriter writer = new BufferedWriter( new FileWriter( file +".processed.data" ) );

	    Scanner scanner = new Scanner( file );
	    while( scanner.hasNext() )
	    {
            String[] tokens = scanner.nextLine().split( "," );

            // normalize the tokens
            double max = new BigDecimal(tokens[1].trim()).doubleValue();
            double min = max;
		
            for ( int i = 1; i < tokens.length; i++ )
            {
                double v = new BigDecimal(tokens[i].trim()).doubleValue();
                if(  v > max )
                {
                max = v;
                }
                if(  v < min )
                {
                min = v;
                }
            }
		
            for ( int i = 1; i < tokens.length; i++ )
            {
                double v = new BigDecimal(tokens[i].trim()).doubleValue();
                tokens[i] = Double.toString( ((v - min) / ( max - min )) );
            }

		
            StringBuilder sb = new StringBuilder();

		// 1 | 0
		String label_ = tokens[0];
		int valint_ = new BigDecimal(label_.trim()).intValue();
		
		if ( valint_ == 0 )
		    sb.append("0 ");
		else
		    sb.append("1 ");

		for( int i=1; i < tokens.length; i++ )
		{ 
		    sb.append( Integer.toString(i) );
		    sb.append(":");
		    double valdbl_ = new BigDecimal(tokens[i].trim()).doubleValue();    
		    sb.append( valdbl_ );
		    if( i <  tokens.length - 1 )
			sb.append(" ");
		}		

		sb.append( "\n" );
		writer.write( sb.toString() );
	    }
	    scanner.close();	
	    writer.close();
	} 
	catch( IOException e )
	{
	    System.out.println( "Could not open file!");
	    System.exit(-1);
	}
    }

    public static void main(String[] args)
    {
	SUSYConvert convert = new SUSYConvert();	
	convert.processFile( "/home/desaijuhi/comp473_Assignment_3/subset.csv" );
	System.out.println("Done.");	
    }

}
