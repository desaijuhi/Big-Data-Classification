/*
 *
 * Question 1 - Decision Tree on KKD dataset
 *
 * Group:
 * ------
 * desaijuhi
 * londttrev
 * mehtashwe
 *
 */
package comp473;

/*
 * Java imports
 */
import java.util.HashMap;
import java.util.Map;
import java.io.*;
import java.net.URI;

/*
 * Spark packages required
 */
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;

/*
 * Hadoop packages required
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class QuestionOneDT 
{
	/*
	 * Driver
	 */
  	public static void main(String[] args) 
  	{
		// Turn logging level down - we don't need all this noise
    		Logger.getLogger("org").setLevel(Level.WARN); 
    		Logger.getLogger("akka").setLevel(Level.WARN);

    		SparkConf sparkConf = new SparkConf().setAppName("QuestionOneDT");
    		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    		// Load and parse the data file.
		String datapath = args[0].trim();
    		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), datapath).toJavaRDD();
    		
		// Split the data into training and test sets (30% held out for testing)
    		JavaRDD<LabeledPoint>[] splits = data.randomSplit( new double[]{0.7, 0.3},12345);
    		JavaRDD<LabeledPoint> trainingData = splits[0].cache();
    		JavaRDD<LabeledPoint> testData = splits[1];

   		 // Set parameters.
    		int numClasses = 2;
    		Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
    		String impurity = "gini";
    		int maxDepth = Integer.parseInt(args[1].trim());
    		int maxBins = Integer.parseInt(args[2].trim());

    	// Train a DecisionTree model for classification.
    		
		long startTime = System.nanoTime();    
		final DecisionTreeModel model = DecisionTree.trainClassifier(trainingData,
									numClasses,
									categoricalFeaturesInfo,
									impurity,
									maxDepth,
									maxBins);
		long estimatedTime = System.nanoTime() - startTime;

    		// Evaluate model on test data
    		JavaPairRDD<Object, Object> predictionAndLabel = testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
    		double testErr = predictionAndLabel.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testData.count();

    		// Evaluate model on training data
    		JavaPairRDD<Object, Object> predictionAndLabeltrain = trainingData.mapToPair(p -> new Tuple2<>(model.predict(p.features()),p.label()));
    		double trainErr = predictionAndLabeltrain.filter(pl -> !pl._1().equals(pl._2())).count() / (double) trainingData.count();

			// Save metrics to a file on hadoop
    		MulticlassMetrics  metrics = new MulticlassMetrics( predictionAndLabel.rdd() );
    		Matrix confusionTest = metrics.confusionMatrix();
    		double testAccuracy = metrics.accuracy() * 100.0;

    		metrics = new MulticlassMetrics( predictionAndLabeltrain.rdd() );
    		Matrix confusionTrain = metrics.confusionMatrix();
    		double trainAccuracy = metrics.accuracy() * 100.0;

    		try
   		 {
	    		String outputStr = 
				"Input file: "
				+ datapath
				+ "\n"
				+ "Time to train model: "
				+ (double)estimatedTime / 1000000000.00 
				+ " seconds\n"
				+ "Test set accuracy: "
		    		+ testAccuracy
		    		+ "\n"
		    		+ "Test set confusion matrix:\n"
		    		+ confusionTest
		    		+"\n"
		    		+ "Training set accuracy: "
		    		+ trainAccuracy
		    		+ "\n"
		    		+ "Training set confusion matrix:\n"
		    		+ confusionTrain
		    		+ "\n"
		    		+ model.toDebugString() +"\n";  
	    
			byte[] b = outputStr.getBytes();
	    		Configuration conf = new Configuration();
            		conf.get("fs.defaultFS");
			String outFileName = "target/results_q1dt_"+args[1].trim()+"_"+args[2].trim()+".txt";
            		FileSystem fs = FileSystem.get(URI.create(outFileName),conf);
            		FSDataOutputStream out = fs.create( new Path(outFileName));
            		out.write( b, 0, b.length );
    		}
    		catch ( IOException e)
    		{
           		 System.out.println( "Error writing results." );
   		 }
		jsc.stop();
 	 }
}
