package nl.tno.stormcv.util;

import nl.tno.stormcv.bolt.CVParticleBolt;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.operation.CannyEdgeOp;
import nl.tno.stormcv.operation.ColorHistogramOp;
import nl.tno.stormcv.operation.DrawFeaturesOp;
import nl.tno.stormcv.operation.FeatureExtractionOp;
import nl.tno.stormcv.operation.GrayscaleOp;
import nl.tno.stormcv.operation.HaarCascadeOp;
import nl.tno.stormcv.operation.ImageEnhancementOp;
import nl.tno.stormcv.operation.ROIExtractionOp;
import nl.tno.stormcv.operation.ScaleImageOp;

public class OperationUtils {

	public static String[] singleOperations = {"scale", "gray", "face_detect", "colorhistogram",
		"face_extraction", "sift_features", "canny_edge","enhance","draw"};
	//private String[] batchOperations = {};
	public static CVParticleBolt operationToBolt(String operation) {

		if (operation.equals("gray")) {
			return new SingleInputBolt(new GrayscaleOp());
		}
		if (operation.equals("face_detect")) {
			return new SingleInputBolt(new HaarCascadeOp("face",
					"lbpcascade_frontalface.xml").outputFrame(true));
		}
		if (operation.equals("scale")) {
			return new SingleInputBolt(new ScaleImageOp(0.66f));
		}
		if (operation.equals("colorhistogram")) {
			return new SingleInputBolt(
					new ColorHistogramOp("colorhistogram").outputFrame(true));
		}
		if (operation.equals("face_extraction")) {
			return new SingleInputBolt(
					new ROIExtractionOp("face").spacing(25));
		}
		
		if (operation.equals("sift_features")) {
			return new SingleInputBolt(
					new FeatureExtractionOp("sift_features", org.opencv.features2d.FeatureDetector.SIFT, org.opencv.features2d.DescriptorExtractor.SIFT).outputFrame(true));
		}
		
		if(operation.equals("canny_edge")) {
			return new SingleInputBolt(
					new CannyEdgeOp("canny_edge"));
		}
		
		if(operation.equals("enhance")) {
			return new SingleInputBolt(
					new ImageEnhancementOp("enhance"));
		}
		
		
		if (operation.equals("draw")) {
			return new SingleInputBolt(
					new DrawFeaturesOp());
		}
		return null;
	}

	public static Boolean isSingleOperation(String operation) {
		for (int i = 0; i < singleOperations.length; i++) {
			if (singleOperations[i].equals(operation)) return true;
		}
		return false;
	}
	
//	public static String getAppNameFromOp(String op) {
//		String appName ="";
//		if(op == "gray") {
//			appName = "gray_app";
//		} else if (op == "scale") {
//			appName = "scale_app";
//		} else if (op == "colorhistogram") {
//			appName = "colorhistogram_app";
//		} else if (op == )
//	}
}
