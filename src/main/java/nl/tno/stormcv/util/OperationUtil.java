package nl.tno.stormcv.util;

import nl.tno.stormcv.bolt.SingleJPEGInputBolt;
import nl.tno.stormcv.constant.BOLT_HANDLE_TYPE;
import nl.tno.stormcv.operation.single.*;

public class OperationUtil {

    public static String[] singleOperations = {"scale", "gray", "face_detect", "colorhistogram",
            "face_extraction", "sift_features", "canny_edge", "enhance", "draw"};

    public static SingleJPEGInputBolt operationToBolt(String operation) {

        if (operation.equals("gray")) {
            return new SingleJPEGInputBolt(new GrayImageOp(), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_MAT);
        }
        if (operation.equals("face_detect")) {
            return new SingleJPEGInputBolt(new HaarCascadeOp("face",
                    "lbpcascade_frontalface.xml").outputFrame(true), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_MAT);
        }
        if (operation.equals("scale")) {
            return new SingleJPEGInputBolt(new ScaleImageOp(0.66f), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE);
        }
        if (operation.equals("colorhistogram")) {
            return new SingleJPEGInputBolt(
                    new ColorHistogramOp("colorhistogram").outputFrame(true), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE);
        }
        if (operation.equals("face_extraction")) {
            return new SingleJPEGInputBolt(
                    new ROIExtractionOp("face").spacing(25), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE);
        }

        if (operation.equals("sift_features")) {
            return new SingleJPEGInputBolt(
                    new FeatureExtractionOp("sift_features", org.opencv.features2d.FeatureDetector.SIFT, org.opencv.features2d.DescriptorExtractor.SIFT).outputFrame(true), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_MAT);
        }

        if (operation.equals("canny_edge")) {
            return new SingleJPEGInputBolt(
                    new CannyEdgeOp("canny_edge"), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_MAT);
        }

        if (operation.equals("enhance")) {
            return new SingleJPEGInputBolt(
                    new ImageEnhancementOp("enhance"), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_MAT);
        }

        if (operation.equals("draw")) {
            return new SingleJPEGInputBolt(
                    new DrawFeaturesOp(), BOLT_HANDLE_TYPE.BOLT_HANDLE_TYPE_BUFFEREDIMAGE);
        }
        return null;
    }

    public static Boolean isSingleOperation(String operation) {
        for (int i = 0; i < singleOperations.length; i++) {
            if (singleOperations[i].equals(operation)) return true;
        }
        return false;
    }
}
