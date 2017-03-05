package edu.fudan.stormcv.bolt;


import com.itextpdf.text.*;
import com.itextpdf.text.List;
import com.itextpdf.text.pdf.CMYKColor;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;

import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import edu.fudan.stormcv.util.connector.ConnectorHolder;
import edu.fudan.stormcv.util.connector.FileConnector;
import edu.fudan.stormcv.util.connector.LocalFileConnector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;

import java.awt.Dimension;
import java.io.File;
import java.io.FileOutputStream;
import java.util.*;

public class StatCollectorBolt extends CVParticleBolt {

    private FrameSerializer serializer = new FrameSerializer();
    private Map<String, String> streamToOutputLocation;
    private Map<String, Integer> streamTotalTaskSize;
    private Map<String, Integer> streamReceivedTaskSize;
    private Map<String, Long> streamTotalProcessTime;
    private Map<String, Long> streamStartTime;
    private Map<String, Long> streamEndTime;
    private Map<String, java.util.List<BaseProcessInfo>> streamToImageProcessInfo;
    private Map<String, Set<String>> streamTask;
    private static final String OutPutFormat = "pdf";
    private ConnectorHolder connectorHolder;

    @Override
    void prepare(Map stormConf, TopologyContext context) {
        this.streamTotalTaskSize = new HashMap<>();
        this.streamReceivedTaskSize = new HashMap<>();
        this.streamToImageProcessInfo = new HashMap<>();
        this.streamStartTime = new HashMap<>();
        this.streamEndTime = new HashMap<>();
        this.streamTotalProcessTime = new HashMap<>();
        this.streamTask = new HashMap<>();
        connectorHolder = new ConnectorHolder(stormConf);
        this.streamToOutputLocation = new HashMap<>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    java.util.List<? extends CVParticle> execute(CVParticle input) throws Exception {
        java.util.List<Frame> result = new ArrayList<>();
        Frame frame = (Frame) input;
        String streamId = frame.getStreamId();
        if (this.streamTotalTaskSize.get(streamId) == null) {
            int totalTaskSize = (int) frame.getMetadata().get("totalTaskSize");
            this.streamTotalTaskSize.put(streamId, totalTaskSize);
            this.streamToImageProcessInfo.put(streamId, new ArrayList<BaseProcessInfo>());
            this.streamStartTime.put(streamId, Long.MAX_VALUE);
            this.streamEndTime.put(streamId, Long.MIN_VALUE);
            this.streamTotalProcessTime.put(streamId, 0L);
            this.streamTask.put(streamId, new HashSet<String>());
        }

        if (this.streamReceivedTaskSize.get(streamId) == null) {
            this.streamReceivedTaskSize.put(streamId, 1);
        } else {
            this.streamReceivedTaskSize.put(streamId, this.streamReceivedTaskSize.get(streamId) + 1);
        }

        BaseProcessInfo info = parseBaseInfo(frame);
        this.streamToImageProcessInfo.get(streamId).add(info);
        this.streamTask.get(streamId).add(info.getBoltTaskIndex());
        if (info.getEmitTime() < this.streamStartTime.get(streamId)) {
            this.streamStartTime.put(streamId, info.getEmitTime());
        }
        if (info.getEndTime() > this.streamEndTime.get(streamId)) {
            this.streamEndTime.put(streamId, info.getEndTime());
        }

        this.streamTotalProcessTime.put(streamId, streamTotalProcessTime.get(streamId) + info.getProcessime());

        if (this.streamTotalTaskSize.get(streamId) == this.streamReceivedTaskSize.get(streamId)) {
            logger.info("stream {} has finished the image processing, start to generate the process report", streamId);
            generateReport(streamId);
            cleanStream(streamId);
        }

        if (this.streamToOutputLocation.get(streamId) == null) {
            String reportLocation = (String) input.getMetadata().get("outputLocation");
            reportLocation = reportLocation.endsWith("/") ? reportLocation : (reportLocation + "/");
            streamToOutputLocation.put(streamId, reportLocation);
        }
        return result;
    }

    private BaseProcessInfo parseBaseInfo(Frame frame) {
        String location = (String) frame.getMetadata().get("location");
        long processTime = (long) frame.getMetadata().get("processTime");
        long emitTime = (long) frame.getMetadata().get("emitTime");
        long endTime = (long) frame.getMetadata().get("endTime");
        String workerSlot = (String) frame.getMetadata().get("workerSlot");
        String boltTaskIndex = String.valueOf(frame.getMetadata().get("boltTaskIndex"));
        return new BaseProcessInfo(location, frame.getBoundingBox().getSize(),
                emitTime, processTime, endTime, workerSlot, boltTaskIndex);
    }


    private void generateReport(String streamId) throws Exception {
        String outPutFileName = streamToOutputLocation.get(streamId) + streamId + "_" + UUID.randomUUID() + "." + OutPutFormat;
        FileConnector connector = connectorHolder.getConnector(outPutFileName);
        connector.moveTo(outPutFileName);

        //check output location
        boolean isLocal = false;
        FileOutputStream outputStream;
        File tmpImage = null;

        if (connector instanceof LocalFileConnector) {
            File file = connector.getAsFile();
            if (!file.exists()) {
                file.createNewFile();
            }
            outputStream = new FileOutputStream(file.getAbsoluteFile());
            isLocal = true;
        } else {
            tmpImage = File.createTempFile("" + outPutFileName.hashCode(), "." + OutPutFormat);
            outputStream = new FileOutputStream(tmpImage);
        }

        //write pdf file
        Document document = new Document(PageSize.A4, 50, 50, 50, 50);
        PdfWriter writer = PdfWriter.getInstance(document, outputStream);
        document.open();

        document.addAuthor("storm-lab369");
        document.addCreator("stormcv");
        document.addSubject("Process Report");

        Paragraph title = new Paragraph("Process Report for " + streamId,
                FontFactory.getFont(FontFactory.HELVETICA, 20, Font.BOLDITALIC, new CMYKColor(255, 255, 255, 0)));
        title.setAlignment(Element.ALIGN_CENTER);
        document.add(title);

        Paragraph title1 = new Paragraph("Image Processing Report",
                FontFactory.getFont(FontFactory.HELVETICA, 18, Font.BOLDITALIC, new CMYKColor(0, 255, 255, 17)));
        Chapter chapter1 = new Chapter(title1, 1);
        chapter1.setNumberDepth(0);
        chapter1.setTriggerNewPage(false);
        document.add(chapter1);

        com.itextpdf.text.List wholeInfoList = new com.itextpdf.text.List(false, false, 15);
        wholeInfoList.add(new ListItem("Total Processed Image Number: " + streamTotalTaskSize.get(streamId)));
        wholeInfoList.add(new ListItem("Average Process Time: " + (streamTotalProcessTime.get(streamId) / streamTotalTaskSize.get(streamId)) + "ms"));
        wholeInfoList.add(new ListItem("Used Bolt Task Number: " + streamTask.get(streamId).size()));
        wholeInfoList.add(new ListItem("Total Process Time: " + (streamEndTime.get(streamId) - streamStartTime.get(streamId)) + "ms"));
        document.add(wholeInfoList);

        Paragraph title2 = new Paragraph("Image Processing Details",
                FontFactory.getFont(FontFactory.HELVETICA, 18, Font.BOLDITALIC, new CMYKColor(0, 255, 255, 17)));
        Chapter chapter2 = new Chapter(title2, 1);
        chapter2.setNumberDepth(0);
        chapter2.setTriggerNewPage(false);
        document.add(chapter2);

        PdfPTable imageDetailtTble = new PdfPTable(5);
        imageDetailtTble.setSpacingBefore(25);
        imageDetailtTble.setSpacingAfter(25);

        Font tableHeadFont = FontFactory.getFont(FontFactory.defaultEncoding, 13, Font.BOLD, new CMYKColor(255, 255, 255, 0));

        PdfPCell locationCell = new PdfPCell(new Phrase(10, new Chunk("Location", tableHeadFont)));
        imageDetailtTble.addCell(locationCell);
        PdfPCell sizeCell = new PdfPCell(new Phrase(10, new Chunk("Size", tableHeadFont)));
        imageDetailtTble.addCell(sizeCell);
        PdfPCell timeCell = new PdfPCell(new Phrase(10, new Chunk("ProcessTime(ms)", tableHeadFont)));
        imageDetailtTble.addCell(timeCell);
        PdfPCell workerSlotCell = new PdfPCell(new Phrase(10, new Chunk("WorkerSlot", tableHeadFont)));
        imageDetailtTble.addCell(workerSlotCell);
        PdfPCell taskIndexCell = new PdfPCell(new Phrase(10, new Chunk("TaskIndex", tableHeadFont)));
        imageDetailtTble.addCell(taskIndexCell);

        java.util.List<BaseProcessInfo> allProcessInfo = streamToImageProcessInfo.get(streamId);
        for (BaseProcessInfo info : allProcessInfo) {
            imageDetailtTble.addCell(info.getLocation());
            imageDetailtTble.addCell(info.getSize().getWidth() + "x" + info.getSize().getHeight());
            imageDetailtTble.addCell(String.valueOf(info.getProcessime()));
            imageDetailtTble.addCell(info.getWorkerSlot());
            imageDetailtTble.addCell(info.getBoltTaskIndex());
        }

        document.add(imageDetailtTble);
        document.close();
        writer.close();

        if (!isLocal) {
            connector.copyFile(tmpImage, true);
        }

        logger.info("Successfully created the report pdf file to {}", outPutFileName);
    }

    private void cleanStream(String streamId) {
        this.streamTotalTaskSize.remove(streamId);
        this.streamReceivedTaskSize.remove(streamId);
        this.streamToImageProcessInfo.remove(streamId);
        this.streamEndTime.remove(streamId);
        this.streamStartTime.remove(streamId);
        this.streamTotalProcessTime.remove(streamId);
        this.streamTask.remove(streamId);
        this.streamToOutputLocation.remove(streamId);
    }

    private class BaseProcessInfo {
        private String location;
        private Dimension size;
        private long emitTime;
        private long processime;
        private long endTime;
        private String workerSlot;
        private String boltTaskIndex;

        BaseProcessInfo(String location, Dimension size, long emitTime, long processime, long endTime,
                        String workerSlot, String boltTaskIndex) {
            this.location = location;
            this.size = size;
            this.emitTime = emitTime;
            this.processime = processime;
            this.endTime = endTime;
            this.workerSlot = workerSlot;
            this.boltTaskIndex = boltTaskIndex;
        }

        public String getLocation() {
            return location;
        }

        public Dimension getSize() {
            return size;
        }

        public long getEmitTime() {
            return emitTime;
        }

        public long getProcessime() {
            return processime;
        }

        public long getEndTime() {
            return endTime;
        }

        public String getWorkerSlot() {
            return workerSlot;
        }

        public String getBoltTaskIndex() {
            return boltTaskIndex;
        }
    }
}
