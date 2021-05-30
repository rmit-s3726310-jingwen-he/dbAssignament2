package index;

import bptree.MyBPNode;
import bptree.MyBPTree;
import bptree.BasicCell;
import bptree.KeyWord;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * build index
 * */
public class BuildIndex {
    /**
     * create tuple
     * */
    private static BasicCell createTuple(String key, int value) {
        KeyWord values = new KeyWord(key);
        BasicCell basicCell = new BasicCell(values);
        basicCell.pAddr = value;
        return basicCell;
    }
    public static BasicCell createTuple(String key) {
        KeyWord values = new KeyWord(key);
        return new BasicCell(values);
    }
    /**
     * for each record, and build index
     * */
    public static void buildIndex(int pageSize, MyBPTree myBPTree){
        String fileName = "heap." + pageSize;
        File searchFile = null;
        searchFile = new File(fileName);
        if (!searchFile.exists() || !searchFile.isFile()) {
            System.err.println(fileName + "is not vaild");
            System.exit(1);
        }
        //start search
        FileInputStream iso = null;
        try {
            //open file read stream
            iso = new FileInputStream(fileName);
            //calculate count of total page
            int pageCnt = iso.available() / pageSize;
            // for each per page
            for (int pageIdx = 0; pageIdx < pageCnt; pageIdx++) {
                byte[] pageContext = new byte[pageSize];
                int readLen = iso.read(pageContext);
                if (readLen != pageSize) {
                    System.err.println("read context length is equal to pageSize");
                }
                //deal perPage
                try {
                    findInOnePage(pageContext,pageSize,pageIdx, myBPTree);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (iso != null) {
                    iso.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //finish building b+tree, then storange to disk
//        boolean isValid = myBPTree.validate();
//        System.out.println("isValid:" + isValid);
        //check leaf node count
        int leafCnt = 0;
        MyBPNode head = myBPTree.getHead();
        while(head!=null){
            leafCnt+=head.getBasicCellList().size();
            head = head.getNext();
        }
        System.out.println("leaf ="+leafCnt);
        myBPTree.saveTree(pageSize);
    }
    /**
     * find record in one page
     */
    public static void findInOnePage(byte[] pageContext, int pageSize, int pageNum, MyBPTree myBPTree) {
        int idx = 0;//in page, index for context
        //foreach record
        while (idx + 3 < pageSize) {
            //1. read record length
            int recordPageAddr = pageNum*pageSize + idx;//record address
            byte[] recordLenBytes = new byte[NumByteOfInt];
            try {
                System.arraycopy(pageContext, idx, recordLenBytes, 0, NumByteOfInt);
            } catch (Exception e) {
                e.printStackTrace();
            }
            idx += NumByteOfInt;
            //transferr recordLen
            int recordLen = ByteBuffer.wrap(recordLenBytes).getInt();
            if (recordLen <= 0) {
                //there is no context in next pos
                return;
            }
            int id, year, month, day, hour, minute, second, weekDay, sensorId, hourlyCount;
            String sensorName;
            byte[] readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(pageContext, idx, readIntBytes, 0, NumByteOfInt);
            idx += NumByteOfInt;
            id = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(pageContext, idx, readIntBytes, 0, NumByteOfInt);
            idx += NumByteOfInt;
            year = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(pageContext, idx, readIntBytes, 0, NumByteOfInt);
            idx += NumByteOfInt;
            month = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(pageContext, idx, readIntBytes, 0, NumByteOfInt);
            idx += NumByteOfInt;
            day = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(pageContext, idx, readIntBytes, 0, NumByteOfInt);
            idx += NumByteOfInt;
            hour = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(pageContext, idx, readIntBytes, 0, NumByteOfInt);
            idx += NumByteOfInt;
            minute = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(pageContext, idx, readIntBytes, 0, NumByteOfInt);
            idx += NumByteOfInt;
            second = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(pageContext, idx, readIntBytes, 0, NumByteOfInt);
            idx += NumByteOfInt;
            weekDay = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(pageContext, idx, readIntBytes, 0, NumByteOfInt);
            idx += NumByteOfInt;
            sensorId = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(pageContext, idx, readIntBytes, 0, NumByteOfInt);
            idx += NumByteOfInt;
            hourlyCount = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            int remainingLen = recordLen - NumByteOfInt * 11;
            byte[] readStrBytes = new byte[remainingLen];
            System.arraycopy(pageContext, idx, readStrBytes, 0, remainingLen);
            idx += remainingLen;
            sensorName = new String(readStrBytes);
            RecordEntity recordEntity = new RecordEntity(id, year, month, day, hour, minute, second, weekDay,
                    sensorId, sensorName, hourlyCount);
            //insert search text into tree
            BasicCell basicCell = createTuple(recordEntity.sensorID+"_" + recordEntity.dateTimeNumToStr(), recordPageAddr);
            myBPTree.insertKey(basicCell);
        }
    }

    static int NumByteOfInt = 4;
    /**
     * get record by address
     * */
    public static String getDataByFilePos(int dataPos,int pageSize){
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile("heap."+pageSize, "rw");
            raf.seek(dataPos);
            byte[] recordLenBr = new byte[NumByteOfInt];
            raf.read(recordLenBr);

            int recordLen = ByteBuffer.wrap(recordLenBr).getInt();
            byte[] recordBr = new byte[recordLen];
            raf.seek(dataPos);raf.read(recordBr);//读取完整的记录
            int rPos = NumByteOfInt;//指向真正的数据区，跳过recordLen
            int id, year, month, day, hour, minute, second, weekDay, sensorId, hourlyCount;
            String sensorName;
            byte[] readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(recordBr, rPos, readIntBytes, 0, NumByteOfInt);
            rPos += NumByteOfInt;
            id = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(recordBr, rPos, readIntBytes, 0, NumByteOfInt);
            rPos += NumByteOfInt;
            year = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(recordBr, rPos, readIntBytes, 0, NumByteOfInt);
            rPos += NumByteOfInt;
            month = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(recordBr, rPos, readIntBytes, 0, NumByteOfInt);
            rPos += NumByteOfInt;
            day = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(recordBr, rPos, readIntBytes, 0, NumByteOfInt);
            rPos += NumByteOfInt;
            hour = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(recordBr, rPos, readIntBytes, 0, NumByteOfInt);
            rPos += NumByteOfInt;
            minute = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(recordBr, rPos, readIntBytes, 0, NumByteOfInt);
            rPos += NumByteOfInt;
            second = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(recordBr, rPos, readIntBytes, 0, NumByteOfInt);
            rPos += NumByteOfInt;
            weekDay = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(recordBr, rPos, readIntBytes, 0, NumByteOfInt);
            rPos += NumByteOfInt;
            sensorId = ByteBuffer.wrap(readIntBytes).getInt();
            readIntBytes = new byte[NumByteOfInt];
            System.arraycopy(recordBr, rPos, readIntBytes, 0, NumByteOfInt);
            rPos += NumByteOfInt;
            hourlyCount = ByteBuffer.wrap(readIntBytes).getInt();
            int remainingLen = recordLen - NumByteOfInt * 11;
            byte[] readStrBytes = new byte[remainingLen];
            System.arraycopy(recordBr, rPos, readStrBytes, 0, remainingLen);
            sensorName = new String(readStrBytes);
            RecordEntity recordEntity = new RecordEntity(id, year, month, day, hour, minute, second, weekDay,
                    sensorId, sensorName, hourlyCount);
            return recordEntity.toString();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if(raf !=null){
                try {
                    raf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * find record by index
     * */
    public static List<String> findRecordByIndex(String searchText, int pageSize){
        MyBPTree tree = new MyBPTree();
        long bt = System.currentTimeMillis();
        tree.readTree(pageSize);
        long et = System.currentTimeMillis();
        System.out.println("read b+ tree index need "+(et-bt)+"ms");
        BasicCell searchBasicCell = BuildIndex.createTuple(searchText);
        //judge whether range find
        if(searchText.length()< 24){
            searchBasicCell.isRange = true;
        }else if(searchText.length() == 24){
            if(searchText.split("_")[1].length()< 22){
                searchBasicCell.isRange = true;
            }
        }
        BasicCell basicCell = tree.selectKeyInTree(searchBasicCell);
        List<String> results =new ArrayList<>();
        //output result
        if(!searchBasicCell.isRange) {
            results.add(getDataByFilePos(basicCell.pAddr, pageSize));
        }else{
            for (Integer pos: basicCell.resultListForRange){
                results.add(getDataByFilePos(pos, pageSize));
            }
        }
        return results;
    }
}
