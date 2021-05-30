import java.io.*;
import java.nio.ByteBuffer;

public class dbload {
    /**
     * load record count
     */
    static int loadRecordeCnt = 0;
    /**
     * file name
     * */
    public static String fileName ="";
    /**
     * page
     * */
    public static byte[] pageArr;
    private static int curPageSize = 0;
    private static int pageSize = 0;
    private static int usePageCnt =0;

    /**
     * write record count
     * */
    private static int recordCnt =0;
    /**
     * arg1: -p
     * arg2:pagesize
     * arg3:datafile
     * */
    public static void main(String[] args) {
        if(args.length < 3){
            System.err.println("arg too less, arg num=" + args.length);
            System.exit(1);
        }
        pageSize = Integer.parseInt(args[1]);
        String dataFile = args[2];
        pageArr = new byte[pageSize];
        //set pageSize
        fileName = "heap." + pageSize;
        // for loading file, use page count
        long beginTime =0;
        long endTime = 0;
        //start read]
        beginTime = System.currentTimeMillis();
        BufferedReader br=null;
        try {
            br =new BufferedReader(new FileReader(dataFile));
            String line;
            //skip first line
            line = br.readLine();
//            System.out.println(line);
            while((line = br.readLine())!=null){
                //deal line transfer to recordEntity
                loadRecordeCnt ++;
                //split line
                String[] cols = line.split(",");

                int id = Integer.parseInt(cols[0]);
                //date time
                int[] dateTimeArr = RecordEntity.dateTimeStrToIntArr(cols[1]);
                int weekDay = RecordEntity.weekStrToNum(cols[5]);
                int sensorID = Integer.parseInt(cols[7]);
                String sensorName = cols[8];
                int hourlyCount = Integer.parseInt(cols[9]);
                //next, transferr to byte arr
                RecordEntity recordEntity = new RecordEntity(id,dateTimeArr[0],dateTimeArr[1],dateTimeArr[2],
                        dateTimeArr[3],dateTimeArr[4],dateTimeArr[5],weekDay,sensorID,sensorName,hourlyCount);
                byte[] recordBuf = recordEntity.transferToByteArr();
                if(loadRecordeCnt % 1000 ==0){
                    System.out.println("\r having load "+loadRecordeCnt+" records");
                    //an early closure
//                    break;
                }
                //add record to page
                addRecordToPage(recordBuf);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(br!=null){
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //last page to write
        writePageToDisk();
        endTime = System.currentTimeMillis();
        System.out.println("\rall load "+loadRecordeCnt+" records");
        System.out.println("create "+ usePageCnt +" pages");
        System.out.println("finish in "+ (endTime - beginTime) + " ms");
    }

    /**
     * add record to page
     * */
    private static void addRecordToPage(byte[] recordBuf) {
        //need to new page
        if(curPageSize + recordBuf.length > pageSize){
            writePageToDisk();
        }
        System.arraycopy(recordBuf,0,pageArr,curPageSize,recordBuf.length);
        curPageSize += recordBuf.length;
    }
    /**
     * write page
     * */
    private static void writePageToDisk(){
        //avoid to write empty page
        if(curPageSize ==0){
            return;
        }
        //if new process, delete old file
        if(usePageCnt == 0){
            File lastFile = new File(fileName);
            boolean delete = lastFile.delete();
        }
        // random write to disk'
        RandomAccessFile  raf = null;
        try {
            raf = new RandomAccessFile(fileName, "rw");
            raf.seek(usePageCnt *pageSize);
            raf.write(pageArr);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(raf !=null){
                try {
                    raf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        // reinit page
        usePageCnt++;
        pageArr = new byte[pageSize];
        curPageSize = 0;
    }
    static class RecordEntity{
        private static int NumByteOfInt =4;
        public int id;
        public int year;
        public int month;
        public int day;
        public int hour;
        public int minute;
        public int second;
        public int weekDay;
        public int sensorID;
        public String sensorName;
        public int hourlyCount;
        public RecordEntity(int id, int year, int month, int day, int hour, int minute, int second, int weekDay, int sensorID, String sensorName, int hourlyCount) {
            this.id = id;
            this.year = year;
            this.month = month;
            this.day = day;
            this.hour = hour;
            this.minute = minute;
            this.second = second;
            this.weekDay = weekDay;
            this.sensorID = sensorID;
            this.sensorName = sensorName;
            this.hourlyCount = hourlyCount;
        }
        /**
         * transfer record object to byte arr
         * */
        public byte[] transferToByteArr(){
            byte[] id;
            byte[] year,month,day,hour,minute,second,weekDay;
            byte[] sensorID;
            byte[] sensorName;
            byte[] hourlyCount;
            byte[] recordLength;
            int recordLen =0;
            id = ByteBuffer.allocate(NumByteOfInt).putInt(this.id).array();
            year = ByteBuffer.allocate(NumByteOfInt).putInt(this.year).array();
            month = ByteBuffer.allocate(NumByteOfInt).putInt(this.month).array();
            day = ByteBuffer.allocate(NumByteOfInt).putInt(this.day).array();
            hour = ByteBuffer.allocate(NumByteOfInt).putInt(this.hour).array();
            minute = ByteBuffer.allocate(NumByteOfInt).putInt(this.minute).array();
            second = ByteBuffer.allocate(NumByteOfInt).putInt(this.second).array();
            weekDay = ByteBuffer.allocate(NumByteOfInt).putInt(this.weekDay).array();
            sensorID = ByteBuffer.allocate(NumByteOfInt).putInt(this.sensorID).array();
            sensorName = this.sensorName.getBytes();
            hourlyCount = ByteBuffer.allocate(NumByteOfInt).putInt(this.hourlyCount).array();
            //cal record
            recordLen = NumByteOfInt * 10 + sensorName.length + NumByteOfInt;
            recordLength = ByteBuffer.allocate(NumByteOfInt).putInt(recordLen).array();
            byte[] recordBuf = new byte[recordLen];
            //copy byte to record buffer
            int nextDesPos =0;
            //put record length into first pos
            System.arraycopy(recordLength,0,recordBuf,nextDesPos, recordLength.length);nextDesPos+= recordLength.length;
            System.arraycopy(id,0,recordBuf,nextDesPos, id.length);nextDesPos+= id.length;
            System.arraycopy(year,0,recordBuf,nextDesPos, year.length);nextDesPos+= year.length;
            System.arraycopy(month,0,recordBuf,nextDesPos, month.length);nextDesPos+= month.length;
            System.arraycopy(day,0,recordBuf,nextDesPos, day.length);nextDesPos+= day.length;
            System.arraycopy(hour,0,recordBuf,nextDesPos, hour.length);nextDesPos+= hour.length;
            System.arraycopy(minute,0,recordBuf,nextDesPos, minute.length);nextDesPos+= minute.length;
            System.arraycopy(second,0,recordBuf,nextDesPos, second.length);nextDesPos+= second.length;
            System.arraycopy(weekDay,0,recordBuf,nextDesPos, weekDay.length);nextDesPos+= weekDay.length;
            System.arraycopy(sensorID,0,recordBuf,nextDesPos, sensorID.length);nextDesPos+= sensorID.length;
            System.arraycopy(hourlyCount,0,recordBuf,nextDesPos, hourlyCount.length);nextDesPos+= hourlyCount.length;
            //put var char into last pos
            System.arraycopy(sensorName,0,recordBuf,nextDesPos, sensorName.length);nextDesPos+= sensorName.length;


            return recordBuf;
        }
        /**
         * 11/01/2019 05:00:00 PM to int[]
         */
        public static int[] dateTimeStrToIntArr(String dateTime){
            //split by blank
            String[] firstArr = dateTime.split(" ");
            String[] dateArr = firstArr[0].split("/");
            String[] timeArr = firstArr[1].split(":");
            // year month day hour minute second
            int[] result = new int[6];
            result[1] = Integer.parseInt(dateArr[0]);
            result[2] = Integer.parseInt(dateArr[1]);
            result[0] = Integer.parseInt(dateArr[2]);
            result[3] = Integer.parseInt(timeArr[0]);
            result[4] = Integer.parseInt(timeArr[1]);
            result[5] = Integer.parseInt(timeArr[2]);
            if(firstArr[2].equals("PM")){
                result[3] += 12;
            }
            return result;
        }
        /**
         * let week str transfer to week num
         * */
        public static int weekStrToNum(String weekStr){
            if(weekStr.equals("Monday")){
                return 1;
            }else if(weekStr.equals("Tuesday")){
                return 2;
            }else if(weekStr.equals("Wednesday")){
                return 3;
            }else if(weekStr.equals("Thursday")){
                return 4;
            }else if(weekStr.equals("Friday")){
                return 5;
            }else if(weekStr.equals("Saturday")){
                return 6;
            }else{
                return 7;
            }
        }
    }
}
