import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class dbquery {
    /**
     * SDT_NAME
     */
    public static String SDTNAME;
    /**
     * binary fileName
     */
    public static String fileName;
    /**
     * size of a page
     */
    public static int pageSize = 0;
    /**
     * match count
     */
    public static int matchCnt = 0;

    public static void main(String[] args) {
        long beginTime = 0;
        long endTime = 0;
        //check arguments
        //arg length is not solid because the input has space
//        if(args.length != 2){
//            System.out.println("arg count is not avoid");
//            System.exit(1);
//        }
        //get search key
//        SDTNAME = args[0];

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < args.length - 1; i++) {
            sb.append(args[i]).append(" ");
        }
        sb.deleteCharAt(sb.length() - 1);
        //get search key
        SDTNAME = sb.toString();
        pageSize = Integer.parseInt(args[args.length - 1]);

        fileName = "heap." + pageSize;
        File searchFile = null;
        searchFile = new File(fileName);
        if (!searchFile.exists() || !searchFile.isFile()) {
            System.err.println(fileName + "is not vaild");
            System.exit(1);
        }
        //start search
        beginTime = System.currentTimeMillis();
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
                    findInOnePage(pageContext);
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
        endTime = System.currentTimeMillis();
        System.out.println("matchCount: " + matchCnt);
        System.out.println("finish in " + (endTime - beginTime) + " ms");
    }

    static int searchRecordCnt = 0;

    /**
     * find record in one page
     */
    public static void findInOnePage(byte[] pageContext) {
        int idx = 0;//in page, index for context
        //per record length
        int recordLen = 0;
        //save per record len
        byte[] recordLenBytes = new byte[NumByteOfInt];
        //foreach record
        while (idx + 3 < pageSize) {
            //1. read record length
            try {
                System.arraycopy(pageContext, idx, recordLenBytes, 0, NumByteOfInt);
            } catch (Exception e) {
                e.printStackTrace();
            }
            idx += NumByteOfInt;
            //transferr recordLen
            recordLen = ByteBuffer.wrap(recordLenBytes).getInt();
            if (recordLen <= 0) {
                //there is no context in next pos
                return;
            }
            searchRecordCnt++;
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
            //match SDT_Name
            if (recordEntity.isMatchBySearchText(SDTNAME)) {
                System.out.println(recordEntity);
                matchCnt++;
            }
        }
    }

    static int NumByteOfInt = 4;

    static class RecordEntity {
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

        private String dateTimeNumToStr() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%02d", this.month)).append('/');
            sb.append(String.format("%02d", this.day)).append('/');
            sb.append(String.format("%02d", this.year)).append(' ');
            boolean isPM = this.hour > 12;
            if (isPM) {
                sb.append(String.format("%02d", this.hour - 12));
            } else {
                sb.append(String.format("%02d", this.hour));
            }
            sb.append(':').append(String.format("%02d", this.minute));
            sb.append(':').append(String.format("%02d", this.second));
            sb.append(' ').append(isPM ? "PM" : "AM");
            return sb.toString();
        }

        public boolean isMatchBySearchText(String text) {
            String matchStr = sensorID + "_"+dateTimeNumToStr();
//            System.out.println(matchStr);
            return matchStr.contains(text);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(id).append(',');
            sb.append(dateTimeNumToStr()).append(',');
            sb.append(this.year).append(',');
            sb.append(monthNumToStr()).append(',');
            sb.append(this.day).append(',');
            sb.append(weekNumToStr()).append(',');
            sb.append(this.hour).append(',');
            sb.append(this.sensorID).append(',');
            sb.append(this.sensorName).append(',');
            sb.append(this.hourlyCount);
            return sb.toString();
        }

        public String monthNumToStr() {
            switch (this.month) {
                case 1:
                    return "January";
                case 2:
                    return "February";
                case 3:
                    return "March";
                case 4:
                    return "April";
                case 5:
                    return "May";
                case 6:
                    return "June";
                case 7:
                    return "July";
                case 8:
                    return "August";
                case 9:
                    return "September";
                case 10:
                    return "October";
                case 11:
                    return "November";
                case 12:
                    return "December";
            }
            return "no month";
        }

        public String weekNumToStr() {
            switch (this.weekDay) {
                case 1:
                    return "Monday";
                case 2:
                    return "Tuesday";
                case 3:
                    return "Wednesday";
                case 4:
                    return "Thursday";
                case 5:
                    return "Friday";
                case 6:
                    return "Saturday";
                case 7:
                    return "Sunday";
            }
            return "no week";
        }
    }
}
