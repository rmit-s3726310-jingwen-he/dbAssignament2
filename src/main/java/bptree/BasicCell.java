package bptree;

import java.util.List;


public class BasicCell {
    /**
     * range search flag
     * */
    public boolean isRange;
    /**
     * for saving range searching results
     * */
    public List<Integer> resultListForRange;
    /**key word*/
    public KeyWord value;
    /**address of record*/
    public int pAddr;

    public BasicCell(KeyWord keyWord) {
        this.value = keyWord;
    }

    public KeyWord getValue() {
        return value;
    }

    /**
     * compare value
     */
    public int compare(BasicCell basicCell) {
        return this.value.compare(basicCell.value);
    }
    /**
     * for range find
     * */
    public boolean contain(BasicCell basicCell){
        String thisValue = (this.value).toString();
        String compareValue = basicCell.value.toString();
        int idx = thisValue.indexOf(compareValue);
        return idx==0;
    }

}
