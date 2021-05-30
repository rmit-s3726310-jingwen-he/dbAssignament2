package bptree;

/**
 * in tree Node,key word
 * */
public class KeyWord {
    private String value;
    public KeyWord(String value) {
        this.value = value;
    }
    public int compare(KeyWord keyWord) {
        int compareResult = value.compareTo(keyWord.getValue());
        if (compareResult == 0) {
            return 0;
        } else if (compareResult < 0) {
            if(keyWord.getValue().indexOf(value) ==0){
                return 0;
            }
            return -1;
        } else {
            return 1;
        }
    }
    public String getValue() {
        return value;
    }
    public String toString() {
        return value;
    }

}
