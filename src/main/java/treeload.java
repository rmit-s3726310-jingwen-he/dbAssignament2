import bptree.MyBPTree;
import index.BuildIndex;

public class treeload {
    public static void main(String[] args) {
        int pageSize = Integer.parseInt(args[0]);
        MyBPTree myBPTree = new MyBPTree();
        long bt = System.currentTimeMillis();
        BuildIndex.buildIndex(pageSize,myBPTree);
        long et = System.currentTimeMillis();
        System.out.println("save tree need:"+(et-bt)+"ms");
    }
}
