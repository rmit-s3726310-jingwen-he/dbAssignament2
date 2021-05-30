package bptree;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;


public class MyBPTree {
    static final int NumOfInt = 4;
    static final int NumOfKey = 25;
    /**
     * B+ tree
     */
    public MyBPNode root = new MyBPNode(true, true);;
    /**
     * head in leaf node list of B+ tree
     */
    public MyBPNode head;

    public MyBPTree() {
        head = root;
    }

    public MyBPNode getHead() {
        return head;
    }

    public BasicCell selectKeyInTree(BasicCell searchKey) {
        return this.root.get(searchKey);
    }

    public void insertKey(BasicCell key) {
        this.root.insert(key, this);
    }
    /**
     * Set the storage location corresponding to each node
     * */
    public void setPageIndex(){
        //Start from 0
        int pageIdx =0;
        //Breadth traversal tree
        Queue<MyBPNode> myBPNodes = new LinkedList<>();
        myBPNodes.offer(root);
        root.pageIndex = pageIdx;pageIdx++;
        while(!myBPNodes.isEmpty()){
            int size = myBPNodes.size();
            for (int i = 0; i < size; i++) {
                MyBPNode node = myBPNodes.poll();
                if(node != null &&!node.isLeaf) {
                    for(MyBPNode childNode:node.getChildren()) {
                        myBPNodes.add(childNode);
                        childNode.pageIndex = pageIdx;pageIdx++;
                    }
                }
            }
        }
    }
    /**
     * Save to file by page
     * */
    public boolean saveTree(int pageSize){
        String fileName = "tree"+pageSize;
        RandomAccessFile raf = null;
        //Set the page index stored by each node
        this.setPageIndex();
        try {
            //Open file descriptor
            raf = new RandomAccessFile(fileName, "rw");
            //Breadth traversal storage
            Queue<MyBPNode> myBPNodes = new LinkedList<>();
            myBPNodes.offer(root);
            while(!myBPNodes.isEmpty()){
                int size = myBPNodes.size();
                for (int i = 0; i < size; i++) {
                    MyBPNode node = myBPNodes.poll();
                    assert node != null;//Here node is not null
                    boolean isLeaf = node.isLeaf;//Whether it is a leaf node
                    int keyCnt = node.getBasicCellList().size();//Number of keywords
                    //Build a byte array, the size is the number of keywords + leaf flag + keyword array + corresponding keys
                    byte[] contentBr = new byte[8+keyCnt*NumOfKey+4*(keyCnt+1)];
                    //The location of the address in the next page
                    int nextDesPos =0;
                    //Convert to byte array，存入contentBr中
                    byte[] keyCntBr = ByteBuffer.allocate(NumOfInt).putInt(keyCnt).array();
                    System.arraycopy(keyCntBr,0,contentBr,nextDesPos, keyCntBr.length);nextDesPos+= NumOfInt;

                    byte[] isLeafBr = ByteBuffer.allocate(NumOfInt).putInt(isLeaf ? 1 : 0).array();
                    System.arraycopy(isLeafBr,0,contentBr,nextDesPos, isLeafBr.length);nextDesPos+= NumOfInt;
                    //Start storing keywords
                    for (BasicCell key : node.getBasicCellList()) {
                        byte[] keyBr = ByteBuffer.allocate(NumOfKey).put(key.getValue().toString().getBytes()).array();
                        System.arraycopy(keyBr,0,contentBr,nextDesPos, keyBr.length); nextDesPos+= NumOfKey;
                    }
                    if(!node.isLeaf) {//Non-leaf node save page number
                        for (MyBPNode childNode: node.getChildren()){
                            //Add to the queue
                            myBPNodes.offer(childNode);
                            byte[] pageIdxBr = ByteBuffer.allocate(NumOfInt).putInt(childNode.pageIndex).array();
                            System.arraycopy(pageIdxBr,0,contentBr,nextDesPos, pageIdxBr.length); nextDesPos+= NumOfInt;
                        }
                    }else{
                        //The leaf node stores the physical address
                        for (int j = 0; j < node.getBasicCellList().size(); j++) {
                            byte[] dataPtr = ByteBuffer.allocate(NumOfInt).putInt(node.getBasicCellList().get(j).pAddr).array();
                            System.arraycopy(dataPtr,0,contentBr,nextDesPos, dataPtr.length); nextDesPos+= NumOfInt;
                        }
                    }
                    //Write to file
                    raf.seek(node.pageIndex *pageSize);
                    raf.write(contentBr);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
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
        return true;
    }
    /**
     * Read index from index file
     * */
    public boolean readTree(int pageSize){
        String fileName = "tree"+pageSize;
        RandomAccessFile raf = null;
        //Read the root file first
        MyBPNode tmpRoot = new MyBPNode(false,true);
        try {
            //Open the file descriptor
            raf = new RandomAccessFile(fileName, "rw");
            //Read page 0, fixed field at the beginning
            byte[] pageContent = new byte[pageSize];
            int pagePos =0;//In-page address
            raf.seek(0);
            int read = raf.read(pageContent);
            //Get the number of keywords
            byte[] rootKeyCntBr = new byte[NumOfInt];
            System.arraycopy(pageContent, pagePos, rootKeyCntBr, 0, NumOfInt);pagePos+=NumOfInt;
            int rootKeyCnt = ByteBuffer.wrap(rootKeyCntBr).getInt();
            //Get the leaf flag
            byte[] rootIsLeafBr = new byte[NumOfInt];
            System.arraycopy(pageContent, pagePos, rootIsLeafBr, 0, NumOfInt);pagePos+=NumOfInt;
            tmpRoot.isLeaf = ByteBuffer.wrap(rootIsLeafBr).getInt() != 0;
            //Build a keyword array
            if(tmpRoot.getBasicCellList()==null){
                tmpRoot.basicCellList = new ArrayList<>();
            }
            if(!tmpRoot.isLeaf &&tmpRoot.children == null){
                tmpRoot.children = new ArrayList<>();
            }
            for (int i = 0; i < rootKeyCnt; i++) {
                byte[] keyBr = new byte[NumOfKey];
                System.arraycopy(pageContent, pagePos, keyBr, 0, NumOfKey);pagePos+=NumOfKey;
                KeyWord values = new KeyWord(new String(keyBr).trim());
                tmpRoot.basicCellList.add(new BasicCell(values));
            }

            if(!tmpRoot.isLeaf) {
                tmpRoot.childrenPageIndexs = new ArrayList<>();
                //Read the page number of the child node
                for (int i = 0; i < rootKeyCnt + 1; i++) {
                    byte[] pageIdxBr = new byte[NumOfInt];
                    System.arraycopy(pageContent, pagePos, pageIdxBr, 0, NumOfInt);
                    pagePos += NumOfInt;
                    tmpRoot.childrenPageIndexs.add(ByteBuffer.wrap(pageIdxBr).getInt());
                }
            }else{
                //Leaf node, read the corresponding physical address
                for (int i = 0; i < rootKeyCnt; i++) {
                    byte[] pageIdxBr = new byte[NumOfInt];
                    System.arraycopy(pageContent, pagePos, pageIdxBr, 0, NumOfInt);
                    pagePos += NumOfInt;
                    tmpRoot.basicCellList.get(i).pAddr =  ByteBuffer.wrap(pageIdxBr).getInt();
                }
            }
            root = tmpRoot;//Update the root node
            head = null;
            if(tmpRoot.isLeaf){//If root is a leaf, return directly
                return true;
            }
            Queue<MyBPNode> myBPNodeQueue = new LinkedList<>();
            myBPNodeQueue.offer(tmpRoot);
            while(!myBPNodeQueue.isEmpty()){
                int size = myBPNodeQueue.size();
                //Read the child nodes of each node
                for (int i = 0; i < size; i++) {
                    MyBPNode parentNode = myBPNodeQueue.poll();
                    assert parentNode!=null;
                    if(parentNode.isLeaf){//The leaf node has no child nodes, set the linked list
                        if(this.head == null){
                            this.head = parentNode;
                        }
                        MyBPNode lastNode = parentNode;
                        while(!myBPNodeQueue.isEmpty()){
                            MyBPNode tmpNode = myBPNodeQueue.poll();
                            lastNode.next = tmpNode;
                            tmpNode.previous = lastNode;
                            lastNode = tmpNode;
                        }
                        break;
                    }
                    //Read the child node according to the child node page number
                    for (int j = 0; j < parentNode.childrenPageIndexs.size(); j++) {
                        raf.seek(parentNode.childrenPageIndexs.get(j) * pageSize);//Offset to the correct position
                        pageContent = new byte[pageSize];
                        raf.read(pageContent);
                        pagePos = 0;//Reset the address in the page
                        //Read the number of keywords and leaf flags
                        byte[] keyCntBr = new byte[NumOfInt];
                        System.arraycopy(pageContent, pagePos, keyCntBr, 0, NumOfInt);pagePos+=NumOfInt;
                        int keyCnt = ByteBuffer.wrap(keyCntBr).getInt();
                        //Get the leaf flag
                        byte[] isLeafBr = new byte[NumOfInt];
                        System.arraycopy(pageContent, pagePos, isLeafBr, 0, NumOfInt);pagePos+=NumOfInt;
                        boolean isLeaf = ByteBuffer.wrap(isLeafBr).getInt() != 0;
                        MyBPNode newNode = new MyBPNode(isLeaf,false);
                        for (int k = 0; k < keyCnt; k++) {
                            byte[] keyBr = new byte[NumOfKey];
                            System.arraycopy(pageContent, pagePos, keyBr, 0, NumOfKey);pagePos+=NumOfKey;
                            KeyWord values = new KeyWord(new String(keyBr).trim());
                            newNode.basicCellList.add(new BasicCell(values));
                        }
                        if(!newNode.isLeaf) {
                            newNode.childrenPageIndexs = new ArrayList<>();
                            //Read the page number of the child node
                            for (int k = 0; k < keyCnt + 1; k++) {
                                byte[] pageIdxBr = new byte[NumOfInt];
                                System.arraycopy(pageContent, pagePos, pageIdxBr, 0, NumOfInt);
                                pagePos += NumOfInt;
                                newNode.childrenPageIndexs.add(ByteBuffer.wrap(pageIdxBr).getInt());
                            }
                        }else{//Leaf node leaf node
                            for (int k = 0; k < keyCnt ; k++) {
                                byte[] pageIdxBr = new byte[NumOfInt];
                                System.arraycopy(pageContent, pagePos, pageIdxBr, 0, NumOfInt);
                                pagePos += NumOfInt;
                                newNode.getBasicCellList().get(k).pAddr = ByteBuffer.wrap(pageIdxBr).getInt();
                            }
                        }
                        parentNode.children.add(newNode);
                        newNode.parent = parentNode;
                        myBPNodeQueue.add(newNode);
                    }
                    parentNode.childrenPageIndexs = null;
                }
            }
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
        return true;
    }

}
