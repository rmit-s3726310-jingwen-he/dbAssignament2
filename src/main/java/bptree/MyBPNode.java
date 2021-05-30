package bptree;

import java.util.ArrayList;
import java.util.List;

public class MyBPNode {
    /**
     * leaf node flag
     */
    public boolean isLeaf;
    /**
     * root node flag
     */
    public boolean isRoot;
    /**
     * parent
     */
    public MyBPNode parent;
    /**
     * prev node
     */
    public MyBPNode previous;
    /**
     * next node
     */
    public MyBPNode next;
    /**
     * key-value in node
     */
    public List<BasicCell> basicCellList;
    /**
     * children node point
     */
    public List<MyBPNode> children;
    /**
     * indicate what the node is in the page
     * */
    public List<Integer> childrenPageIndexs;
    /**
     * page index for disk
     * */
    public int pageIndex;

    /**
     * max count of k-v pair
     */
    final int maxLength = 140;

    public void setParent(MyBPNode parent) {
        this.parent = parent;
    }

    public MyBPNode getNext() {
        return next;
    }

    public List<BasicCell> getBasicCellList() {
        return basicCellList;
    }

    public List<MyBPNode> getChildren() {
        return children;
    }

    public MyBPNode(boolean isLeaf) {
        this.isLeaf = isLeaf;

        if (!isLeaf) {
            children = new ArrayList<>();
        }
        basicCellList = new ArrayList<>();
    }

    public MyBPNode(boolean isLeaf, boolean isRoot) {
        this(isLeaf);
        this.isRoot = isRoot;
    }
    /**
     * find by key
     * */
    public BasicCell get(BasicCell key) {
        if (isLeaf) {
            if(!key.isRange) {
                for (BasicCell basicCell : basicCellList) {
                    if (key.compare(basicCell) == 0) {
                        return basicCell;
                    }
                }
                return null;
            }else{
                boolean isNext = false;
                boolean isPrev = false;
                MyBPNode curNode = null;
                MyBPNode curNode0 = null;
                key.resultListForRange = new ArrayList<>();
                //First find the first leftmost matching child node
                for (BasicCell basicCell : basicCellList) {
                    if (basicCell.contain(key)) {//Find the first one, search directly from the leaf node
                        isNext = true;
                        key.resultListForRange.add(basicCell.pAddr);
                    }else{
                        isNext = false;
                    }
                }

                curNode = this.next;
                curNode0 = this.previous;

                //Search backward
                while(isNext && curNode!=null){
                    for (BasicCell basicCell : curNode.basicCellList) {
                        if (basicCell.contain(key)) {
                            isNext = true;
                            key.resultListForRange.add(basicCell.pAddr);
                        }else{
                            isNext = false;
                        }
                    }
                    if(isNext){
                        curNode = curNode.next;
                    }
                }
                //Look forward
                if(curNode0 !=null){
                    isPrev = true;
                }
                while(isPrev && curNode0!=null){
                    for (int i = curNode0.basicCellList.size()-1; i >=0 ; i--) {
                        BasicCell basicCell = curNode0.basicCellList.get(i);
                        if (basicCell.contain(key)) {
                            isPrev = true;
                            key.resultListForRange.add(basicCell.pAddr);
                        }else{
                            isPrev = false;
                        }
                    }
                    if(isPrev){
                        curNode0 = curNode0.previous;
                    }
                }
                return key;
            }
        } else {//Choose which child node to continue searching
            // Less than the first node
            if (key.compare(basicCellList.get(0)) < 0) {
                return children.get(0).get(key);
            } else if (key.compare(basicCellList.get(basicCellList.size() - 1)) >= 0) {//Greater than or equal to the last node
                return children.get(children.size() - 1).get(key);
            } else {
                for (int i = 0; i < (basicCellList.size() - 1); i++) {
                    if (key.compare(basicCellList.get(i)) >= 0 && key.compare(basicCellList.get(i + 1)) < 0) {
                        return children.get(i + 1).get(key);
                    }
                }
            }
        }
        return null;
    }
    /**
     * Insert new kv pair
     * */
    public void insert(BasicCell key, MyBPTree tree) {
        if (isLeaf) {
            if (!isLeafToSplit()) {
                // Leaf nodes do not need to split
                insertInLeaf(key);
            } else {
                //Need to split into left and right nodes
                MyBPNode left = new MyBPNode(true);
                MyBPNode right = new MyBPNode(true);
                if (previous != null) {
                    left.previous = previous;
                    previous.next = left;
                } else {
                    tree.head = left;
                }
                if (next != null) {
                    right.next = next;
                    next.previous = right;
                }
                left.next = right;
                right.previous = left;
                // for GC
                previous = null;
                next = null;
                // Split after insertion
                insertInLeaf(key);
                int leftSize = getUpper(basicCellList.size(), 2);
                int rightSize = basicCellList.size() - leftSize;
                // Left and right node copy
                for (int i = 0; i < leftSize; i++) {
                    left.basicCellList.add(basicCellList.get(i));
                }
                for (int i = 0; i < rightSize; i++) {
                    right.basicCellList.add(basicCellList.get(leftSize + i));
                }
                // Not the root node
                if (!isRoot) {
                    // Adjust the relationship between parent and child nodes
                    // Find the position of the current node in the parent node
                    int index = parent.children.indexOf(this);
                    // Delete current pointer
                    parent.children.remove(this);
                    left.setParent(parent);
                    right.setParent(parent);
                    // Add the pointer of the split node to the parent node
                    parent.children.add(index, left);
                    parent.children.add(index + 1, right);
                    // for GC
                    basicCellList = null;
                    children = null;
                    // Insert keywords in parent node [non-leaf node]
                    parent.insertInParent(right.basicCellList.get(0));
                    parent.updateNode(tree);
                    // for GC
                    parent = null;
                } else {// this node is the root node
                    //Regenerate the root node
                    isRoot = false;
                    MyBPNode rootNode = new MyBPNode(false, true);
                    tree.root = rootNode;
                    left.parent = rootNode;
                    right.parent = rootNode;
                    rootNode.children.add(left);
                    rootNode.children.add(right);
                    // for GC
                    basicCellList = null;
                    children = null;
                    // insert keyword at root node
                    rootNode.insertInParent(right.basicCellList.get(0));
                }
            }
        } else {
            // if it is not a leaf node search down the pointer
            if (key.compare(basicCellList.get(0)) < 0) {
                children.get(0).insert(key, tree);
            } else if (key.compare(basicCellList.get(basicCellList.size() - 1)) >= 0) {
                children.get(children.size() - 1).insert(key, tree);
            } else {
                // traversal comparison
                for (int i = 0; i < (basicCellList.size() - 1); i++) {
                    if (key.compare(basicCellList.get(i)) >= 0 && key.compare(basicCellList.get(i + 1)) < 0) {
                        children.get(i + 1).insert(key, tree);
                        break;
                    }
                }
            }
        }
    }
    /**
     * round up
     */
    private int getUpper(int x, int y) {
        if (y == 2) {
            int remainder = x & 1;
            if (remainder == 0) {
                return x >> 1;
            } else {
                return (x >> 1) + 1;
            }
        } else {
            int remainder = x % y;
            if (remainder == 0) {
                return x / y;
            } else {
                return x / y + 1;
            }
        }
    }
    /**
     * After inserting the keyword in the non-leaf node, check whether it needs to be split
     */
    private void updateNode(MyBPTree tree) {
        // Need to split
        if (isNodeToSplit()) {
            MyBPNode left = new MyBPNode(false);
            MyBPNode right = new MyBPNode(false);
            int pLeftSize = getUpper(children.size(), 2);
            int pRightSize = children.size() - pLeftSize;
            // Keyword promoted to parent node
            BasicCell keyToParent = basicCellList.get(pLeftSize - 1);
            // Copy the keyword on the left
            for (int i = 0; i < (pLeftSize - 1); i++) {
                left.basicCellList.add(basicCellList.get(i));
            }
            // Copy the pointer on the left
            for (int i = 0; i < pLeftSize; i++) {
                left.children.add(children.get(i));
                left.children.get(i).setParent(left);
            }
            // Copy the keyword on the right, and the first keyword on the right is promoted to the parent node
            for (int i = 0;  i < (pRightSize - 1); i++) {
                right.basicCellList.add(basicCellList.get(pLeftSize + i));
            }
            // Copy the pointer on the right
            for (int i = 0; i < pRightSize; i++) {
                right.children.add(children.get(pLeftSize + i));
                right.children.get(i).setParent(right);
            }
            if (!isRoot) {//The parent node of the non-leaf node inserts the key
                int index = parent.children.indexOf(this);
                parent.children.remove(index);
                left.parent = parent;
                right.parent = parent;
                parent.children.add(index, left);
                parent.children.add(index + 1, right);
                // Insert keywords
                parent.basicCellList.add(index, keyToParent);
                parent.updateNode(tree);
                basicCellList.clear();
                children.clear();
                basicCellList = null;
                children = null;
                parent = null;
            } else {
                // Is the root node
                isRoot = false;
                MyBPNode rootNode = new MyBPNode(false, true);
                tree.root = rootNode;
                left.parent = rootNode;
                right.parent = rootNode;
                rootNode.children.add(left);
                rootNode.children.add(right);
                children.clear();
                basicCellList.clear();
                children = null;
                basicCellList = null;
                // Insert keywords
                rootNode.basicCellList.add(keyToParent);
            }
        }
    }

    /**
     * Whether the leaf node needs to be split,  is used to judge before inserting
     */
    private boolean isLeafToSplit() {
        if (basicCellList.size() >= (maxLength - 1)) {
            return true;
        }
        return false;

    }

    /**
     * Whether the intermediate node needs to be split, pointers and keywords have been inserted
     */
    private boolean isNodeToSplit() {
        // Since the keyword is inserted first, there is no need for [=]
        if (children.size() > maxLength) {
            return true;
        }
        return false;
    }


    /**
     * Insert into the current leaf node without splitting
     */
    private void insertInLeaf(BasicCell key) {
        if (!isLeaf) {
            throw new UnsupportedOperationException("can't insert into middle node.");
        }
        insertImpl(key);
    }

    /**
     * insert into non leaf nodes without splitting
     */
    private void insertInParent(BasicCell key) {
        if (isLeaf) {
            throw new UnsupportedOperationException("can't insert into leaf node.");
        }
        insertImpl(key);
    }
    private void insertImpl(BasicCell key) {
        //Traversal insert
        for (int i = 0; i < basicCellList.size(); i++) {
            if (basicCellList.get(i).compare(key) == 0) {
                // If the key value already exists, do not insert
                return;
            } else if (basicCellList.get(i).compare(key) > 0) {
                basicCellList.add(i, key);
                return;
            }
        }
        // Insert to the end
        basicCellList.add(key);
    }



}
