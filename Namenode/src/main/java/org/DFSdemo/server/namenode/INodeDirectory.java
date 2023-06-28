package org.DFSdemo.server.namenode;

import java.util.ArrayList;
import java.util.Collections;

public class INodeDirectory extends INode{
    ArrayList<INode> children;

    INodeDirectory(INodeDirectory parent, String username, int permission, String name) {
        super(parent, username, permission, name);
        children=new ArrayList<>();
    }
    INodeDirectory(String username, int permission){  //用于根节点
        super(username, permission);
        children=new ArrayList<>();
    }

    @Override
    boolean isDirectory() {
         return true;
    }

    int searchChild(INode child){
        return Collections.binarySearch(children, child);
    }

    ArrayList<INode> getChildren(){
        return children;
    }

    INode getChild(int index){
        return children.get(index);
    }

    boolean addChild(INode child){
        int i=searchChild(child);
        if(i>0){
            return false;
        }else{
            children.add(-i, child);
            return true;
        }
    }

    boolean removeChild(INode child){
        int i=searchChild(child);
        if(i<0){
            return false;
        }else {
            children.remove(i);
            return true;
        }
    }

    void clearChildren(){
        children.clear();
    }

    int numOfChildren(){
        return children.size();
    }

    @Override
    void setName(String newName) {
        INode inode=new INode(this, "user", 10, newName);
        int i=searchChild(inode);
        if(i>0){
            //名称已存在
            return;
        }
        super.setName(newName);
    }
}
