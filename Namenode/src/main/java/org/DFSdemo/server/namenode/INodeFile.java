package org.DFSdemo.server.namenode;

import java.util.Vector;

public class INodeFile extends INode{
    long header;
    Vector<BlockInfo> blocks;
    INodeFile(INodeDirectory parent, String username, int permission, String name) {
        super(parent, username, permission, name);
    }

    @Override
    boolean isFile() {
        return true;
    }

    long getHeader(){
        return header;
    }
    void setHeader(long header){
        this.header=header;
    }

    Vector<BlockInfo> getBlocks(){
        return blocks;
    }

    int numOfBlocks(){
        return blocks.size();
    }
}
