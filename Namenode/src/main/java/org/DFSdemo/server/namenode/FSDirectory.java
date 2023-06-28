package org.DFSdemo.server.namenode;

import java.util.HashMap;
import org.DFSdemo.server.datanode.HeartbeatManager;
import org.DFSdemo.server.datanode.HeartbeatMonitor;
import org.DFSdemo.server.datanode.HeartbeatReceiver;

public class FSDirectory {
    //单例设计模式
    private static final FSDirectory fileSystem = new FSDirectory();
    INodeDirectory root;
    HashMap<Long, INode> INodeMap;   //存储id与inode的映射关系
    INodeDirectory currentDirectory;
    HeartbeatReceiver hbReceiver;
    HeartbeatManager hbManager;
    HeartbeatMonitor hbMonitor;

    static final String DEFAULT_USER="admin";
    static final int DEFAULT_DIR_PERMISSION=100;
    static final int DEFAULT_FILE_PERMISSION=111;

    private FSDirectory(){
        root=new INodeDirectory(DEFAULT_USER, DEFAULT_DIR_PERMISSION);
        INodeMap=new HashMap<>();
        currentDirectory=root;
        hbManager=new HeartbeatManager();
        hbMonitor=new HeartbeatMonitor(hbManager);
        hbReceiver=new HeartbeatReceiver(hbManager);
    }
    public static FSDirectory getInstance(){
        return fileSystem;
    }

    String getINodeName(long id){
        INode inode=INodeMap.get(id);
        return inode.getName();
    }

    void receiveHeartbeat(String nodeId){
        hbReceiver.receiveHeartbeat(nodeId);
    }

    boolean monitorHeartbeat(String nodeId, long threshold){
        return hbMonitor.monitorHeartbeat(nodeId, threshold);
    }

    void setCurrentDirectory(INodeDirectory dir){
        currentDirectory=dir;
    }
    INodeDirectory getCurrentDirectory(){
        return currentDirectory;
    }

    HashMap<Long, INode> getINodeMap(){
        return INodeMap;
    }

    String getUserName(){
        return "user";
    }

    String getWorkingPath(){
        return currentDirectory.getFullPathName();
    }

//    INode search(String name){
//        //构造一个同名的临时inode用于搜索
//        INode inode=new INode(currentDirectory, DEFAULT_USER, DEFAULT_DIR_PERMISSION, name);
//        //保存当前目录
//        INodeDirectory currentDir=currentDirectory;
//        int i=currentDirectory.searchChild(inode);
//        if(i>0){
//            return currentDirectory.getChild(i);
//        }else{
//            if(currentDirectory.numOfChildren()>0){
//                for(INode child : currentDirectory.children){
//                    if(child.isDirectory()){
//                        //重设当前目录
//                        setCurrentDirectory((INodeDirectory) child);
//                        INode temp = search(name);
//                        //恢复当前目录
//                        setCurrentDirectory(currentDir);
//                        if(temp!=null) return temp;
//                    }
//                }
//            }
//        }
//        return null;
//    }

    void setPermission(INode inode, int permission){
        inode.setPermission(permission);
    }

    int ifHaveINode(String name){
        //用于搜索的临时变量
        INode inode = new INode(currentDirectory, getUserName(), 1, name);
        return currentDirectory.searchChild(inode);
    }

    void rename(String oldName, String newName){
        int i=ifHaveINode(oldName);
        if(i>0){
            currentDirectory.getChild(i).setName(newName);
        }else{
            //名称已存在
        }
    }

    void addDirectory(String name){
        INodeDirectory dir=new INodeDirectory(currentDirectory, getUserName(), DEFAULT_DIR_PERMISSION, name);
        currentDirectory.addChild(dir);
        INodeMap.put(dir.getId(), dir);
    }
    void removeDirectory(String name){
        int i=ifHaveINode(name);
        INode dir;
        if(i>0){
            dir=currentDirectory.getChild(i);
            INodeMap.remove(dir.getId());
            currentDirectory.removeChild(dir);
        }
    }
    void renameDirectory(String oldName, String newName){
        rename(oldName, newName);
    }

    void addFile(String name){
        INodeFile file=new INodeFile(currentDirectory, DEFAULT_USER, DEFAULT_FILE_PERMISSION, name);
        currentDirectory.addChild(file);
        INodeMap.put(file.getId(), file);
    }
    void removeFile(String name){
        int i=ifHaveINode(name);
        INode file;
        if(i>0){
            file=currentDirectory.getChild(i);
            INodeMap.remove(file.getId());
            currentDirectory.removeChild(file);
        }
    }
    void renameFile(String oldName, String newName){
        rename(oldName, newName);
    }
}
