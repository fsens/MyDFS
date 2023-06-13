package org.DFSdemo.server.namenode;

import java.util.Date;

public class INode implements Comparable<INode>{
    private INodeDirectory parent;  //父节点
    private String username;    //所属用户名
    private int permission; //文件权限信息
    private Date modificationTime;  //上次修改时间
    private Date accessTime;   //上次访问时间
    private String name;    //文件名
    private String fullPathName;  //绝对路径名
    private long id;      //系统中的唯一标识

    INode(INodeDirectory parent, String username, int permission, String name) {
        this.parent = parent;
        this.username = username;
        this.permission = permission;
        this.name = name;
        modificationTime = new Date();
        accessTime = new Date();
        //id=getINodeID();
        fullPathName = constructPath(this.parent);
    }
    INode(String username, int permission){  //用于根节点
        this.parent=null;
        this.username=username;
        this.permission=permission;
        this.name="/";
        modificationTime=new Date();
        accessTime=new Date();
        //id=getINodeID();
        fullPathName="/";
    }

    boolean isRoot(){
        return fullPathName.length() == 1;
    }
    boolean isDirectory() {
        return false;
    }
    boolean isFile(){
        return false;
    }

    String constructPath(INodeDirectory parent){  //用于构建路径名，需要递归
        String path;
        if(isRoot()){
            return fullPathName;
        }else {
            path = parent.constructPath(parent.getParent());
            return path + name;
        }
    }

    INodeDirectory getParent(){
        return parent;
    }
    void setParent(INodeDirectory newParent){
        parent=newParent;
        fullPathName=constructPath(parent);
    }

    String getUsername(){
        return username;
    }
    void setUsername(String newUser){
        username=newUser;
    }

    int getPermission(){
        return permission;
    }
    void setPermission(int newPermission){
        permission=newPermission;
    }

    Date getModificationTime(){
        return modificationTime;
    }
    void setModificationTime(){
        modificationTime=new Date();
    }

    Date getAccessTime(){
        return accessTime;
    }
    void setAccessTime(){
        accessTime=new Date();
    }

    String getName(){
        return name;
    }
    void setName(String newName){
        name=newName;
    }

    String getFullPathName(){
        return fullPathName;
    }

    long getId(){
        return id;
    }

    @Override
    public int compareTo(INode o) {
        return name.compareTo(o.name);
    }
}
