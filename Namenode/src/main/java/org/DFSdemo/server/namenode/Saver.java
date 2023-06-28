package org.DFSdemo.server.namenode;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Saver {
    private final String inodeSql;
    private final String inodeDirectorySql;
    private final String inodeFileSql;
    private final String blockSql;
    Connection conn;
    FSDirectory fs;

    Saver(Connection conn){
        this.conn=conn;
        inodeSql="INSERT INTO inode VALUES (?,?,?,?,?,?)";
        inodeDirectorySql="INSERT INTO inodedirectory VALUES (?,?)";
        inodeFileSql="INSERT INTO inodefile VALUES (?,?)";
        blockSql="INSERT INTO block VALUES (?,?,?,?)";
        fs=FSDirectory.getInstance();
    }

    void save(){
        try{
            PreparedStatement pstmt1 = conn.prepareStatement(inodeSql);
            PreparedStatement pstmt2 = conn.prepareStatement(inodeDirectorySql);
            PreparedStatement pstmt3 = conn.prepareStatement(inodeFileSql);
            PreparedStatement pstmt4 = conn.prepareStatement(blockSql);
            for(INode inode : fs.getINodeMap().values()){
                pstmt1.setLong(1, inode.getId());
                pstmt1.setLong(2, inode.getParent().getId());
                pstmt1.setString(3, inode.getName());
                pstmt1.setInt(4, inode.getPermission());
                pstmt1.setString(5, inode.getModificationTime().toString());
                pstmt1.setString(6, inode.getAccessTime().toString());
                pstmt1.executeUpdate();
                if(inode.isDirectory()){
                    for(INode child : ((INodeDirectory)inode).getChildren()){
                        pstmt2.setLong(1, inode.getId());
                        pstmt2.setLong(2, child.getId());
                        pstmt2.executeUpdate();
                    }
                }else{
                    pstmt3.setLong(1, inode.getId());
                    pstmt3.setLong(2, ((INodeFile)inode).getHeader());
                    pstmt3.executeUpdate();
                }
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}
