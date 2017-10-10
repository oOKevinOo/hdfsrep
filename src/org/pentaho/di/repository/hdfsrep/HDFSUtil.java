package org.pentaho.di.repository.hdfsrep;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by zhiwen wang on 2017/10/1.
 */
public class HDFSUtil {
    public static FileStatus getFileStatus(String path , FileSystem fs) throws IOException {
        return fs.getFileStatus(new Path(path));
    }

    public static FileStatus[] listFileStatus(String path , FileSystem fs) throws IOException {
        return fs.listStatus(new Path(path));
    }
}
