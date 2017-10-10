package org.pentaho.di.repository.hdfsrep;

import org.apache.hadoop.conf.Configuration;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.BaseRepositoryMeta;
import org.pentaho.di.repository.RepositoryCapabilities;
import org.pentaho.di.repository.RepositoryMeta;
import org.w3c.dom.Node;

import java.util.List;


/**
 * Created by zhiwen wang on 2017/9/29.
 */
public class KettleHDFSRepositoryMeta extends BaseRepositoryMeta implements RepositoryMeta {
    public static String REPOSITORY_TYPE_ID = "KettleHDFSRepository";
    private String baseDirectory;
    private boolean readOnly;
    private boolean hidingHiddenFiles;
    private String hadoopCoreConfigFilePath;
    private String hadoopHDFSConfigFilePath;
    private Configuration conf;
    public KettleHDFSRepositoryMeta() {
        super(REPOSITORY_TYPE_ID);
    }

    public KettleHDFSRepositoryMeta(String id, String name, String description,String baseDirectory,String coreFile,String hdfsFile,boolean readOnly,boolean hidingHiddenFiles) {
        super(id, name, description);
        this.baseDirectory=baseDirectory;
        this.hadoopCoreConfigFilePath = coreFile;
        this.hadoopHDFSConfigFilePath = hdfsFile;
        this.readOnly = readOnly;
        this.hidingHiddenFiles = hidingHiddenFiles;
        this.conf = new Configuration();
        this.conf.addResource(hadoopCoreConfigFilePath);
        this.conf.addResource(hadoopHDFSConfigFilePath);
    }
    @Override
    public RepositoryCapabilities getRepositoryCapabilities() {
         return new RepositoryCapabilities() {
            public boolean supportsUsers() {
                return false;
            }

            public boolean managesUsers() {
                return false;
            }

            public boolean isReadOnly() {
                return readOnly;
            }

            public boolean supportsRevisions() {
                return false;
            }

            public boolean supportsMetadata() {
                return false;
            }

            public boolean supportsLocking() {
                return false;
            }

            public boolean hasVersionRegistry() {
                return false;
            }

            public boolean supportsAcls() {
                return false;
            }

            public boolean supportsReferences() {
                return false;
            }
        };
    }

    @Override
    public void loadXML(Node repnode, List<DatabaseMeta> databases) throws KettleException {
        super.loadXML( repnode, databases );
        try {
            baseDirectory = XMLHandler.getTagValue( repnode, "base_directory" );
            hadoopCoreConfigFilePath = XMLHandler.getTagValue( repnode, "hadoop_core_file" );
            hadoopHDFSConfigFilePath = XMLHandler.getTagValue( repnode, "hadoop_hdfs_file" );
            readOnly = "Y".equalsIgnoreCase( XMLHandler.getTagValue( repnode, "read_only" ) );
            hidingHiddenFiles = "Y".equalsIgnoreCase( XMLHandler.getTagValue( repnode, "hides_hidden_files" ) );
        } catch ( Exception e ) {
            throw new KettleException( "Unable to load Kettle file repository meta object", e );
        }
    }

    @Override
    public String getXML() {
        StringBuffer retval = new StringBuffer();

        retval.append( "  " ).append( XMLHandler.openTag( XML_TAG ) );
        retval.append( super.getXML() );
        retval.append( "    " ).append( XMLHandler.addTagValue( "base_directory", baseDirectory ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "hadoop_core_file", hadoopCoreConfigFilePath ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "hadoop_hdfs_file", hadoopHDFSConfigFilePath ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "read_only", readOnly ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "hides_hidden_files", hidingHiddenFiles ) );
        retval.append( "  " ).append( XMLHandler.closeTag( XML_TAG ) );

        return retval.toString();
    }

    @Override
    public RepositoryMeta clone() {
        return null;
    }
    public Configuration getConf() {
        return this.conf;
    }

    public String getBaseDirectory() {
        return baseDirectory;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isHidingHiddenFiles() {
        return hidingHiddenFiles;
    }

    public String getHadoopCoreConfigFilePath() {
        return hadoopCoreConfigFilePath;
    }

    public String getHadoopHDFSConfigFilePath() {
        return hadoopHDFSConfigFilePath;
    }
}
