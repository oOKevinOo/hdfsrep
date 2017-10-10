package org.pentaho.di.repository.hdfsrep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ProgressMonitorListener;
import org.pentaho.di.core.changed.ChangedFlagInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.core.xml.XMLInterface;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.partition.PartitionSchema;
import org.pentaho.di.repository.*;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;


/**
 * Created by zhiwen wang on 2017/9/29.
 */
public class KettleHDFSRepository  extends AbstractRepository {
    private static final String EXT_TRANSFORMATION = ".ktr";
    private static final String EXT_JOB = ".kjb";
    private static final String EXT_DATABASE = ".kdb";
    private static final String EXT_SLAVE_SERVER = ".ksl";
    private static final String EXT_CLUSTER_SCHEMA = ".kcs";
    private static final String EXT_PARTITION_SCHEMA = ".kps";

    private static final String LOG_FILE = "repository.log";

    public static final String FILE_REPOSITORY_VERSION = "0.1";

    private KettleHDFSRepositoryMeta repositoryMeta;
    private KettleHDFSRepositorySecurityProvider securityProvider;
    private boolean connected;


    private FileSystem fs;

    private LogChannelInterface log;

    private Map<Class<? extends IRepositoryService>, IRepositoryService> serviceMap;
    private List<Class<? extends IRepositoryService>> serviceList;

    public HDFSXmlMetaStore metaStore;

    @Override
    public void connect(String username, String password) throws KettleException {
        String coreConf = this.repositoryMeta.getHadoopCoreConfigFilePath();
        String hdfsConf = this.repositoryMeta.getHadoopHDFSConfigFilePath();
        Configuration conf = new Configuration();
        try {
            conf.addResource(new File(coreConf).toURL());
            conf.addResource(new File(hdfsConf).toURL());
            conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
            this.fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new KettleException( e );
        }
        //todo 暂时不打算管理repository元数据
        String metaStoreRootFolder = this.repositoryMeta.getBaseDirectory() + File.separator + ".meta";
        Path metaStoreRootFolderFile = new Path( this.repositoryMeta.getBaseDirectory() + File.separator + ".meta" );
        try {
            if ( !fs.exists(metaStoreRootFolderFile) ) {
                if ( this.repositoryMeta.isReadOnly() ) {
                    this.metaStore = null;
                } else {
                    if ( fs.mkdirs(metaStoreRootFolderFile) ) {
                        this.metaStore = new HDFSXmlMetaStore( metaStoreRootFolder,fs );
                    } else {
                        this.metaStore = null;
                    }
                }
            } else {
                this.metaStore = new HDFSXmlMetaStore( metaStoreRootFolder,fs );
            }
        } catch (IOException e1) {
            e1.printStackTrace();
        } catch (MetaStoreException e1) {
            e1.printStackTrace();
        }
        connected = true;
    }

    @Override
    public void disconnect() {
        fs = null;
        metaStore = null;
        connected = false;
    }

    @Override
    public boolean isConnected() {
        return this.connected;
    }

    @Override
    public void init(RepositoryMeta repositoryMeta) {
        this.serviceMap = new HashMap<Class<? extends IRepositoryService>, IRepositoryService>();
        this.serviceList = new ArrayList<Class<? extends IRepositoryService>>();
        this.repositoryMeta = (KettleHDFSRepositoryMeta) repositoryMeta;
        this.securityProvider = new KettleHDFSRepositorySecurityProvider( );
        this.serviceMap.put( RepositorySecurityProvider.class, securityProvider );
        this.serviceList.add( RepositorySecurityProvider.class );
        this.log = new LogChannel( this );
        try {
            connect(null,null);
        } catch (KettleException e) {
            e.printStackTrace();
        }
    }

    private String calcDirectoryName( RepositoryDirectoryInterface dir ) {
        StringBuilder directory = new StringBuilder();
        String baseDir = repositoryMeta.getBaseDirectory();
        baseDir = Const.replace( baseDir, "\\", "/" );
        directory.append( baseDir );
        if ( !baseDir.endsWith( "/" ) ) {
            directory.append( "/" );
        }

        if ( dir != null ) {
            String path = calcRelativeElementDirectory( dir );
            if ( path.startsWith( "/" ) ) {
                directory.append( path.substring( 1 ) );
            } else {
                directory.append( path );
            }
            if ( !path.endsWith( "/" ) ) {
                directory.append( "/" );
            }
        }
        return directory.toString();
    }

    public String calcRelativeElementDirectory( RepositoryDirectoryInterface dir ) {
        if ( dir != null ) {
            return dir.getPath();
        } else {
            return "/";
        }
    }

    public String calcObjectId( RepositoryDirectoryInterface dir ) {
        StringBuilder id = new StringBuilder();

        String path = calcRelativeElementDirectory( dir );
        id.append( path );
        if ( !path.endsWith( "/" ) ) {
            id.append( "/" );
        }

        return id.toString();
    }

    public String calcObjectId(RepositoryDirectoryInterface directory, String name, String extension ) {
        StringBuilder id = new StringBuilder();
        String path = calcRelativeElementDirectory( directory );
        id.append( path );
        if ( !path.endsWith( "/" ) ) {
            id.append( "/" );
        }

        if ( name.startsWith( "/" ) ) {
            id.append( name.substring( 1 ) ).append( extension );
        } else {
            id.append( name ).append( extension );
        }

        return id.toString();
    }
    public String calcObjectId( RepositoryElementInterface element ) {
        RepositoryDirectoryInterface directory = element.getRepositoryDirectory();
        String name = element.getName();
        String extension = element.getRepositoryElementType().getExtension();

        return calcObjectId( directory, name, extension );
    }

    private String calcFilename( RepositoryElementInterface element ) {
        return calcFilename( element.getRepositoryDirectory(), element.getName(), element
                .getRepositoryElementType().getExtension() );
    }

    private String calcFilename(RepositoryDirectoryInterface dir, String name, String extension ) {
        StringBuilder filename = new StringBuilder();
        filename.append( calcDirectoryName( dir ) );

        String objectName = name + extension;
        filename.append( objectName );

        return filename.toString();
    }

    public String calcFilename( ObjectId id ) {
        return calcDirectoryName( null ) + id.toString();
    }

    @Override
    public boolean exists(String name, RepositoryDirectoryInterface repositoryDirectory, RepositoryObjectType objectType) throws KettleException {
        Path path = new Path(calcFilename( repositoryDirectory, name, objectType.getExtension() ));
        try {
            return fs.exists(path);
        } catch (IOException e) {
            throw new KettleException(e);
        }
    }

    @Override
    public ObjectId getTransformationID(String name, RepositoryDirectoryInterface repositoryDirectory) throws KettleException {
        return  getObjectId( repositoryDirectory, name, EXT_TRANSFORMATION );
    }
    private ObjectId getObjectId(RepositoryDirectoryInterface repositoryDirectory, String name, String extension ) throws KettleException {
        try {
            String filename = calcFilename( repositoryDirectory, name, extension );
            if ( !fs.exists( new Path(filename ))) {
                return null;
            }
            return new StringObjectId( calcObjectId( repositoryDirectory, name, extension ) );
        } catch ( Exception e ) {
            throw new KettleException( "Error finding ID for directory ["
                    + repositoryDirectory + "] and name [" + name + "]", e );
        }
    }

    @Override
    public ObjectId getJobId(String name, RepositoryDirectoryInterface repositoryDirectory) throws KettleException {
        return  getObjectId( repositoryDirectory, name, EXT_JOB );
    }

    @Override
    public void save(RepositoryElementInterface repositoryElement, String versionComment, ProgressMonitorListener monitor, boolean overwrite) throws KettleException {
        try {
            if (!(repositoryElement instanceof XMLInterface)
                    && !(repositoryElement instanceof SharedObjectInterface)) {
                throw new KettleException("Class ["
                        + repositoryElement.getClass().getName()
                        + "] needs to implement the XML Interface in order to save it to disk");
            }

            if (!Const.isEmpty(versionComment)) {
                insertLogEntry("Save repository element : " + repositoryElement.toString() + " : " + versionComment);
            }

            ObjectId objectId = new StringObjectId(calcObjectId(repositoryElement));
            String xml = ( (XMLInterface) repositoryElement ).getXML();
            String fileName = calcFilename(repositoryElement);
            OutputStream os =  fs.create(new Path(fileName),overwrite);
            os.write( xml.getBytes( Const.XML_ENCODING ) );
            os.close();

            if ( repositoryElement instanceof ChangedFlagInterface) {
                ( (ChangedFlagInterface) repositoryElement ).clearChanged();
            }
            //todo exception for aready exit file

        } catch (Exception e) {

        }


    }


    @Override
    public void save(RepositoryElementInterface repositoryElement, String versionComment, Calendar versionDate, ProgressMonitorListener monitor, boolean overwrite) throws KettleException {
        save(repositoryElement,versionComment,monitor,overwrite);
    }

    @Override
    public RepositoryDirectoryInterface getDefaultSaveDirectory(RepositoryElementInterface repositoryElement) throws KettleException {
        return  getUserHomeDirectory();
    }
    public RepositoryDirectoryInterface getUserHomeDirectory() throws KettleException {
        RepositoryDirectory root = new RepositoryDirectory();
        root.setObjectId( null );
        return loadRepositoryDirectoryTree( root );
    }
    public RepositoryDirectoryInterface loadRepositoryDirectoryTree(RepositoryDirectoryInterface dir ) throws KettleException {
        try {
            String folderName = calcDirectoryName( dir );
            for(FileStatus fileStatus : HDFSUtil.listFileStatus(folderName,fs)) {
                if(!fileStatus.isFile()) {
                    String sub = fileStatus.getPath().getName();
                    RepositoryDirectory subDir = new RepositoryDirectory( dir, sub);
                    subDir.setObjectId( new StringObjectId( calcObjectId( subDir ) ) );
                    dir.addSubdirectory( subDir );
                    loadRepositoryDirectoryTree( subDir );
                }
            }
            return dir;
        } catch ( Exception e ) {
            throw new KettleException( "Unable to load the directory tree from this file repository", e );
        }
    }

    @Override
    public void clearSharedObjectCache() {

    }

    @Override
    public TransMeta loadTransformation(String transname, RepositoryDirectoryInterface repdir, ProgressMonitorListener monitor, boolean setInternalVariables, String revision) throws KettleException {
        // This is a standard load of a transformation serialized in XML...
        //
        try {
            String filename = calcDirectoryName(repdir) + transname ;
            if(!filename.endsWith(EXT_TRANSFORMATION)) {
                filename = filename + EXT_TRANSFORMATION;
            }
            Path path = new Path(filename);
            TransMeta transMeta = new TransMeta(fs.open(path),this, setInternalVariables,null,null);
            transMeta.setRepository(this);
            transMeta.setMetaStore(getMetaStore());
            transMeta.setFilename(null);
            transMeta.setName(transname);
            transMeta.setObjectId(new StringObjectId(calcObjectId(repdir, transname, EXT_TRANSFORMATION)));
//todo metadata opp is del
//        readDatabases( transMeta, true );
            transMeta.clearChanged();

            return transMeta;
        }
        catch (IOException e) {
            throw new KettleException(e);
        }
    }

    @Override
    public TransMeta loadTransformation(ObjectId id_transformation, String versionLabel) throws KettleException {
        RepositoryObject jobInfo = getObjectInformation( id_transformation, RepositoryObjectType.TRANSFORMATION );
        return loadTransformation( jobInfo.getName(), jobInfo.getRepositoryDirectory(), null, true, versionLabel );
    }

    @Override
    public SharedObjects readTransSharedObjects(TransMeta transMeta) throws KettleException {
        // todo unknow what this use, read carefully later!!!

        // First the normal shared objects...
        //
        SharedObjects sharedObjects = transMeta.readSharedObjects();

        // Then we read the databases etc...
        //
        for ( ObjectId id : getDatabaseIDs( false ) ) {
            DatabaseMeta databaseMeta = loadDatabaseMeta( id, null ); // Load last version
            databaseMeta.shareVariablesWith( transMeta );
            transMeta.addOrReplaceDatabase( databaseMeta );
        }

        for ( ObjectId id : getSlaveIDs( false ) ) {
            SlaveServer slaveServer = loadSlaveServer( id, null ); // Load last version
            slaveServer.shareVariablesWith( transMeta );
            transMeta.addOrReplaceSlaveServer( slaveServer );
        }

        for ( ObjectId id : getClusterIDs( false ) ) {
            ClusterSchema clusterSchema = loadClusterSchema( id, transMeta.getSlaveServers(), null ); // Load last version
            clusterSchema.shareVariablesWith( transMeta );
            transMeta.addOrReplaceClusterSchema( clusterSchema );
        }

        for ( ObjectId id : getPartitionSchemaIDs( false ) ) {
            PartitionSchema partitionSchema = loadPartitionSchema( id, null ); // Load last version
            transMeta.addOrReplacePartitionSchema( partitionSchema );
        }

        return sharedObjects;
    }

    @Override
    public ObjectId renameTransformation(ObjectId id_transformation, RepositoryDirectoryInterface newDirectory, String newName) throws KettleException {
        return null;
    }

    @Override
    public ObjectId renameTransformation(ObjectId id_transformation, String versionComment, RepositoryDirectoryInterface newDirectory, String newName) throws KettleException {
            ObjectId objectId = null;
//                    renameObject( id_transformation, newDir, newName, EXT_TRANSFORMATION );
            if ( !Const.isEmpty( versionComment ) ) {
                insertLogEntry( "Rename transformation : " + versionComment );
            }
            return objectId;
    }
    private ObjectId renameObject(ObjectId id, RepositoryDirectoryInterface newDirectory, String newName,
                                  String extension ) throws KettleException {
        try {
            // In case of a root object, the ID is the same as the relative filename...
            //
//            FileObject fileObject = KettleVFS.getFileObject( calcDirectoryName( null ) + id.getId() );

            // Same name, different folder?
            String oldName = calcFilename(id);
            if ( Const.isEmpty( newName ) ) {
                newName = calcObjectName( id );
            }
            // The new filename can be anywhere so we re-calculate a new ID...
            //
            String newFilename = calcDirectoryName( newDirectory ) + newName + extension;

//            FileObject newObject = KettleVFS.getFileObject( newFilename );
//            fileObject.moveTo( newObject );
            fs.rename(new Path(oldName),new Path(newFilename));
            return new StringObjectId( calcObjectId( newDirectory, newName, extension ) );
        } catch ( Exception e ) {
            throw new KettleException( "Unable to rename object with ID [" + id + "] to [" + newName + "]", e );
        }

    }
    private String calcObjectName( ObjectId id ) {
        int slashIndex = id.getId().lastIndexOf( '/' );
        int dotIndex = id.getId().lastIndexOf( '.' );

        return id.getId().substring( slashIndex + 1, dotIndex );
    }
    @Override
    public void deleteTransformation(ObjectId id_transformation) throws KettleException {

    }

    @Override
    public JobMeta loadJob(String jobname, RepositoryDirectoryInterface repdir, ProgressMonitorListener monitor, String revision) throws KettleException {
        try {
            String filename = calcDirectoryName( repdir ) + jobname;
            if(!filename.endsWith(EXT_JOB)) {
                filename = filename  + EXT_JOB;
            }
            Path path = new Path(filename);
            JobMeta jobMeta = null;

            jobMeta = new JobMeta( fs.open(path), this,null );
            jobMeta.setFilename( null );
            jobMeta.setName( jobname );
            jobMeta.setObjectId( new StringObjectId( calcObjectId( repdir, jobname, EXT_JOB ) ) );

//        readDatabases( jobMeta, true );
            jobMeta.clearChanged();
            return jobMeta;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public JobMeta loadJob(ObjectId id_job, String versionLabel) throws KettleException {
        RepositoryObject jobInfo = getObjectInformation( id_job, RepositoryObjectType.JOB );
        return loadJob( jobInfo.getName(), jobInfo.getRepositoryDirectory(), null, versionLabel );
    }

    @Override
    public SharedObjects readJobMetaSharedObjects(JobMeta jobMeta) throws KettleException {

        // First the normal shared objects...
        //
        SharedObjects sharedObjects = jobMeta.readSharedObjects();

        // Then we read the databases etc...
        //
        for ( ObjectId id : getDatabaseIDs( false ) ) {
            DatabaseMeta databaseMeta = loadDatabaseMeta( id, null ); // Load last version
            databaseMeta.shareVariablesWith( jobMeta );
            jobMeta.addOrReplaceDatabase( databaseMeta );
        }

        for ( ObjectId id : getSlaveIDs( false ) ) {
            SlaveServer slaveServer = loadSlaveServer( id, null ); // Load last version
            slaveServer.shareVariablesWith( jobMeta );
            jobMeta.addOrReplaceSlaveServer( slaveServer );
        }

        return sharedObjects;
    }

    @Override
    public ObjectId renameJob(ObjectId id_job, String versionComment, RepositoryDirectoryInterface newDirectory, String newName) throws KettleException {
        ObjectId objectId = renameObject( id_job, newDirectory, newName, EXT_JOB );
        if ( !Const.isEmpty( versionComment ) ) {
            insertLogEntry( "Rename job : " + versionComment );
        }
        return objectId;
    }

    @Override
    public ObjectId renameJob(ObjectId id_job, RepositoryDirectoryInterface newDirectory, String newName) throws KettleException {
        return renameJob( id_job, null, newDirectory, newName );
    }

    @Override
    public void deleteJob(ObjectId id_job) throws KettleException {

    }

    @Override
    public DatabaseMeta loadDatabaseMeta(ObjectId id_database, String revision) throws KettleException {
        try {
            return new DatabaseMeta( loadNodeFromXML( id_database, DatabaseMeta.XML_TAG ) );
        } catch ( Exception e ) {
            throw new KettleException( "Unable to load database connection from the file repository", e );
        }
    }

    public Node loadNodeFromXML(ObjectId id, String tag ) throws KettleException {
        try {
            // The object ID is the base name of the file in the Base directory folder
            //
            String filename = calcDirectoryName( null ) + id.getId();
            Path path = new Path(filename);
//            FileObject fileObject = KettleVFS.getFileObject( filename );
            Document document = XMLHandler.loadXMLFile( fs.open(path) );
            Node node = XMLHandler.getSubNode( document, tag );
            return node;
        } catch ( Exception e ) {
            throw new KettleException( "Unable to load XML object from object with ID ["
                    + id + "] and tag [" + tag + "]", e );
        }
    }

    @Override
    public void deleteDatabaseMeta(String databaseName) throws KettleException {
        deleteRootObject( databaseName, EXT_DATABASE );
    }
    public void deleteRootObject( String name, String extension ) throws KettleException {
        try {
            String filename = calcDirectoryName( null ) + name + extension;
            Path path = new Path(filename);
            fs.delete(path,true);
        } catch ( Exception e ) {
            throw new KettleException( "Unable to delete database with name ["
                    + name + "] and extension [" + extension + "]", e );
        }
    }

    @Override
    public ObjectId[] getDatabaseIDs(boolean includeDeleted) throws KettleException {
        return getRootObjectIDs( EXT_DATABASE );
    }
    private ObjectId[] getRootObjectIDs(String extension ) throws KettleException {
        try {
            // Get all the files in the root directory with a certain extension...
            //
            List<ObjectId> list = new ArrayList<ObjectId>();

            String folderName = repositoryMeta.getBaseDirectory();
//            FileObject folder = KettleVFS.getFileObject( folderName );

            for (FileStatus fileStatus : HDFSUtil.listFileStatus(folderName,fs) ) {
                if ( fileStatus.isFile() ) {
                    if (  !repositoryMeta.isHidingHiddenFiles() ) {
                        String name = fileStatus.getPath().getName();

                        if ( name.endsWith( extension ) ) {
                            list.add( new StringObjectId( name ) );
                        }
                    }
                }
            }

            return list.toArray( new ObjectId[list.size()] );
        } catch ( Exception e ) {
            throw new KettleException( "Unable to get root object ids for extension [" + extension + "]", e );
        }
    }

    @Override
    public String[] getDatabaseNames(boolean includeDeleted) throws KettleException {
        return convertRootIDsToNames( getDatabaseIDs( false ) );
    }
    private String[] convertRootIDsToNames( ObjectId[] ids ) {
        String[] names = new String[ids.length];
        for ( int i = 0; i < names.length; i++ ) {
            String id = ids[i].getId();
            names[i] = id.substring( 0, id.length() - 4 ); // get rid of the extension
        }
        return names;
    }

    @Override
    public List<DatabaseMeta> readDatabases() throws KettleException {
        List<DatabaseMeta> list = new ArrayList<DatabaseMeta>();
        for ( ObjectId id : getDatabaseIDs( false ) ) {
            list.add( loadDatabaseMeta( id, null ) );
        }
        return list;
    }

    @Override
    public ObjectId getDatabaseID(String name) throws KettleException {
        ObjectId match = getObjectId( null, name, EXT_DATABASE );
        if ( match == null ) {
            // exact match failed, trying to find the DB case-insensitively
            ObjectId[] existingIds = getDatabaseIDs( false );
            String[] existingNames = getDatabaseNames( existingIds );
            int index = DatabaseMeta.indexOfName( existingNames, name );
            if ( index != -1 ) {
                return getObjectId( null, existingNames[ index ], EXT_DATABASE );
            }
        }

        return match;
    }
    private String[] getDatabaseNames( ObjectId[] databaseIds ) throws KettleException {
        return convertRootIDsToNames( databaseIds );
    }

    @Override
    public ClusterSchema loadClusterSchema(ObjectId id_cluster_schema, List<SlaveServer> slaveServers, String versionLabel) throws KettleException {
        try {
            return new ClusterSchema( loadNodeFromXML( id_cluster_schema, ClusterSchema.XML_TAG ), slaveServers );
        } catch ( Exception e ) {
            throw new KettleException( "Unable to load cluster schema from the file repository", e );
        }
    }

    @Override
    public ObjectId[] getClusterIDs(boolean includeDeleted) throws KettleException {
        return getRootObjectIDs( EXT_CLUSTER_SCHEMA );
    }

    @Override
    public String[] getClusterNames(boolean includeDeleted) throws KettleException {
        return convertRootIDsToNames( getClusterIDs( false ) );
    }

    @Override
    public ObjectId getClusterID(String name) throws KettleException {
        return new StringObjectId( calcObjectId( (RepositoryDirectory) null ) + name + EXT_SLAVE_SERVER );
    }

    @Override
    public void deleteClusterSchema(ObjectId id_cluster) throws KettleException {
        deleteFile( id_cluster.getId() );
    }
    public void deleteFile( String filename ) throws KettleException {
        try {
            Path fileObject = new Path( filename );
            fs.delete(fileObject,true);
        } catch ( Exception e ) {
            throw new KettleException( "Unable to delete file with name [" + filename + "]", e );
        }
    }

    @Override
    public SlaveServer loadSlaveServer(ObjectId id_slave_server, String versionLabel) throws KettleException {
        try {
            return new SlaveServer( loadNodeFromXML( id_slave_server, SlaveServer.XML_TAG ) );
        } catch ( Exception e ) {
            throw new KettleException( "Unable to load slave server from the file repository", e );
        }
    }

    @Override
    public ObjectId[] getSlaveIDs(boolean includeDeleted) throws KettleException {
        return getRootObjectIDs( EXT_SLAVE_SERVER );
    }

    @Override
    public String[] getSlaveNames(boolean includeDeleted) throws KettleException {
        return convertRootIDsToNames( getSlaveIDs( false ) );
    }

    @Override
    public List<SlaveServer> getSlaveServers() throws KettleException {
        List<SlaveServer> list = new ArrayList<SlaveServer>();
        for ( ObjectId id : getSlaveIDs( false ) ) {
            list.add( loadSlaveServer( id, null ) ); // Load last version
        }
        return list;
    }

    @Override
    public ObjectId getSlaveID(String name) throws KettleException {
        // Only return the ID if the slave server exists
        Object slaveID = name + EXT_SLAVE_SERVER;

        Object[] ids = getRootObjectIDs( EXT_SLAVE_SERVER );
        for ( Object rootID : ids ) {
            if ( rootID.toString().equals( slaveID ) ) {
                return new StringObjectId( slaveID.toString() );
            }
        }

        return null;
    }

    @Override
    public void deleteSlave(ObjectId id_slave) throws KettleException {
        deleteFile( calcDirectoryName( null ) + id_slave.getId() );
    }

    @Override
    public PartitionSchema loadPartitionSchema(ObjectId id_partition_schema, String versionLabel) throws KettleException {
        try {
            return new PartitionSchema( loadNodeFromXML( id_partition_schema, PartitionSchema.XML_TAG ) );
        } catch ( Exception e ) {
            throw new KettleException( "Unable to load partition schema from the file repository", e );
        }
    }

    @Override
    public ObjectId[] getPartitionSchemaIDs(boolean includeDeleted) throws KettleException {
        return getRootObjectIDs( EXT_PARTITION_SCHEMA );
    }

    @Override
    public String[] getPartitionSchemaNames(boolean includeDeleted) throws KettleException {
        return convertRootIDsToNames( getPartitionSchemaIDs( false ) );
    }

    @Override
    public ObjectId getPartitionSchemaID(String name) throws KettleException {
        return new StringObjectId( calcObjectId( (RepositoryDirectory) null ) + name + EXT_SLAVE_SERVER );
    }

    @Override
    public void deletePartitionSchema(ObjectId id_partition_schema) throws KettleException {
        deleteFile( id_partition_schema.getId() );
    }

    @Override
    public RepositoryDirectoryInterface loadRepositoryDirectoryTree() throws KettleException {
        RepositoryDirectory root = new RepositoryDirectory();
        root.setObjectId( new StringObjectId( "/" ) );
        return loadRepositoryDirectoryTree( root );
    }

    @Override
    public RepositoryDirectoryInterface findDirectory(String directory) throws KettleException {
        return loadRepositoryDirectoryTree().findDirectory( directory );
    }

    @Override
    public RepositoryDirectoryInterface findDirectory(ObjectId directory) throws KettleException {
        return loadRepositoryDirectoryTree().findDirectory( directory );
    }

    @Override
    public void saveRepositoryDirectory(RepositoryDirectoryInterface dir) throws KettleException {
        try {
            String filename = calcDirectoryName( dir );
            ObjectId objectId = new StringObjectId( calcRelativeElementDirectory( dir ) );
            Path makeDir = new Path(filename);
            fs.mkdirs(makeDir);
//            FileObject fileObject = KettleVFS.getFileObject( filename );
//            fileObject.createFolder(); // also create parents

            dir.setObjectId( objectId );

            log.logDetailed( "New id of directory = " + dir.getObjectId() );
        } catch ( Exception e ) {
            throw new KettleException( "Unable to save directory [" + dir + "] in the repository", e );
        }
    }

    @Override
    public void deleteRepositoryDirectory(RepositoryDirectoryInterface dir) throws KettleException {

    }

    @Override
    public ObjectId renameRepositoryDirectory(ObjectId id, RepositoryDirectoryInterface newParentDir, String newName) throws KettleException {
        if ( newParentDir != null || newName != null ) {
            try {
                // In case of a root object, the ID is the same as the relative filename...
                RepositoryDirectoryInterface tree = loadRepositoryDirectoryTree();
                RepositoryDirectoryInterface dir = tree.findDirectory( id );

                if ( dir == null ) {
                    throw new KettleException( "Could not find folder [" + id + "]" );
                }

                // If newName is null, keep the current name
                newName = ( newName != null ) ? newName : dir.getName();
                Path folder = new Path(dir.getPath());
//                FileObject folder = KettleVFS.getFileObject( dir.getPath() );

                String newFolderName = null;

                if ( newParentDir != null ) {
//                    FileObject newParentFolder = KettleVFS.getFileObject( newParentDir.getPath() );
                    Path newParentFolder = new Path(newParentDir.getPath());
                    newFolderName = newParentFolder.toString() + "/" + newName;
                } else {
                    newFolderName = folder.getParent().toString() + "/" + newName;
                }

//                FileObject newFolder = KettleVFS.getFileObject( newFolderName );
                Path newFolder = new Path(newFolderName);
                fs.rename(folder,newFolder);
//                folder( newFolder );

                return new StringObjectId( dir.getObjectId() );
            } catch ( Exception e ) {
                throw new KettleException( "Unable to rename directory folder to [" + id + "]" );
            }
        }
        return ( id );
    }

    @Override
    public RepositoryDirectoryInterface createRepositoryDirectory(RepositoryDirectoryInterface parentDirectory, String directoryPath) throws KettleException {
        String folder = calcDirectoryName( parentDirectory );
        String newFolder = folder;
        if ( folder.endsWith( "/" ) ) {
            newFolder += directoryPath;
        } else {
            newFolder += "/" + directoryPath;
        }
        Path parent = new Path(newFolder);
//        FileObject parent = KettleVFS.getFileObject( newFolder );
        try {
//            parent.createFolder();
            fs.create(parent,true);
        } catch ( IOException e ) {
            throw new KettleException( "Unable to create folder " + newFolder, e );
        }

        // Incremental change of the directory structure...
        //
        RepositoryDirectory newDir = new RepositoryDirectory( parentDirectory, directoryPath );
        parentDirectory.addSubdirectory( newDir );
        newDir.setObjectId( new StringObjectId( calcObjectId( newDir ) ) );

        return newDir;
    }

    @Override
    public String[] getTransformationNames(ObjectId id_directory, boolean includeDeleted) throws KettleException {
        try {
            List<String> list = new ArrayList<String>();

            RepositoryDirectoryInterface tree = loadRepositoryDirectoryTree();
            RepositoryDirectoryInterface directory = tree.findDirectory( id_directory );

            String folderName = calcDirectoryName( directory );
            Path folder = new Path(folderName);
//            FileObject folder = KettleVFS.getFileObject( folderName );

            for ( FileStatus child : fs.listStatus(folder)) {
                if ( child.isFile() ) {
                    if (  !repositoryMeta.isHidingHiddenFiles() ) {
                        String name = child.getPath().getName();

                        if ( name.endsWith( EXT_TRANSFORMATION ) ) {

                            String transName = name.substring( 0, name.length() - 4 );
                            list.add( transName );
                        }
                    }
                }
            }

            return list.toArray( new String[list.size()] );
        } catch ( Exception e ) {
            throw new KettleException(
                    "Unable to get list of transformations names in folder with id : " + id_directory, e );
        }
    }

    @Override
    public List<RepositoryElementMetaInterface> getJobObjects(ObjectId id_directory, boolean includeDeleted) throws KettleException {
        try {
            List<RepositoryElementMetaInterface> list = new ArrayList<RepositoryElementMetaInterface>();

            RepositoryDirectoryInterface tree = loadRepositoryDirectoryTree();
            RepositoryDirectoryInterface directory = tree.findDirectory( id_directory );

            String folderName = calcDirectoryName( directory );
            Path folder = new Path(folderName);
//            FileObject folder = KettleVFS.getFileObject( folderName );

            for ( FileStatus child : fs.listStatus(folder) ) {
                if ( child.isFile() ) {
                    if ( !repositoryMeta.isHidingHiddenFiles() ) {
                        String name = child.getPath().getName();

                        if ( name.endsWith( EXT_JOB ) ) {

                            String jobName = name.substring( 0, name.length() - 4 );

                            ObjectId id = new StringObjectId( calcObjectId( directory, jobName, EXT_JOB ) );
                            Date date = new Date( child.getModificationTime());
                            list.add( new RepositoryObject(
                                    id, jobName, directory, "-", date, RepositoryObjectType.JOB, "", false ) );
                        }
                    }
                }
            }

            return list;
        } catch ( Exception e ) {
            throw new KettleException( "Unable to get list of jobs in folder with id : " + id_directory, e );
        }
    }

    @Override
    public List<RepositoryElementMetaInterface> getTransformationObjects(ObjectId idDirectory, boolean includeDeleted) throws KettleException {
        try {
            List<RepositoryElementMetaInterface> list = new ArrayList<RepositoryElementMetaInterface>();

            RepositoryDirectoryInterface tree = loadRepositoryDirectoryTree();
            RepositoryDirectoryInterface directory = tree.findDirectory( idDirectory );

            String folderName = calcDirectoryName( directory );
            Path folder = new Path(folderName);
//            FileObject folder = KettleVFS.getFileObject( folderName );

            for ( FileStatus child : fs.listStatus(folder) ) {
                if (child.isFile() ) {
                    if ( !repositoryMeta.isHidingHiddenFiles() ) {

                        String name = child.getPath().getName();

                        if ( name.endsWith( EXT_TRANSFORMATION ) ) {

                            String transName = name.substring( 0, name.length() - 4 );

                            ObjectId id = new StringObjectId( calcObjectId( directory, transName, EXT_TRANSFORMATION ) );
                            Date date = new Date( child.getModificationTime() );
                            list.add( new RepositoryObject(
                                    id, transName, directory, "-", date, RepositoryObjectType.TRANSFORMATION, "", false ) );
                        }
                    }
                }
            }

            return list;
        } catch ( Exception e ) {
            throw new KettleException( "Unable to get list of transformations in folder with id : " + idDirectory, e );
        }
    }

    @Override
    public List<RepositoryElementMetaInterface> getJobAndTransformationObjects(ObjectId id_directory, boolean includeDeleted) throws KettleException {
        // TODO not the most efficient impl; also, no sorting is done
        List<RepositoryElementMetaInterface> objs = new ArrayList<RepositoryElementMetaInterface>();
        objs.addAll( getJobObjects( id_directory, includeDeleted ) );
        objs.addAll( getTransformationObjects( id_directory, includeDeleted ) );
        return objs;
    }

    @Override
    public String[] getJobNames(ObjectId id_directory, boolean includeDeleted) throws KettleException {
        try {
            List<String> list = new ArrayList<String>();

            RepositoryDirectoryInterface tree = loadRepositoryDirectoryTree();
            RepositoryDirectoryInterface directory = tree.findDirectory( id_directory );

            String folderName = calcDirectoryName( directory );
//            FileObject folder = KettleVFS.getFileObject( folderName );
            Path folder = new Path(folderName);
            for ( FileStatus child : fs.listStatus(folder) ) {
                if ( child.isFile()) {
                    if (  !repositoryMeta.isHidingHiddenFiles() ) {
                        String name = child.getPath().getName();

                        if ( name.endsWith( EXT_JOB ) ) {

                            String jobName = name.substring( 0, name.length() - 4 );
                            list.add( jobName );
                        }
                    }
                }
            }

            return list.toArray( new String[list.size()] );
        } catch ( Exception e ) {
            throw new KettleException(
                    "Unable to get list of transformations names in folder with id : " + id_directory, e );
        }
    }

    @Override
    public String[] getDirectoryNames(ObjectId id_directory) throws KettleException {
        RepositoryDirectoryInterface tree = loadRepositoryDirectoryTree();
        RepositoryDirectoryInterface directory = tree.findDirectory( id_directory );
        String[] names = new String[directory.getNrSubdirectories()];
        for ( int i = 0; i < names.length; i++ ) {
            names[i] = directory.getSubdirectory( i ).getName();
        }
        return names;
    }

    @Override
    public ObjectId insertLogEntry(String description) throws KettleException {
        String logfile = calcDirectoryName( null ) + LOG_FILE;
        try {
            OutputStream outputStream = KettleVFS.getOutputStream( logfile, true );
            outputStream.write( description.getBytes() );
            outputStream.write( Const.CR.getBytes() );
            outputStream.close();
            return new StringObjectId( logfile );
        } catch ( IOException e ) {
            throw new KettleException( "Unable to write log entry to file [" + logfile + "]" );
        }
    }

    @Override
    public void insertStepDatabase(ObjectId id_transformation, ObjectId id_step, ObjectId id_database) throws KettleException {

    }

    @Override
    public void insertJobEntryDatabase(ObjectId id_job, ObjectId id_jobentry, ObjectId id_database) throws KettleException {

    }

    @Override
    public void saveConditionStepAttribute(ObjectId id_transformation, ObjectId id_step, String code, Condition condition) throws KettleException {

    }

    @Override
    public Condition loadConditionFromStepAttribute(ObjectId id_step, String code) throws KettleException {
        return null;
    }

    @Override
    public boolean getStepAttributeBoolean(ObjectId id_step, int nr, String code, boolean def) throws KettleException {
        return false;
    }

    @Override
    public long getStepAttributeInteger(ObjectId id_step, int nr, String code) throws KettleException {
        return 0;
    }

    @Override
    public String getStepAttributeString(ObjectId id_step, int nr, String code) throws KettleException {
        return null;
    }

    @Override
    public void saveStepAttribute(ObjectId id_transformation, ObjectId id_step, int nr, String code, String value) throws KettleException {

    }

    @Override
    public void saveStepAttribute(ObjectId id_transformation, ObjectId id_step, int nr, String code, boolean value) throws KettleException {

    }

    @Override
    public void saveStepAttribute(ObjectId id_transformation, ObjectId id_step, int nr, String code, long value) throws KettleException {

    }

    @Override
    public void saveStepAttribute(ObjectId id_transformation, ObjectId id_step, int nr, String code, double value) throws KettleException {

    }

    @Override
    public int countNrStepAttributes(ObjectId id_step, String code) throws KettleException {
        return 0;
    }

    @Override
    public int countNrJobEntryAttributes(ObjectId id_jobentry, String code) throws KettleException {
        return 0;
    }

    @Override
    public long getJobEntryAttributeInteger(ObjectId id_jobentry, int nr, String code) throws KettleException {
        return 0;
    }

    @Override
    public String getJobEntryAttributeString(ObjectId id_jobentry, int nr, String code) throws KettleException {
        return null;
    }

    @Override
    public void saveJobEntryAttribute(ObjectId id_job, ObjectId id_jobentry, int nr, String code, String value) throws KettleException {

    }

    @Override
    public void saveJobEntryAttribute(ObjectId id_job, ObjectId id_jobentry, int nr, String code, boolean value) throws KettleException {

    }

    @Override
    public void saveJobEntryAttribute(ObjectId id_job, ObjectId id_jobentry, int nr, String code, long value) throws KettleException {

    }

    @Override
    public DatabaseMeta loadDatabaseMetaFromStepAttribute(ObjectId id_step, String code, List<DatabaseMeta> databases) throws KettleException {
        return null;
    }

    @Override
    public void saveDatabaseMetaStepAttribute(ObjectId id_transformation, ObjectId id_step, String code, DatabaseMeta database) throws KettleException {

    }

    @Override
    public DatabaseMeta loadDatabaseMetaFromJobEntryAttribute(ObjectId id_jobentry, String nameCode, int nr, String idCode, List<DatabaseMeta> databases) throws KettleException {
        return null;
    }

    @Override
    public void saveDatabaseMetaJobEntryAttribute(ObjectId id_job, ObjectId id_jobentry, int nr, String nameCode, String idCode, DatabaseMeta database) throws KettleException {

    }

    @Override
    public void undeleteObject(RepositoryElementMetaInterface repositoryObject) throws KettleException {

    }

    @Override
    public List<Class<? extends IRepositoryService>> getServiceInterfaces() throws KettleException {
        return serviceList;
    }

    @Override
    public IRepositoryService getService(Class<? extends IRepositoryService> clazz) throws KettleException {
        return serviceMap.get( clazz );
    }

    @Override
    public boolean hasService(Class<? extends IRepositoryService> clazz) throws KettleException {
        return serviceMap.containsKey( clazz );
    }

    @Override
    public RepositoryObject getObjectInformation(ObjectId objectId, RepositoryObjectType objectType) throws KettleException {
        try {
            String filename = calcDirectoryName( null );
            if ( objectId.getId().startsWith( "/" ) ) {
                filename += objectId.getId().substring( 1 );
            } else {
                filename += objectId.getId();
            }
            Path fileObject = new Path(filename);
//            FileObject fileObject = KettleVFS.getFileObject( filename );
            if ( !fs.exists(fileObject) ) {
                return null;
            }
            String fname = fileObject.getName();
            if ( (( fname.endsWith(".kjb")) || ( fname.endsWith(".ktr"))) && fname.length() > 4 ) {
                fname = fname.substring( 0, fname.length() - 4 );
            }

            String filePath = fileObject.getParent().toString();
            final Path baseDirObject = new Path( repositoryMeta.getBaseDirectory() );

            final int baseDirObjectPathLength = baseDirObject.toString().length();
            final String dirPath =
                    baseDirObjectPathLength <= filePath.length() ? filePath.substring( baseDirObjectPathLength ) : "/";
            RepositoryDirectoryInterface directory = loadRepositoryDirectoryTree().findDirectory( dirPath );
            Date lastModified = new Date( fs.listStatus(fileObject)[0].getModificationTime());

            return new RepositoryObject( objectId, fname, directory, "-", lastModified, objectType, "", false );

        } catch ( Exception e ) {
            throw new KettleException( "Unable to get object information for object with id=" + objectId, e );
        }
    }

    @Override
    public String getConnectMessage() {
        return null;
    }

    @Override
    public String[] getJobsUsingDatabase(ObjectId id_database) throws KettleException {
        return new String[0];
    }

    @Override
    public String[] getTransformationsUsingDatabase(ObjectId id_database) throws KettleException {
        return new String[0];
    }

    @Override
    public IRepositoryImporter getImporter() {
            return new RepositoryImporter( this );
    }

    @Override
    public IRepositoryExporter getExporter() throws KettleException {
        return new RepositoryExporter( this );
    }

    @Override
    public IMetaStore getMetaStore() {
        return this.metaStore;
    }

    @Override
    public boolean getJobEntryAttributeBoolean(ObjectId id_jobentry, int nr, String code, boolean def) throws KettleException {
        return false;
    }
    @Override
    public String getName() {
        return repositoryMeta.getName();
    }

    @Override
    public String getVersion() {
        return FILE_REPOSITORY_VERSION;
    }

    @Override
    public RepositoryMeta getRepositoryMeta() {
        return repositoryMeta;
    }

    @Override
    public IUser getUserInfo() {
        return null;
    }

    @Override
    public RepositorySecurityProvider getSecurityProvider() {
        return this.securityProvider;
    }

    @Override
    public RepositorySecurityManager getSecurityManager() {
        return null;
    }

    @Override
    public LogChannelInterface getLog() {
        return this.log;
    }

    public FileSystem getFs() {
        return fs;
    }

}
