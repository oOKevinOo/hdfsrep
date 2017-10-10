package org.pentaho.di.repository.hdfsrep;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.pentaho.metastore.api.*;
import org.pentaho.metastore.api.exceptions.*;
import org.pentaho.metastore.api.security.IMetaStoreElementOwner;
import org.pentaho.metastore.api.security.MetaStoreElementOwnerType;
import org.pentaho.metastore.stores.xml.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by zhiwen wang on 2017/10/9.
 */
public class HDFSXmlMetaStore extends BaseMetaStore implements IMetaStore {
    private String rootFolder;
    private Path rootFile;
    private FileSystem fs;
    private final XmlMetaStoreCache metaStoreCache;

    public HDFSXmlMetaStore() throws MetaStoreException, IOException {
        this((XmlMetaStoreCache)(new AutomaticXmlMetaStoreCache()));
    }
    public HDFSXmlMetaStore(XmlMetaStoreCache metaStoreCacheImpl) throws MetaStoreException, IOException {
        this(System.getProperty("java.io.tmpdir") + File.separator + UUID.randomUUID(), metaStoreCacheImpl);
    }
    public HDFSXmlMetaStore(FileSystem fs , XmlMetaStoreCache metaStoreCacheImpl) throws MetaStoreException, IOException {
        this(System.getProperty("java.io.tmpdir") + File.separator + UUID.randomUUID(),fs , metaStoreCacheImpl);
    }
    public HDFSXmlMetaStore(String rootFolder,FileSystem fs) throws MetaStoreException, IOException {
        this(rootFolder,fs, new AutomaticXmlMetaStoreCache());
    }
    public HDFSXmlMetaStore(String rootFolder,XmlMetaStoreCache metaStoreCacheImpl) throws MetaStoreException, IOException {
        this(rootFolder,null, new AutomaticXmlMetaStoreCache());
    }

    public HDFSXmlMetaStore(String rootFolder, FileSystem fs, XmlMetaStoreCache metaStoreCacheImpl) throws MetaStoreException, IOException {
        this.rootFolder = rootFolder + File.separator + "metastore";
        this.rootFile = new Path(this.rootFolder);
        this.fs = fs;
        if(!fs.exists(this.rootFile) && !fs.mkdirs(this.rootFile)) {
            throw new MetaStoreException("Unable to create XML meta store root folder: " + this.rootFolder);
        } else {
            this.setName(this.rootFolder);
            this.metaStoreCache = metaStoreCacheImpl;
        }
    }

    @Override
    public List<String> getNamespaces() throws MetaStoreException {
//        this.lockStore();

        try {
            FileStatus[] files = this.listFolders(this.rootFile);
            ArrayList namespaces = new ArrayList(files.length);
            FileStatus[] arr$ = files;
            int len$ = files.length;

            for(int i$ = 0; i$ < len$; ++i$) {
                FileStatus file = arr$[i$];
                namespaces.add(file.getPath().getName());
            }

            ArrayList var10 = namespaces;
            return var10;
        } finally {
//            this.unlockStore();
        }
    }
    protected FileStatus[] listFolders(Path folder) {
        FileStatus[] folders = new FileStatus[0];
        try {
        if(!fs.exists(folder)) {
            fs.mkdirs(folder);
        }
        folders = fs.listStatus(folder,new PathFilter() {
            public boolean accept(Path file) {
                try {
                    return fs.isDirectory(file);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                    return false;
                }
            });
        } catch (IOException e) {
            return folders = new FileStatus[0];
        }

        if(folders == null) {
            folders = new FileStatus[0];
        }
        return folders;
    }

    @Override
    public void createNamespace(String namespace) throws MetaStoreException, MetaStoreNamespaceExistsException {
//        this.lockStore();

        try {
            String spaceFolder = XmlUtil.getNamespaceFolder(this.rootFolder, namespace);
            Path spaceFile = new Path(spaceFolder);
            if(fs.exists(spaceFile)) {
                throw new MetaStoreNamespaceExistsException("The namespace with name \'" + namespace + "\' already exists.");
            }

            if(!fs.mkdirs(spaceFile)) {
                throw new MetaStoreException("Unable to create XML meta store namespace folder: " + spaceFolder);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
//            this.unlockStore();
        }
    }

    @Override
    public void deleteNamespace(String namespace) throws MetaStoreException, MetaStoreDependenciesExistsException {
//        this.lockStore();

        try {
            String spaceFolder = XmlUtil.getNamespaceFolder(this.rootFolder, namespace);
            Path spaceFile = new Path(spaceFolder);
            if(fs.exists(spaceFile)) {
                List elementTypes = this.getElementTypes(namespace, false);
                if(!elementTypes.isEmpty()) {
                    ArrayList dependencies = new ArrayList(elementTypes.size());
                    Iterator i$ = elementTypes.iterator();

                    while(i$.hasNext()) {
                        IMetaStoreElementType elementType = (IMetaStoreElementType)i$.next();
                        dependencies.add(elementType.getId());
                    }

                    throw new MetaStoreDependenciesExistsException(dependencies, "Unable to delete the XML meta store namespace with name \'" + namespace + "\' as it still contains dependencies");
                }

                if(fs.delete(spaceFile,true)) {
                    return;
                }

                throw new MetaStoreException("Unable to delete XML meta store namespace folder, check to see if it\'s empty");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
//            this.unlockStore();
        }
    }

    @Override
    public boolean namespaceExists(String namespace) throws MetaStoreException {
//        this.lockStore();

        boolean var4 = false;
        try {
            String spaceFolder = XmlUtil.getNamespaceFolder(this.rootFolder, namespace);
            Path spaceFile = new Path(spaceFolder);
            var4 = fs.exists(spaceFile);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
//            this.unlockStore();
        }

        return var4;
    }

    @Override
    public List<IMetaStoreElementType> getElementTypes(String namespace) throws MetaStoreException {
        return  getElementTypes( namespace, true);
    }
    protected synchronized List<IMetaStoreElementType> getElementTypes(String namespace, boolean lock) throws MetaStoreException {
        if(lock) {
//            this.lockStore();
        }

        try {
            String spaceFolder = XmlUtil.getNamespaceFolder(this.rootFolder, namespace);
            Path spaceFolderFile = new Path(spaceFolder);
            FileStatus[] elementTypeFolders = this.listFolders(spaceFolderFile);
            ArrayList elementTypes = new ArrayList(elementTypeFolders.length);
            FileStatus[] arr$ = elementTypeFolders;
            int len$ = elementTypeFolders.length;

            for(int i$ = 0; i$ < len$; ++i$) {
                FileStatus elementTypeFolder = arr$[i$];
                String elementTypeId = elementTypeFolder.getPath().getName();
                XmlMetaStoreElementType elementType = this.getElementType(namespace, elementTypeId, false);
                elementTypes.add(elementType);
            }

            ArrayList var16 = elementTypes;
            return var16;
        } finally {
            if(lock) {
//                this.unlockStore();
            }

        }
    }

    protected synchronized XmlMetaStoreElementType getElementType(String namespace, String elementTypeId, boolean lock) throws MetaStoreException {
        if(lock) {
//            this.lockStore();
        }

        XmlMetaStoreElementType var6;
        try {
            String elementTypeFile = XmlUtil.getElementTypeFile(this.rootFolder, namespace, elementTypeId);
            XmlMetaStoreElementType elementType = new XmlMetaStoreElementType(namespace, elementTypeFile);
            elementType.setMetaStoreName(this.getName());
            var6 = elementType;
        } finally {
            if(lock) {
//                this.unlockStore();
            }

        }

        return var6;
    }

    @Override
    public List<String> getElementTypeIds(String namespace) throws MetaStoreException {
//        this.lockStore();

        try {
            String spaceFolder = XmlUtil.getNamespaceFolder(this.rootFolder, namespace);
            Path spaceFolderFile = new Path(spaceFolder);
            FileStatus[] elementTypeFolders = this.listFolders(spaceFolderFile);
            ArrayList ids = new ArrayList(elementTypeFolders.length);
            FileStatus[] arr$ = elementTypeFolders;
            int len$ = elementTypeFolders.length;

            for(int i$ = 0; i$ < len$; ++i$) {
                FileStatus elementTypeFolder = arr$[i$];
                String elementTypeId = elementTypeFolder.getPath().getName();
                ids.add(elementTypeId);
            }

            ArrayList var14 = ids;
            return var14;
        } finally {
//            this.unlockStore();
        }
    }

    @Override
    public IMetaStoreElementType getElementType(String namespace, String elementTypeId) throws MetaStoreException {
        return this.getElementType(namespace, elementTypeId, true);
    }

    @Override
    public IMetaStoreElementType getElementTypeByName(String namespace, String elementTypeName) throws MetaStoreException {
        Iterator i$ = this.getElementTypes(namespace).iterator();

        IMetaStoreElementType elementType;
        do {
            if(!i$.hasNext()) {
                return null;
            }

            elementType = (IMetaStoreElementType)i$.next();
        } while(elementType.getName() == null || !elementType.getName().equalsIgnoreCase(elementTypeName));

        return (XmlMetaStoreElementType)elementType;
    }

    @Override
    public void createElementType(String namespace, IMetaStoreElementType elementType) throws MetaStoreException, MetaStoreElementTypeExistsException {
//        this.lockStore();

        try {
            if(elementType.getId() == null) {
                elementType.setId(elementType.getName());
            }

            String elementTypeFolder = XmlUtil.getElementTypeFolder(this.rootFolder, namespace, elementType.getName());
            Path elementTypeFolderFile = new Path(elementTypeFolder);
            if(fs.exists(elementTypeFolderFile)) {
                throw new MetaStoreElementTypeExistsException(this.getElementTypes(namespace, false), "The specified element type already exists with the same ID");
            }

            if(!fs.mkdirs(elementTypeFolderFile)) {
                throw new MetaStoreException("Unable to create XML meta store element type folder \'" + elementTypeFolder + "\'");
            }

            String elementTypeFilename = XmlUtil.getElementTypeFile(this.rootFolder, namespace, elementType.getName());
            XmlMetaStoreElementType xmlType = new XmlMetaStoreElementType(namespace, elementType.getId(), elementType.getName(), elementType.getDescription());
            xmlType.setFilename(elementTypeFilename);
            xmlType.save();
            this.metaStoreCache.registerElementTypeIdForName(namespace, elementType.getName(), elementType.getId());
            this.metaStoreCache.registerProcessedFile(elementTypeFolder, fs.getFileStatus(new Path(elementTypeFolder)).getModificationTime());
            xmlType.setMetaStoreName(this.getName());
            elementType.setMetaStoreName(this.getName());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
//            this.unlockStore();
        }
    }

    @Override
    public void updateElementType(String namespace, IMetaStoreElementType elementType) throws MetaStoreException {
//        this.lockStore();

        try {
            String elementTypeFolder = XmlUtil.getElementTypeFolder(this.rootFolder, namespace, elementType.getName());
            Path elementTypeFolderFile = new Path(elementTypeFolder);
            if(!fs.exists(elementTypeFolderFile)) {
                throw new MetaStoreException("The specified element type with ID \'" + elementType.getId() + "\' doesn\'t exists so we can\'t update it.");
            }

            String elementTypeFilename = XmlUtil.getElementTypeFile(this.rootFolder, namespace, elementType.getName());
            XmlMetaStoreElementType xmlType = new XmlMetaStoreElementType(namespace, elementType.getId(), elementType.getName(), elementType.getDescription());
            xmlType.setFilename(elementTypeFilename);
            xmlType.save();
            this.metaStoreCache.registerElementTypeIdForName(namespace, elementType.getName(), elementType.getId());
            this.metaStoreCache.registerProcessedFile(elementTypeFolder, fs.getFileStatus(elementTypeFolderFile).getModificationTime());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
//            this.unlockStore();
        }
    }

    @Override
    public void deleteElementType(String namespace, IMetaStoreElementType elementType) throws MetaStoreException, MetaStoreDependenciesExistsException {
//        this.lockStore();

        try {
            String elementTypeFilename = XmlUtil.getElementTypeFile(this.rootFolder, namespace, elementType.getName());
            Path elementTypeFile = new Path(elementTypeFilename);
            if(fs.exists(elementTypeFile)) {
                List elements = this.getElements(namespace, elementType, false, true);
                if(!elements.isEmpty()) {
                    ArrayList elementTypeFolder1 = new ArrayList();
                    Iterator elementTypeFolderFile1 = elements.iterator();

                    while(elementTypeFolderFile1.hasNext()) {
                        IMetaStoreElement element = (IMetaStoreElement)elementTypeFolderFile1.next();
                        elementTypeFolder1.add(element.getId());
                    }

                    throw new MetaStoreDependenciesExistsException(elementTypeFolder1, "Unable to delete element type with name \'" + elementType.getName() + "\' in namespace \'" + namespace + "\' because there are still elements present");
                }

                if(!fs.delete(elementTypeFile,true)) {
                    throw new MetaStoreException("Unable to delete element type XML file \'" + elementTypeFilename + "\'");
                }

                String elementTypeFolder = XmlUtil.getElementTypeFolder(this.rootFolder, namespace, elementType.getName());
                Path elementTypeFolderFile = new Path(elementTypeFolder);
                if(!fs.delete(elementTypeFolderFile,true)) {
                    throw new MetaStoreException("Unable to delete element type XML folder \'" + elementTypeFolder + "\'");
                }

                this.metaStoreCache.unregisterElementTypeId(namespace, elementType.getId());
                this.metaStoreCache.unregisterProcessedFile(elementTypeFolder);
                return;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
//            this.unlockStore();
        }
    }
    protected synchronized List<IMetaStoreElement> getElements(String namespace, IMetaStoreElementType elementType, boolean lock, boolean includeProcessedFiles) throws MetaStoreException {
        if(lock) {
//            this.lockStore();
        }

        ArrayList var17 = null;
        try {
            String elementTypeFolder = XmlUtil.getElementTypeFolder(this.rootFolder, namespace, elementType.getName());
            Path elementTypeFolderFile = new Path(elementTypeFolder);
            FileStatus[] elementTypeFiles = this.listFiles(elementTypeFolderFile, includeProcessedFiles);
            ArrayList elements = new ArrayList(elementTypeFiles.length);
            FileStatus[] arr$ = elementTypeFiles;
            int len$ = elementTypeFiles.length;

            for(int i$ = 0; i$ < len$; ++i$) {
                FileStatus elementTypeFile = arr$[i$];
                String elementId = elementTypeFile.getPath().getName();
                if(!elementId.equals(".type.xml")) {
                    elementId = elementId.substring(0, elementId.length() - 4);
                    elements.add(this.getElement(namespace, elementType, elementId, false));
                }
            }

            var17 = elements;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(lock) {
//                this.unlockStore();
            }

        }

        return var17;
    }
    protected synchronized IMetaStoreElement getElement(String namespace, IMetaStoreElementType elementType, String elementId, boolean lock) throws MetaStoreException {
        if(lock) {
//            this.lockStore();
        }

        XmlMetaStoreElement var8 = null;
        try {
            String elementFilename = XmlUtil.getElementFile(this.rootFolder, namespace, elementType.getName(), elementId);
            Path elementFile = new Path(elementFilename);
            XmlMetaStoreElement element;
            if(!fs.exists(elementFile)) {
                element = null;
                return element;
            }

            element = new XmlMetaStoreElement(elementFilename);
            this.metaStoreCache.registerElementIdForName(namespace, elementType, element.getName(), elementId);
            this.metaStoreCache.registerProcessedFile(elementFilename, fs.getFileStatus(elementFile).getModificationTime());
            var8 = element;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(lock) {
//                this.unlockStore();
            }

        }

        return var8;
    }

    protected FileStatus[] listFiles(Path folder, final boolean includeProcessedFiles) throws IOException {
        FileStatus[] files = fs.listStatus(folder, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                if(!includeProcessedFiles) {
                    Map processedFiles = HDFSXmlMetaStore.this.metaStoreCache.getProcessedFiles();
                    Long fileLastModified = (Long)processedFiles.get(path.getName());
                    try {
                        if(fileLastModified != null && fileLastModified.equals(Long.valueOf(fs.getFileStatus(path).getModificationTime()))) {
                            return false;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                return false;
            }
        });
        if(files == null) {
            files = new FileStatus[0];
        }

        return files;
    }
    @Override
    public List<IMetaStoreElement> getElements(String namespace, IMetaStoreElementType elementType) throws MetaStoreException {
        return this.getElements(namespace, elementType, true, true);
    }

    @Override
    public List<String> getElementIds(String namespace, IMetaStoreElementType elementType) throws MetaStoreException {
//        this.lockStore();

        ArrayList var15 = null;
        try {
            String elementTypeFolder = XmlUtil.getElementTypeFolder(this.rootFolder, namespace, elementType.getName());
            Path elementTypeFolderFile = new Path(elementTypeFolder);
            FileStatus[] elementTypeFiles = this.listFiles(elementTypeFolderFile, true);
            ArrayList elementIds = new ArrayList(elementTypeFiles.length);
            FileStatus[] arr$ = elementTypeFiles;
            int len$ = elementTypeFiles.length;

            for(int i$ = 0; i$ < len$; ++i$) {
                FileStatus elementTypeFile = arr$[i$];
                String elementId = elementTypeFile.getPath().getName();
                if(!elementId.equals(".type.xml")) {
                    elementId = elementId.substring(0, elementId.length() - 4);
                    elementIds.add(elementId);
                }
            }

            var15 = elementIds;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
//            this.unlockStore();
        }

        return var15;
    }

    @Override
    public IMetaStoreElement getElement(String namespace, IMetaStoreElementType elementType, String elementId) throws MetaStoreException {
        return this.getElement(namespace, elementType, elementId, true);
    }

    @Override
    public IMetaStoreElement getElementByName(String namespace, IMetaStoreElementType elementType, String name) throws MetaStoreException {
//        this.lockStore();

        IMetaStoreElement var7;
        try {
            String chachedElementId = this.metaStoreCache.getElementIdByName(namespace, elementType, name);
            IMetaStoreElement i$;
            IMetaStoreElement element;
            if(chachedElementId != null) {
                i$ = this.getElement(namespace, elementType, chachedElementId, false);
                if(i$ != null && i$.getName().equalsIgnoreCase(name)) {
                    element = i$;
                    return element;
                }
            }

            Iterator i$1 = this.getElements(namespace, elementType, false, false).iterator();

            do {
                if(!i$1.hasNext()) {
                    i$ = null;
                    return i$;
                }

                element = (IMetaStoreElement)i$1.next();
            } while(element.getName() == null || !element.getName().equalsIgnoreCase(name));

            var7 = element;
        } finally {
//            this.unlockStore();
        }

        return var7;
    }

    @Override
    public void createElement(String namespace, IMetaStoreElementType elementType, IMetaStoreElement element) throws MetaStoreException, MetaStoreElementExistException {
//        this.lockStore();

        try {
            if(element.getId() == null) {
                element.setId(element.getName());
            }

            String elementFilename = XmlUtil.getElementFile(this.rootFolder, namespace, elementType.getName(), element.getId());
            Path elementFile = new Path(elementFilename);
            if(fs.exists(elementFile)) {
                throw new MetaStoreElementExistException(this.getElements(namespace, elementType, false, true), "The specified element already exists with the same ID: \'" + element.getId() + "\'");
            }

            XmlMetaStoreElement xmlElement = new XmlMetaStoreElement(element);
            xmlElement.setFilename(elementFilename);
            xmlElement.save();
            this.metaStoreCache.registerElementIdForName(namespace, elementType, xmlElement.getName(), element.getId());
            this.metaStoreCache.registerProcessedFile(elementFilename, fs.getFileStatus(new Path(elementFilename)).getModificationTime());
            element.setId(xmlElement.getName());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
//            this.unlockStore();
        }
    }

    @Override
    public void deleteElement(String namespace, IMetaStoreElementType elementType, String elementId) throws MetaStoreException {
//        this.lockStore();

        try {
            String elementFilename = XmlUtil.getElementFile(this.rootFolder, namespace, elementType.getName(), elementId);
            Path elementFile = new Path(elementFilename);
            if(fs.exists(elementFile)) {
                if(!fs.delete(elementFile,true)) {
                    throw new MetaStoreException("Unable to delete element with ID \'" + elementId + "\' in filename \'" + elementFilename + "\'");
                }

                this.metaStoreCache.unregisterElementId(namespace, elementType, elementId);
                this.metaStoreCache.unregisterProcessedFile(elementFilename);
                return;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
//            this.unlockStore();
        }
    }

    @Override
    public void updateElement(String namespace, IMetaStoreElementType elementType, String elementId, IMetaStoreElement element) throws MetaStoreException {
        if(elementType.getMetaStoreName() != null && elementType.getMetaStoreName().equals(this.getName())) {
//            this.lockStore();

            try {
                String elementFilename = XmlUtil.getElementFile(this.rootFolder, namespace, elementType.getName(), element.getName());
                Path elementFile = new Path(elementFilename);
                if(!fs.exists(elementFile)) {
                    throw new MetaStoreException("The specified element to update doesn\'t exist with ID: \'" + elementId + "\'");
                }

                XmlMetaStoreElement xmlElement = new XmlMetaStoreElement(element);
                xmlElement.setFilename(elementFilename);
                xmlElement.setIdWithFilename(elementFilename);
                xmlElement.save();
                this.metaStoreCache.registerElementIdForName(namespace, elementType, xmlElement.getName(), xmlElement.getId());
                this.metaStoreCache.registerProcessedFile(elementFilename, fs.getFileStatus(elementFile).getModificationTime());
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
//                this.unlockStore();
            }

        } else {
            throw new MetaStoreException("The element type \'" + elementType.getName() + "\' needs to explicitly belong to the meta store in which you are updating.");
        }
    }

    @Override
    public IMetaStoreElementType newElementType(String namespace) throws MetaStoreException {
        return new XmlMetaStoreElementType(namespace, (String)null, (String)null, (String)null);
    }

    @Override
    public IMetaStoreElement newElement() throws MetaStoreException {
        return new XmlMetaStoreElement();
    }

    @Override
    public IMetaStoreElement newElement(IMetaStoreElementType elementType, String id, Object value) throws MetaStoreException {
        return new XmlMetaStoreElement(elementType, id, value);
    }

    @Override
    public IMetaStoreAttribute newAttribute(String id, Object value) throws MetaStoreException {
        return new XmlMetaStoreAttribute(id, value);
    }

    @Override
    public IMetaStoreElementOwner newElementOwner(String s, MetaStoreElementOwnerType ownerType) throws MetaStoreException {
        return new XmlMetaStoreElementOwner(name, ownerType);
    }

    protected void lockStore() throws MetaStoreException {
        boolean waiting = true;
        long totalTime = 0L;

        do {
            if(!waiting) {
                return;
            }

            Path lockFile = new Path(this.rootFile, ".lock");
            try {
                if(fs.createNewFile(lockFile)) {
                    return;
                }
            } catch (IOException var7) {
                throw new MetaStoreException("Unable to create lock file: " + lockFile.toString(), var7);
            }

            try {
                Thread.sleep(100L);
            } catch (InterruptedException var6) {
                throw new RuntimeException(var6);
            }

            totalTime += 100L;
        } while(totalTime <= 10000L);

        throw new MetaStoreException("Maximum wait time of 10 seconds exceed while acquiring lock");
    }

    protected void unlockStore()  {
        Path lockFile = new Path(this.rootFile, ".lock");
        try {
            fs.delete(lockFile,true);
        } catch (IOException e) {
          e.printStackTrace();
        }
    }

    public String getRootFolder() {
        return rootFolder;
    }

    public void setRootFolder(String rootFolder) {
        this.rootFolder = rootFolder;
    }

    public Path getRootFile() {
        return rootFile;
    }

    public void setRootFile(Path rootFile) {
        this.rootFile = rootFile;
    }

    public FileSystem getFs() {
        return fs;
    }

    public void setFs(FileSystem fs) {
        this.fs = fs;
    }

    public XmlMetaStoreCache getMetaStoreCache() {
        return metaStoreCache;
    }
}
