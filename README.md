# hdfsrep
a repository for kettle base on hdfs
目前已实现repository基本功能，暂未写dialog和不能保存。可以结合file system使用。
使用方法：
1.用户目录下的repositories.xml文件中添加元数据，具体为
 <repository>    <id>KettleHDFSRepository</id>
    <name>hadoop</name>
    <description>hadoop for test</description>
    <base_directory>/test/kettle/rep</base_directory>
  <!-- hadoop 配置文件路径 需要core-site.xml和hdfs-site.xml两个配置文件-->
	<hadoop_core_file>C:/Users/Administrator/.kettle/hadoop/core-site.xml</hadoop_core_file>
	<hadoop_hdfs_file>C:/Users/Administrator/.kettle/hadoop/hdfs-site.xml</hadoop_hdfs_file>
	<read_only>N</read_only>
	<hides_hidden_files>N</hides_hidden_files>
  </repository>
  
  2.将程序生成jar包并和相关依赖一起放到kettle的lib目录，并修改*kettle-engine*.jar包中的kettle-repositories.xml文件，添加以下内容：
  <repository id="KettleHDFSRepository">
		<description>i18n:org.pentaho.di.repository:RepositoryType.Name.KettleHDFSRepository</description>
		<tooltip>i18n:org.pentaho.di.repository:RepositoryType.Description.KettleHDFSRepository</tooltip>
		<classname>org.pentaho.di.repository.hdfsrep.KettleHDFSRepository</classname>
		<meta-classname>org.pentaho.di.repository.hdfsrep.KettleHDFSRepositoryMeta</meta-classname>
		<version-browser-classname/>
	</repository>
    或者以上操作都不做，直接将本项目里面artificial目录下的kettle-engine.jar文件直接替换kettle lib目录中的文件
